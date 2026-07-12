"""Structural contracts for deterministic time-series fingerprints."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import zipfile
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, localcontext
from io import StringIO
from itertools import islice
from pathlib import Path
from typing import Any, cast

from histdatacom.data_quality.contracts import (
    QualityFinding,
    QualityLocation,
    QualityRule,
    QualitySeverity,
    QualityTarget,
    QualityTargetKind,
)
from histdatacom.data_quality.calendar import (
    SESSION_MARKET_CLOSED,
    SESSION_NO_ACTIVE_WINDOW,
    SESSION_STATE_WEEKEND_CLOSURE,
    SOURCE_WEEKDAY_NAMES,
    calendar_regime_payload_for_target,
    classify_histdata_source_timestamp,
    classify_histdata_timestamp,
)
from histdatacom.data_quality.calendar_profiles import (
    HistDataCalendarProfile,
    default_calendar_profile,
)
from histdatacom.data_quality.classical_baselines import (
    ClassicalBaselineProfile,
    classical_baseline_diagnostics_from_training_frame,
)
from histdatacom.data_quality.classical_model_contracts import (
    ClassicalModelInputProfile,
    build_classical_model_input,
)
from histdatacom.data_quality.autoregressive import (
    AutoregressiveProfile,
    autoregressive_from_model_input,
)
from histdatacom.data_quality.exponential_smoothing import (
    ExponentialSmoothingProfile,
    exponential_smoothing_from_model_input,
)
from histdatacom.data_quality.limits import (
    BoundedReportLimit,
    bounded_report_limit,
)
from histdatacom.data_quality.polars_cache import (
    read_fingerprint_parity_polars_cache,
    read_quality_polars_cache,
)
from histdatacom.data_quality.remediation import (
    remediation_hint_payloads_for_flags,
)
from histdatacom.data_quality.symbols import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY as CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    CROSS_SERIES_FINGERPRINT_RULE_ID as CROSS_SERIES_FINGERPRINT_RULE_ID,
    CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION as CROSS_SERIES_FINGERPRINT_SCHEMA_VERSION,
    HistDataCrossSeriesFingerprintRule as HistDataCrossSeriesFingerprintRule,
    symbol_metadata_for,
)
from histdatacom.data_quality.synthetic_constraints import (
    synthetic_constraints_from_fingerprint,
)
from histdatacom.data_quality.time import (
    DEFAULT_TIMESTAMP_INSPECTION_SAMPLE_LIMIT,
    timestamp_topology_payload_for_target,
)
from histdatacom.data_quality.training_features import (
    TRAINING_REQUIRED_COLUMNS,
    TRAINING_SCHEMA_VERSION,
    ensure_tick_training_features,
    quality_report_from_training_features,
)
from histdatacom.data_quality.ticks import (
    DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS,
    DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS,
)
from histdatacom.histdata_ascii import (
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    format_influx_line,
    normalize_ascii_row,
    parse_ascii_lines,
    to_polars_frame,
)
from histdatacom.publication_safety import publish_safe_path
from histdatacom.runtime_contracts import JSONValue

TIME_SERIES_FINGERPRINT_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint.v1"
)
TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-coverage.v1"
)
TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-topology-summary.v1"
)
TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-topology-attention.v1"
)
TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-distribution-summary.v1"
)
TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-distribution-attention.v1"
)
TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-regime-summary.v1"
)
TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-conditional-distributions.v1"
)
TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-dynamics.v1"
)
TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-dependence.v1"
)
TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-stationarity.v1"
)
TIME_SERIES_FINGERPRINT_DECOMPOSITION_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-decomposition.v1"
)
TIME_SERIES_FINGERPRINT_DECOMPOSITION_TRAINING_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-decomposition-training-projection.v1"
)
TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-audit.v1"
)
TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-readiness-summary.v1"
)
TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-readiness-risk.v1"
)
TIME_SERIES_FINGERPRINT_PARITY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-cache-source-parity.v1"
)
TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-cache-source-parity-summary.v1"
)
TIME_SERIES_FINGERPRINT_METADATA_KEY = "time_series_fingerprint"
TIME_SERIES_FINGERPRINT_COVERAGE_METADATA_KEY = (
    "time_series_fingerprint_coverage"
)
TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_topology_summary"
)
TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_METADATA_KEY = (
    "time_series_fingerprint_topology_attention"
)
TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_distribution_summary"
)
TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_METADATA_KEY = (
    "time_series_fingerprint_distribution_attention"
)
TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_regime_summary"
)
TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_readiness_summary"
)
TIME_SERIES_FINGERPRINT_READINESS_RISK_METADATA_KEY = (
    "time_series_fingerprint_readiness_risk"
)
TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_cache_source_parity_summary"
)
SERIES_FINGERPRINT_RULE_ID = "fingerprint.series"
SERIES_FINGERPRINT_SUMMARY_CODE = "FINGERPRINT_SERIES_SUMMARY"
SERIES_FINGERPRINT_SOURCE_UNAVAILABLE_CODE = "FINGERPRINT_SOURCE_UNAVAILABLE"

DEFAULT_FINGERPRINT_QUANTILES = (
    0.01,
    0.05,
    0.25,
    0.5,
    0.75,
    0.95,
    0.99,
)
DEFAULT_FINGERPRINT_LAGS = (1, 2, 3, 5, 10, 30, 60, 240, 1440)
DEFAULT_FINGERPRINT_ROLLING_WINDOWS = (60, 240, 1440)
DEFAULT_FINGERPRINT_HISTOGRAM_BINS = 32
DEFAULT_FINGERPRINT_MAX_ROWS = 1_000_000
DEFAULT_FINGERPRINT_ROUNDING_DIGITS = 12
DEFAULT_FINGERPRINT_TOPOLOGY_INSPECTION_SAMPLE_LIMIT = (
    DEFAULT_TIMESTAMP_INSPECTION_SAMPLE_LIMIT
)
DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT = 128
DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT = 32
DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT = 128
DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT = 32
DEFAULT_FINGERPRINT_REGIME_SUMMARY_LIMIT = 16
DEFAULT_FINGERPRINT_REGIME_COUNT_LIMIT = 8
DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT = 16
DEFAULT_FINGERPRINT_READINESS_LAG_LIMIT = 16
DEFAULT_FINGERPRINT_READINESS_RISK_TARGET_LIMIT = 16
DEFAULT_FINGERPRINT_READINESS_RISK_SECTION_LIMIT = 8
DEFAULT_FINGERPRINT_READINESS_RISK_REASON_LIMIT = 8
DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_COUNT = 1
DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_RATE = 0.0
DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_COUNT = 1
DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_RATE = 0.0
DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_COUNT = 1
DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_RATE = 0.0
DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_TRUNCATED = True
DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_CACHE_FLOAT_PRECISION = True
DEFAULT_FINGERPRINT_PARITY_MISMATCH_LIMIT = 16
DEFAULT_FINGERPRINT_PARITY_SUMMARY_LIMIT = 32
_CALENDAR_POLICY_CONTEXT_TEXT_LIMIT = 128
SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES = (TICK,)
SUPPORTED_SERIES_FINGERPRINT_KINDS = (
    QualityTargetKind.CSV,
    QualityTargetKind.ZIP,
    QualityTargetKind.CACHE,
)
ACTIONABLE_TOPOLOGY_FLAGS = (
    "unavailable_topology",
    "invalid_timestamps",
    "non_monotonic_timestamps",
    "duplicate_timestamps",
    "suspicious_gaps",
    "weekend_activity",
)
DISTRIBUTION_ATTENTION_FLAGS = (
    "missing_distribution",
    "empty_distribution",
    "high_invalid_row_rate",
    "partial_rows_present",
    "negative_tick_spreads_present",
    "zero_tick_spread_rate_present",
    "truncated_distribution",
    "missing_precision_counts",
    "cache_float_precision_basis",
)
FINGERPRINT_AUDIT_SECTIONS = (
    "coverage",
    "temporal_topology",
    "calendar_regimes",
    "tick_distribution",
    "conditional_distributions",
    "microstructure_dynamics",
    "dependence",
    "stationarity_diagnostics",
    "decomposition",
    "cache_source_parity",
    "classical_baselines",
    "classical_model_input",
    "exponential_smoothing",
    "autoregressive",
    "synthetic_constraints",
)
FINGERPRINT_DYNAMICS_SECTIONS = ("microstructure_dynamics",)


@dataclass(frozen=True, slots=True)
class HistDataFingerprintDistributionAttentionProfile:
    """Operator-tunable advisory thresholds for distribution attention."""

    invalid_row_min_count: int = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_COUNT
    )
    invalid_row_min_rate: float = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_RATE
    )
    zero_spread_min_count: int = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_COUNT
    )
    zero_spread_min_rate: float = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_RATE
    )
    negative_spread_min_count: int = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_COUNT
    )
    negative_spread_min_rate: float = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_RATE
    )
    flag_truncated_distribution: bool = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_TRUNCATED
    )
    flag_cache_float_precision: bool = (
        DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_CACHE_FLOAT_PRECISION
    )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "invalid_row_min_count": self.invalid_row_min_count,
            "invalid_row_min_rate": self.invalid_row_min_rate,
            "zero_spread_min_count": self.zero_spread_min_count,
            "zero_spread_min_rate": self.zero_spread_min_rate,
            "negative_spread_min_count": self.negative_spread_min_count,
            "negative_spread_min_rate": self.negative_spread_min_rate,
            "flag_truncated_distribution": self.flag_truncated_distribution,
            "flag_cache_float_precision": self.flag_cache_float_precision,
        }


@dataclass(frozen=True, slots=True)
class HistDataFingerprintParityProfile:
    """Opt-in bounded cache/source parity controls."""

    enabled: bool = False
    mismatch_limit: int = DEFAULT_FINGERPRINT_PARITY_MISMATCH_LIMIT

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "enabled": self.enabled,
            "mismatch_limit": self.mismatch_limit,
        }


@dataclass(frozen=True, slots=True)
class HistDataFingerprintProfile:
    """Operator-tunable limits for deterministic fingerprint summaries."""

    quantiles: tuple[float, ...] = DEFAULT_FINGERPRINT_QUANTILES
    lags: tuple[int, ...] = DEFAULT_FINGERPRINT_LAGS
    rolling_windows: tuple[int, ...] = DEFAULT_FINGERPRINT_ROLLING_WINDOWS
    histogram_bins: int = DEFAULT_FINGERPRINT_HISTOGRAM_BINS
    max_rows: int = DEFAULT_FINGERPRINT_MAX_ROWS
    rounding_digits: int = DEFAULT_FINGERPRINT_ROUNDING_DIGITS
    topology_inspection_sample_limit: int = (
        DEFAULT_FINGERPRINT_TOPOLOGY_INSPECTION_SAMPLE_LIMIT
    )
    calendar_profile: HistDataCalendarProfile = field(
        default_factory=default_calendar_profile,
        repr=False,
        compare=False,
    )
    distribution_attention: HistDataFingerprintDistributionAttentionProfile = (
        field(default_factory=HistDataFingerprintDistributionAttentionProfile)
    )
    cache_source_parity: HistDataFingerprintParityProfile = field(
        default_factory=HistDataFingerprintParityProfile
    )
    classical_baselines: ClassicalBaselineProfile = field(
        default_factory=ClassicalBaselineProfile
    )
    classical_model_input: ClassicalModelInputProfile = field(
        default_factory=ClassicalModelInputProfile
    )
    exponential_smoothing: ExponentialSmoothingProfile = field(
        default_factory=ExponentialSmoothingProfile
    )
    autoregressive: AutoregressiveProfile = field(
        default_factory=AutoregressiveProfile
    )

    def to_metadata(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "quantiles": list(self.quantiles),
            "lags": list(self.lags),
            "rolling_windows": list(self.rolling_windows),
            "histogram_bins": self.histogram_bins,
            "max_rows": self.max_rows,
            "rounding_digits": self.rounding_digits,
            "topology_inspection_sample_limit": (
                self.topology_inspection_sample_limit
            ),
            "distribution_attention": (
                self.distribution_attention.to_metadata()
            ),
            "cache_source_parity": self.cache_source_parity.to_metadata(),
            "classical_baselines": self.classical_baselines.to_metadata(),
            "classical_model_input": (self.classical_model_input.to_metadata()),
            "exponential_smoothing": self.exponential_smoothing.to_metadata(),
            "autoregressive": self.autoregressive.to_metadata(),
        }


@dataclass(slots=True)
class HistDataSeriesFingerprintRule:
    """Emit canonical target-scoped time-series fingerprints."""

    profile: HistDataFingerprintProfile = field(
        default_factory=HistDataFingerprintProfile
    )
    rule_id: str = SERIES_FINGERPRINT_RULE_ID
    description: str = (
        "Emit deterministic target-scoped time-series fingerprints."
    )

    def evaluate(self, target: QualityTarget) -> tuple[QualityFinding, ...]:
        """Return one bounded fingerprint finding for one target."""
        payload = _series_fingerprint_payload(target, self.profile)
        source = cast(dict[str, JSONValue], payload["source"])
        unavailable = source.get("kind") == "unavailable"
        code = (
            SERIES_FINGERPRINT_SOURCE_UNAVAILABLE_CODE
            if unavailable
            else SERIES_FINGERPRINT_SUMMARY_CODE
        )
        message = (
            "Target source is unavailable for canonical fingerprinting."
            if unavailable
            else "Canonical target time-series fingerprint."
        )
        return (
            QualityFinding(
                severity=QualitySeverity.INFO,
                code=code,
                message=message,
                rule_id=self.rule_id,
                target=target,
                location=QualityLocation(
                    path=target.path,
                    column=TIME_SERIES_FINGERPRINT_METADATA_KEY,
                ),
                metadata={TIME_SERIES_FINGERPRINT_METADATA_KEY: payload},
            ),
        )


def fingerprint_quality_rules(
    profile: HistDataFingerprintProfile | None = None,
) -> tuple[QualityRule, ...]:
    """Return target-scoped fingerprint quality rules."""
    rule: QualityRule = HistDataSeriesFingerprintRule(
        profile=profile or HistDataFingerprintProfile()
    )
    return (rule,)


def series_fingerprint_coverage_summary(
    findings: Iterable[QualityFinding],
    *,
    discovered_target_count: int | None = None,
    skipped_fingerprint_target_count: int = 0,
    skipped_reason_counts: Mapping[str, int] | None = None,
) -> dict[str, JSONValue] | None:
    """Return a bounded run summary for emitted series fingerprints."""
    source_kind_counts: Counter[str] = Counter()
    cache_source_counts: Counter[str] = Counter()
    unavailable_reason_counts: Counter[str] = Counter()
    target_kind_counts: Counter[str] = Counter()
    timeframe_counts: Counter[str] = Counter()
    fingerprint_target_count = 0
    supported_readable_count = 0
    unavailable_count = 0
    parsed_non_empty_coverage_count = 0

    for finding in findings:
        if finding.rule_id != SERIES_FINGERPRINT_RULE_ID:
            continue
        payload = finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        if not isinstance(payload, Mapping):
            continue

        target_axis = _payload_mapping(payload.get("target_axis"))
        coverage = _payload_mapping(payload.get("coverage"))
        source = _payload_mapping(payload.get("source"))

        source_kind = _summary_key(source.get("kind"))
        target_kind = _summary_key(
            target_axis.get("kind") or finding.target.kind.value
        )
        timeframe = _summary_key(
            target_axis.get("timeframe") or finding.target.timeframe
        )

        fingerprint_target_count += 1
        source_kind_counts[source_kind] += 1
        target_kind_counts[target_kind] += 1
        timeframe_counts[timeframe] += 1

        if source_kind == "unavailable":
            unavailable_count += 1
            unavailable_reason_counts[_summary_key(source.get("reason"))] += 1
        else:
            supported_readable_count += 1

        if source_kind == "cache":
            cache_source_counts[_summary_key(source.get("cache_source"))] += 1

        if _has_parsed_non_empty_coverage(coverage):
            parsed_non_empty_coverage_count += 1

    if not fingerprint_target_count:
        return None

    normalized_skipped_reason_counts = Counter(
        {
            _summary_key(reason): int(count)
            for reason, count in (skipped_reason_counts or {}).items()
            if int(count) > 0
        }
    )
    skipped_fingerprint_target_count = max(
        int(skipped_fingerprint_target_count),
        sum(normalized_skipped_reason_counts.values()),
    )
    normalized_discovered_target_count = (
        fingerprint_target_count + skipped_fingerprint_target_count
        if discovered_target_count is None
        else max(int(discovered_target_count), fingerprint_target_count)
    )

    return {
        "schema_version": TIME_SERIES_FINGERPRINT_COVERAGE_SCHEMA_VERSION,
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "discovered_target_count": normalized_discovered_target_count,
        "evaluated_fingerprint_target_count": fingerprint_target_count,
        "fingerprint_target_count": fingerprint_target_count,
        "skipped_fingerprint_target_count": skipped_fingerprint_target_count,
        "supported_readable_count": supported_readable_count,
        "unavailable_count": unavailable_count,
        "parsed_non_empty_coverage_count": parsed_non_empty_coverage_count,
        "skipped_reason_counts": _counter_payload(
            normalized_skipped_reason_counts
        ),
        "source_kind_counts": _counter_payload(source_kind_counts),
        "cache_source_counts": _counter_payload(cache_source_counts),
        "unavailable_reason_counts": _counter_payload(
            unavailable_reason_counts
        ),
        "target_kind_counts": _counter_payload(target_kind_counts),
        "timeframe_counts": _counter_payload(timeframe_counts),
    }


def series_fingerprint_topology_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded target summaries for fingerprint timestamp topology."""
    target_summaries = _series_fingerprint_topology_target_summaries(findings)

    if not target_summaries:
        return None
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT,
    )

    status_counts = Counter(
        _summary_key(item.get("status")) for item in target_summaries
    )
    computed_from_counts = Counter(
        _summary_key(item.get("computed_from")) for item in target_summaries
    )
    sampling_basis_counts = Counter(
        _summary_key(item.get("sampling_basis")) for item in target_summaries
    )
    cache_source_counts = Counter(
        _summary_key(item.get("cache_source"))
        for item in target_summaries
        if item.get("cache_source") is not None
    )
    flag_counts: Counter[str] = Counter()
    for item in target_summaries:
        flags = item.get("flags")
        if isinstance(flags, list):
            flag_counts.update(_summary_key(flag) for flag in flags)

    included: list[JSONValue] = [
        dict(item) for item in target_limit_state.slice(target_summaries)
    ]
    omitted_count = max(0, len(target_summaries) - len(included))

    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_TOPOLOGY_SUMMARY_SCHEMA_VERSION
        ),
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "target_count": len(target_summaries),
        "included_target_count": len(included),
        "omitted_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {"targets": target_limit_state.limit_payload()},
        "status_counts": _counter_payload(status_counts),
        "computed_from_counts": _counter_payload(computed_from_counts),
        "cache_source_counts": _counter_payload(cache_source_counts),
        "sampling_basis_counts": _counter_payload(sampling_basis_counts),
        "flag_counts": _counter_payload(flag_counts),
        "target_summaries": included,
    }


def series_fingerprint_topology_attention_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded attention-first summaries for topology findings."""
    target_summaries = _series_fingerprint_topology_target_summaries(findings)
    if not target_summaries:
        return None
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT,
    )
    return _topology_attention_summary_from_targets(
        target_summaries,
        target_limit=target_limit_state,
    )


def series_fingerprint_distribution_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded target summaries for fingerprint distributions."""
    target_summaries = _series_fingerprint_distribution_target_summaries(
        findings
    )
    if not target_summaries:
        return None
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT,
    )

    distribution_kind_counts = Counter(
        _summary_key(item.get("distribution_kind")) for item in target_summaries
    )
    status_counts = Counter(
        _summary_key(item.get("status")) for item in target_summaries
    )
    source_kind_counts = Counter(
        _summary_key(item.get("source_kind")) for item in target_summaries
    )
    distribution_source_counts = Counter(
        _summary_key(item.get("distribution_source"))
        for item in target_summaries
    )
    precision_source_counts = Counter(
        _summary_key(item.get("precision_source")) for item in target_summaries
    )
    cache_source_counts = Counter(
        _summary_key(item.get("cache_source"))
        for item in target_summaries
        if item.get("cache_source") is not None
    )

    included: list[JSONValue] = [
        dict(item) for item in target_limit_state.slice(target_summaries)
    ]
    omitted_count = max(0, len(target_summaries) - len(included))

    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_DISTRIBUTION_SUMMARY_SCHEMA_VERSION
        ),
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "target_count": len(target_summaries),
        "included_target_count": len(included),
        "omitted_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {"targets": target_limit_state.limit_payload()},
        "distribution_target_count": sum(
            1
            for item in target_summaries
            if item.get("distribution_kind") != "missing"
        ),
        "tick_distribution_target_count": distribution_kind_counts["tick"],
        "missing_distribution_target_count": status_counts["missing"],
        "unavailable_distribution_target_count": status_counts["unavailable"],
        "empty_distribution_target_count": sum(
            1
            for item in target_summaries
            if item.get("distribution_kind") != "missing"
            and _int_payload(item.get("row_count")) == 0
        ),
        "invalid_row_target_count": sum(
            1
            for item in target_summaries
            if _int_payload(item.get("invalid_row_count")) > 0
        ),
        "partial_row_target_count": sum(
            1
            for item in target_summaries
            if _int_payload(item.get("partial_row_count")) > 0
        ),
        "truncated_distribution_target_count": sum(
            1 for item in target_summaries if item.get("truncated") is True
        ),
        "cache_backed_distribution_target_count": sum(
            1
            for item in target_summaries
            if item.get("distribution_source") == "cache"
        ),
        "text_backed_distribution_target_count": sum(
            1
            for item in target_summaries
            if item.get("distribution_source") == "text"
        ),
        "total_invalid_row_count": sum(
            _int_payload(item.get("invalid_row_count"))
            for item in target_summaries
        ),
        "total_partial_row_count": sum(
            _int_payload(item.get("partial_row_count"))
            for item in target_summaries
        ),
        "distribution_kind_counts": _counter_payload(distribution_kind_counts),
        "status_counts": _counter_payload(status_counts),
        "source_kind_counts": _counter_payload(source_kind_counts),
        "distribution_source_counts": _counter_payload(
            distribution_source_counts
        ),
        "cache_source_counts": _counter_payload(cache_source_counts),
        "precision_source_counts": _counter_payload(precision_source_counts),
        "target_summaries": included,
    }


def series_fingerprint_distribution_attention_summary(
    findings: Iterable[QualityFinding],
    *,
    profile: HistDataFingerprintProfile | None = None,
    target_limit: int | None = DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded attention-first summaries for distributions."""
    attention_profile = (
        profile or HistDataFingerprintProfile()
    ).distribution_attention
    target_summaries = _series_fingerprint_distribution_target_summaries(
        findings
    )
    if not target_summaries:
        return None
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT,
    )

    attention_targets = [
        attention
        for target in target_summaries
        if (
            attention := _distribution_attention_target_summary(
                target,
                attention_profile,
            )
        )
        is not None
    ]
    attention_targets.sort(key=_distribution_attention_sort_key)

    attention_level_counts = Counter(
        _summary_key(item.get("attention_level")) for item in attention_targets
    )
    attention_flag_counts: Counter[str] = Counter()
    for item in attention_targets:
        flags = item.get("attention_flags")
        if isinstance(flags, list):
            attention_flag_counts.update(_summary_key(flag) for flag in flags)

    included: list[JSONValue] = [
        dict(item) for item in target_limit_state.slice(attention_targets)
    ]
    omitted_count = max(0, len(attention_targets) - len(included))

    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_DISTRIBUTION_ATTENTION_SCHEMA_VERSION
        ),
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "distribution_target_count": len(target_summaries),
        "attention_target_count": len(attention_targets),
        "included_attention_target_count": len(included),
        "omitted_attention_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {"targets": target_limit_state.limit_payload()},
        "attention_thresholds": attention_profile.to_metadata(),
        "attention_level_counts": _counter_payload(attention_level_counts),
        "attention_flag_counts": _counter_payload(attention_flag_counts),
        "target_summaries": included,
    }


def series_fingerprint_regime_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_FINGERPRINT_REGIME_SUMMARY_LIMIT,
    count_limit: int | None = DEFAULT_FINGERPRINT_REGIME_COUNT_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded calendar/session and conditioned-spread summaries."""
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_REGIME_SUMMARY_LIMIT,
    )
    count_limit_state = bounded_report_limit(
        count_limit,
        default_limit=DEFAULT_FINGERPRINT_REGIME_COUNT_LIMIT,
        minimum_limit=1,
        allow_unbounded=False,
    )
    target_summaries = _series_fingerprint_regime_target_summaries(
        findings,
        count_limit=count_limit_state.effective_limit,
    )
    if not target_summaries:
        return None

    calendar_status_counts = Counter(
        _summary_key(
            _payload_mapping(item.get("calendar_regimes")).get("status")
        )
        for item in target_summaries
    )
    conditional_status_counts = Counter(
        _summary_key(
            _payload_mapping(item.get("conditional_distributions")).get(
                "status"
            )
        )
        for item in target_summaries
    )
    computed_from_counts: Counter[str] = Counter()
    cache_source_counts: Counter[str] = Counter()
    profile_source_counts: Counter[str] = Counter()
    profile_version_counts: Counter[str] = Counter()
    calendar_profile_complete_count = 0
    calendar_profile_static_advisory_count = 0
    aggregate_counts: dict[str, Counter[str]] = {
        "session_state_counts": Counter(),
        "active_session_counts": Counter(),
        "special_tag_counts": Counter(),
        "holiday_tag_counts": Counter(),
        "event_tag_counts": Counter(),
        "hour_of_day_counts": Counter(),
        "day_of_week_counts": Counter(),
    }

    for item in target_summaries:
        calendar = _payload_mapping(item.get("calendar_regimes"))
        computed_from = _optional_summary_key(calendar.get("computed_from"))
        if computed_from:
            computed_from_counts[computed_from] += 1
        cache_source = _optional_summary_key(calendar.get("cache_source"))
        if cache_source:
            cache_source_counts[cache_source] += 1
        profile = _payload_mapping(calendar.get("calendar_profile"))
        profile_source = _optional_summary_key(profile.get("source"))
        if profile_source:
            profile_source_counts[profile_source] += 1
        profile_version = _optional_summary_key(profile.get("version"))
        if profile_version:
            profile_version_counts[profile_version] += 1
        if profile.get("complete") is True:
            calendar_profile_complete_count += 1
        if profile.get("static_advisory") is True:
            calendar_profile_static_advisory_count += 1
        for key, counter in aggregate_counts.items():
            counter.update(
                _counter_from_mapping(_payload_mapping(calendar.get(key)))
            )

    included: list[JSONValue] = [
        dict(item) for item in target_limit_state.slice(target_summaries)
    ]
    omitted_count = max(0, len(target_summaries) - len(included))

    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_REGIME_SUMMARY_SCHEMA_VERSION
        ),
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "target_count": len(target_summaries),
        "included_target_count": len(included),
        "omitted_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "count_limit": count_limit_state.effective_limit,
        "limit_metadata": {
            "targets": target_limit_state.limit_payload(),
            "counts": count_limit_state.limit_payload(),
        },
        "calendar_regime_target_count": sum(
            1
            for item in target_summaries
            if _payload_mapping(item.get("calendar_regimes")).get("status")
            == "available"
        ),
        "conditional_distribution_target_count": sum(
            1
            for item in target_summaries
            if _payload_mapping(item.get("conditional_distributions")).get(
                "status"
            )
            == "available"
        ),
        "calendar_status_counts": _counter_payload(calendar_status_counts),
        "conditional_status_counts": _counter_payload(
            conditional_status_counts
        ),
        "computed_from_counts": _counter_payload(computed_from_counts),
        "cache_source_counts": _counter_payload(cache_source_counts),
        "calendar_profile": {
            "complete_count": calendar_profile_complete_count,
            "incomplete_count": max(
                0,
                len(target_summaries) - calendar_profile_complete_count,
            ),
            "static_advisory_count": calendar_profile_static_advisory_count,
            "source_counts": _counter_payload(profile_source_counts),
            "version_counts": _counter_payload(profile_version_counts),
        },
        "top_session_state_counts": _bounded_count_rows(
            aggregate_counts["session_state_counts"],
            limit=count_limit_state.effective_limit,
        ),
        "top_active_session_counts": _bounded_count_rows(
            aggregate_counts["active_session_counts"],
            limit=count_limit_state.effective_limit,
        ),
        "top_special_tag_counts": _bounded_count_rows(
            aggregate_counts["special_tag_counts"],
            limit=count_limit_state.effective_limit,
        ),
        "top_holiday_tag_counts": _bounded_count_rows(
            aggregate_counts["holiday_tag_counts"],
            limit=count_limit_state.effective_limit,
        ),
        "top_event_tag_counts": _bounded_count_rows(
            aggregate_counts["event_tag_counts"],
            limit=count_limit_state.effective_limit,
        ),
        "top_hour_of_day_counts": _bounded_count_rows(
            aggregate_counts["hour_of_day_counts"],
            limit=count_limit_state.effective_limit,
        ),
        "top_day_of_week_counts": _bounded_count_rows(
            aggregate_counts["day_of_week_counts"],
            limit=count_limit_state.effective_limit,
        ),
        "target_summaries": included,
    }


def series_fingerprint_parity_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_FINGERPRINT_PARITY_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return a bounded cache/source parity rollup from fingerprint findings."""
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_PARITY_SUMMARY_LIMIT,
    )
    targets: list[dict[str, JSONValue]] = []
    status_counts: Counter[str] = Counter()
    mismatch_code_counts: Counter[str] = Counter()
    cache_source_counts: Counter[str] = Counter()
    freshness_counts: Counter[str] = Counter()
    computed_from_counts: Counter[str] = Counter()
    for finding in findings:
        fingerprint = _payload_mapping(
            finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        )
        parity = _payload_mapping(fingerprint.get("cache_source_parity"))
        if not parity:
            continue
        bases = _payload_mapping(parity.get("bases"))
        raw_cache = _payload_mapping(bases.get("raw_cache"))
        enriched_cache = _payload_mapping(bases.get("enriched_cache"))
        calendar = _payload_mapping(fingerprint.get("calendar_regimes"))
        status = _summary_key(parity.get("status"))
        status_counts[status] += 1
        mismatch_codes = [
            str(item) for item in _string_list(parity.get("mismatch_codes"))
        ]
        mismatch_code_counts.update(mismatch_codes)
        cache_source = _optional_summary_key(raw_cache.get("cache_source"))
        if cache_source:
            cache_source_counts[cache_source] += 1
        freshness = _optional_summary_key(raw_cache.get("freshness"))
        if freshness:
            freshness_counts[freshness] += 1
        computed_from = _optional_summary_key(calendar.get("computed_from"))
        if computed_from:
            computed_from_counts[computed_from] += 1
        target_summary: dict[str, JSONValue] = {
            "target_axis": dict(_payload_mapping(parity.get("target_axis"))),
            "status": status,
            "compared_section_count": _int_payload(
                parity.get("compared_section_count")
            ),
            "mismatched_section_count": _int_payload(
                parity.get("mismatched_section_count")
            ),
            "mismatch_codes": cast(JSONValue, mismatch_codes),
            "skipped_reasons": cast(
                JSONValue,
                [
                    str(item)
                    for item in _string_list(parity.get("skipped_reasons"))
                ],
            ),
            "cache_source": cache_source,
            "freshness": freshness,
            "computed_from": computed_from,
            "training_substrate": {
                "status": enriched_cache.get("status"),
                "training_schema_version": enriched_cache.get(
                    "training_schema_version"
                ),
                "cache_was_enriched": enriched_cache.get("cache_was_enriched"),
                "legacy_cache_enriched_on_read": enriched_cache.get(
                    "legacy_cache_enriched_on_read"
                ),
            },
        }
        targets.append(target_summary)
    if not targets:
        return None
    targets.sort(key=_fingerprint_parity_target_sort_key)
    included: list[JSONValue] = [
        dict(item) for item in target_limit_state.slice(targets)
    ]
    omitted_count = max(0, len(targets) - len(included))
    return {
        "schema_version": TIME_SERIES_FINGERPRINT_PARITY_SUMMARY_SCHEMA_VERSION,
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "target_count": len(targets),
        "compared_target_count": sum(
            count
            for status, count in status_counts.items()
            if status in {"match", "mismatch"}
        ),
        "matching_target_count": status_counts.get("match", 0),
        "mismatched_target_count": status_counts.get("mismatch", 0),
        "not_compared_target_count": status_counts.get("not_compared", 0),
        "included_target_count": len(included),
        "omitted_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {"targets": target_limit_state.limit_payload()},
        "status_counts": _counter_payload(status_counts),
        "mismatch_code_counts": _counter_payload(mismatch_code_counts),
        "computed_from_counts": _counter_payload(computed_from_counts),
        "cache_source_counts": _counter_payload(cache_source_counts),
        "freshness_counts": _counter_payload(freshness_counts),
        "target_summaries": included,
    }


def _fingerprint_parity_target_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[int, str, str, str, str, str]:
    status_rank = {"mismatch": 0, "not_compared": 1, "match": 2}
    axis = _payload_mapping(target.get("target_axis"))
    return (
        status_rank.get(_summary_key(target.get("status")), 99),
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
    )


def series_fingerprint_readiness_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded target summaries for fingerprint audit/readiness."""
    target_summaries = _series_fingerprint_readiness_target_summaries(findings)
    if not target_summaries:
        return None
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT,
    )

    applicable_status_counts = Counter(
        _summary_key(item.get("applicable_dynamics_status"))
        for item in target_summaries
    )
    section_status_counts: dict[str, JSONValue] = {}
    for section in FINGERPRINT_AUDIT_SECTIONS:
        counts = Counter(
            _summary_key(
                _payload_mapping(item.get("section_statuses")).get(section)
            )
            for item in target_summaries
            if section in _payload_mapping(item.get("section_statuses"))
        )
        if counts:
            section_status_counts[section] = _counter_payload(counts)

    dynamics_status_counts: dict[str, JSONValue] = {}
    dynamics_reason_counts: dict[str, JSONValue] = {}
    for section in FINGERPRINT_DYNAMICS_SECTIONS:
        status_counts: Counter[str] = Counter()
        reason_counts: Counter[str] = Counter()
        for item in target_summaries:
            dynamics = _payload_mapping(item.get(section))
            status_counts[_summary_key(dynamics.get("status"))] += 1
            reason = _optional_summary_key(dynamics.get("reason"))
            if reason:
                reason_counts[reason] += 1
        dynamics_status_counts[section] = _counter_payload(status_counts)
        dynamics_reason_counts[section] = _counter_payload(reason_counts)

    dependence_status_counts: Counter[str] = Counter()
    dependence_reason_counts: Counter[str] = Counter()
    dependence_acf_basis_counts: Counter[str] = Counter()
    dependence_limitation_counts: Counter[str] = Counter()
    dependence_skipped_lag_reason_counts: Counter[str] = Counter()
    dependence_computed_lag_count = 0
    dependence_skipped_lag_count = 0
    stationarity_status_counts: Counter[str] = Counter()
    stationarity_reason_counts: Counter[str] = Counter()
    stationarity_basis_counts: Counter[str] = Counter()
    stationarity_limitation_counts: Counter[str] = Counter()
    stationarity_skipped_window_reason_counts: Counter[str] = Counter()
    stationarity_recommended_transform_counts: Counter[str] = Counter()
    stationarity_computed_window_count = 0
    stationarity_skipped_window_count = 0
    decomposition_status_counts: Counter[str] = Counter()
    decomposition_reason_counts: Counter[str] = Counter()
    decomposition_basis_counts: Counter[str] = Counter()
    decomposition_limitation_counts: Counter[str] = Counter()
    decomposition_skipped_window_reason_counts: Counter[str] = Counter()
    decomposition_stationarity_status_counts: Counter[str] = Counter()
    decomposition_structural_break_status_counts: Counter[str] = Counter()
    decomposition_computed_window_count = 0
    decomposition_skipped_window_count = 0
    decomposition_structural_break_candidate_count = 0
    topology_limitation_counts: Counter[str] = Counter()
    dynamics_limitation_counts: Counter[str] = Counter()
    row_order_counts: Counter[str] = Counter()
    computed_from_counts: Counter[str] = Counter()
    cache_source_counts: Counter[str] = Counter()
    skipped_reason_counts: Counter[str] = Counter()
    tick_spread_conditioning_status_counts: Counter[str] = Counter()
    for item in target_summaries:
        topology_limitation_counts.update(
            _summary_key(value)
            for value in _string_list(item.get("topology_limitations"))
        )
        skipped_reason_counts.update(
            _summary_key(value)
            for value in _string_list(item.get("section_skip_reasons"))
        )
        tick_conditioning = _payload_mapping(
            item.get("tick_spread_conditioning")
        )
        tick_spread_conditioning_status_counts[
            _summary_key(tick_conditioning.get("status"))
        ] += 1
        for section in FINGERPRINT_DYNAMICS_SECTIONS:
            dynamics = _payload_mapping(item.get(section))
            dynamics_limitation_counts.update(
                _summary_key(value)
                for value in _string_list(dynamics.get("limitations"))
            )
            row_order = _summary_key(dynamics.get("row_order"))
            computed_from = _summary_key(dynamics.get("computed_from"))
            cache_source = _optional_summary_key(dynamics.get("cache_source"))
            if row_order != "unknown":
                row_order_counts[row_order] += 1
            if computed_from != "unknown":
                computed_from_counts[computed_from] += 1
            if cache_source:
                cache_source_counts[cache_source] += 1
        dependence = _payload_mapping(item.get("dependence"))
        dependence_status_counts[_summary_key(dependence.get("status"))] += 1
        dependence_reason = _optional_summary_key(dependence.get("reason"))
        if dependence_reason:
            dependence_reason_counts[dependence_reason] += 1
        acf_basis = _summary_key(dependence.get("acf_basis"))
        if acf_basis != "unknown":
            dependence_acf_basis_counts[acf_basis] += 1
        dependence_limitation_counts.update(
            _summary_key(value)
            for value in _string_list(dependence.get("limitations"))
        )
        dependence_skipped_lag_reason_counts.update(
            _counter_from_mapping(
                _payload_mapping(dependence.get("skipped_lag_reason_counts"))
            )
        )
        dependence_computed_lag_count += _int_payload(
            dependence.get("computed_lag_count")
        )
        dependence_skipped_lag_count += _int_payload(
            dependence.get("skipped_lag_count")
        )
        stationarity = _payload_mapping(item.get("stationarity_diagnostics"))
        stationarity_status_counts[
            _summary_key(stationarity.get("status"))
        ] += 1
        stationarity_reason = _optional_summary_key(stationarity.get("reason"))
        if stationarity_reason:
            stationarity_reason_counts[stationarity_reason] += 1
        stationarity_basis = _summary_key(stationarity.get("calculation_basis"))
        if stationarity_basis != "unknown":
            stationarity_basis_counts[stationarity_basis] += 1
        stationarity_limitation_counts.update(
            _summary_key(value)
            for value in _string_list(stationarity.get("limitations"))
        )
        stationarity_skipped_window_reason_counts.update(
            _counter_from_mapping(
                _payload_mapping(
                    stationarity.get("skipped_window_reason_counts")
                )
            )
        )
        stationarity_recommended_transform_counts.update(
            _summary_key(value)
            for value in _string_list(
                stationarity.get("recommended_transforms")
            )
        )
        stationarity_computed_window_count += _int_payload(
            stationarity.get("computed_window_count")
        )
        stationarity_skipped_window_count += _int_payload(
            stationarity.get("skipped_window_count")
        )
        decomposition = _payload_mapping(item.get("decomposition"))
        decomposition_status_counts[
            _summary_key(decomposition.get("status"))
        ] += 1
        decomposition_reason = _optional_summary_key(
            decomposition.get("reason")
        )
        if decomposition_reason:
            decomposition_reason_counts[decomposition_reason] += 1
        decomposition_basis = _summary_key(
            decomposition.get("calculation_basis")
        )
        if decomposition_basis != "unknown":
            decomposition_basis_counts[decomposition_basis] += 1
        decomposition_limitation_counts.update(
            _summary_key(value)
            for value in _string_list(decomposition.get("limitations"))
        )
        decomposition_skipped_window_reason_counts.update(
            _counter_from_mapping(
                _payload_mapping(
                    decomposition.get("skipped_window_reason_counts")
                )
            )
        )
        decomposition_computed_window_count += _int_payload(
            decomposition.get("computed_window_count")
        )
        decomposition_skipped_window_count += _int_payload(
            decomposition.get("skipped_window_count")
        )
        decomposition_stationarity_status_counts[
            _summary_key(
                _payload_mapping(decomposition.get("stationarity")).get(
                    "status"
                )
            )
        ] += 1
        structural_break = _payload_mapping(
            decomposition.get("structural_break")
        )
        decomposition_structural_break_status_counts[
            _summary_key(structural_break.get("status"))
        ] += 1
        decomposition_structural_break_candidate_count += _int_payload(
            structural_break.get("candidate_count")
        )

    profile_complete_count = sum(
        1
        for item in target_summaries
        if _payload_mapping(item.get("profile_completeness")).get(
            "calendar_profile_complete"
        )
        is True
    )
    profile_static_advisory_count = sum(
        1
        for item in target_summaries
        if _payload_mapping(item.get("profile_completeness")).get(
            "calendar_profile_static_advisory"
        )
        is True
    )

    included: list[JSONValue] = [
        dict(item) for item in target_limit_state.slice(target_summaries)
    ]
    omitted_count = max(0, len(target_summaries) - len(included))

    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION
        ),
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "target_count": len(target_summaries),
        "included_target_count": len(included),
        "omitted_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {"targets": target_limit_state.limit_payload()},
        "applicable_dynamics_status_counts": _counter_payload(
            applicable_status_counts
        ),
        "section_status_counts": section_status_counts,
        "dynamics_status_counts": dynamics_status_counts,
        "dynamics_reason_counts": dynamics_reason_counts,
        "dependence_status_counts": _counter_payload(dependence_status_counts),
        "dependence_reason_counts": _counter_payload(dependence_reason_counts),
        "dependence_acf_basis_counts": _counter_payload(
            dependence_acf_basis_counts
        ),
        "dependence_limitation_counts": _counter_payload(
            dependence_limitation_counts
        ),
        "dependence_skipped_lag_reason_counts": _counter_payload(
            dependence_skipped_lag_reason_counts
        ),
        "dependence_computed_lag_count": dependence_computed_lag_count,
        "dependence_skipped_lag_count": dependence_skipped_lag_count,
        "stationarity_status_counts": _counter_payload(
            stationarity_status_counts
        ),
        "stationarity_reason_counts": _counter_payload(
            stationarity_reason_counts
        ),
        "stationarity_basis_counts": _counter_payload(
            stationarity_basis_counts
        ),
        "stationarity_limitation_counts": _counter_payload(
            stationarity_limitation_counts
        ),
        "stationarity_skipped_window_reason_counts": _counter_payload(
            stationarity_skipped_window_reason_counts
        ),
        "stationarity_recommended_transform_counts": _counter_payload(
            stationarity_recommended_transform_counts
        ),
        "stationarity_computed_window_count": (
            stationarity_computed_window_count
        ),
        "stationarity_skipped_window_count": (
            stationarity_skipped_window_count
        ),
        "decomposition_status_counts": _counter_payload(
            decomposition_status_counts
        ),
        "decomposition_reason_counts": _counter_payload(
            decomposition_reason_counts
        ),
        "decomposition_basis_counts": _counter_payload(
            decomposition_basis_counts
        ),
        "decomposition_limitation_counts": _counter_payload(
            decomposition_limitation_counts
        ),
        "decomposition_skipped_window_reason_counts": _counter_payload(
            decomposition_skipped_window_reason_counts
        ),
        "decomposition_stationarity_status_counts": _counter_payload(
            decomposition_stationarity_status_counts
        ),
        "decomposition_structural_break_status_counts": _counter_payload(
            decomposition_structural_break_status_counts
        ),
        "decomposition_computed_window_count": (
            decomposition_computed_window_count
        ),
        "decomposition_skipped_window_count": (
            decomposition_skipped_window_count
        ),
        "decomposition_structural_break_candidate_count": (
            decomposition_structural_break_candidate_count
        ),
        "topology_limitation_counts": _counter_payload(
            topology_limitation_counts
        ),
        "dynamics_limitation_counts": _counter_payload(
            dynamics_limitation_counts
        ),
        "row_order_counts": _counter_payload(row_order_counts),
        "computed_from_counts": _counter_payload(computed_from_counts),
        "cache_source_counts": _counter_payload(cache_source_counts),
        "section_skip_reason_counts": _counter_payload(skipped_reason_counts),
        "tick_spread_conditioning_status_counts": _counter_payload(
            tick_spread_conditioning_status_counts
        ),
        "profile_completeness": {
            "calendar_profile_complete_count": profile_complete_count,
            "calendar_profile_incomplete_count": max(
                0,
                len(target_summaries) - profile_complete_count,
            ),
            "calendar_profile_static_advisory_count": (
                profile_static_advisory_count
            ),
        },
        "target_summaries": included,
    }


def series_fingerprint_readiness_risk_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int | None = DEFAULT_FINGERPRINT_READINESS_RISK_TARGET_LIMIT,
    section_limit: (
        int | None
    ) = DEFAULT_FINGERPRINT_READINESS_RISK_SECTION_LIMIT,
    reason_limit: int | None = DEFAULT_FINGERPRINT_READINESS_RISK_REASON_LIMIT,
    report_surface_evidence: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue] | None:
    """Return a bounded cross-section fingerprint readiness risk ranking."""
    finding_tuple = tuple(findings)
    readiness = series_fingerprint_readiness_summary(
        finding_tuple,
        target_limit=-1,
    )
    if readiness is None:
        return None
    regimes = series_fingerprint_regime_summary(
        finding_tuple,
        target_limit=-1,
        count_limit=DEFAULT_FINGERPRINT_REGIME_COUNT_LIMIT,
    )
    return fingerprint_readiness_risk_summary(
        readiness,
        regime_summary=regimes,
        target_limit=target_limit,
        section_limit=section_limit,
        reason_limit=reason_limit,
        report_surface_evidence=report_surface_evidence,
    )


def fingerprint_readiness_risk_summary(
    readiness_summary: Mapping[str, JSONValue],
    *,
    regime_summary: Mapping[str, JSONValue] | None = None,
    target_limit: int | None = DEFAULT_FINGERPRINT_READINESS_RISK_TARGET_LIMIT,
    section_limit: (
        int | None
    ) = DEFAULT_FINGERPRINT_READINESS_RISK_SECTION_LIMIT,
    reason_limit: int | None = DEFAULT_FINGERPRINT_READINESS_RISK_REASON_LIMIT,
    report_surface_evidence: Mapping[str, JSONValue] | None = None,
) -> dict[str, JSONValue] | None:
    """Rank fingerprint targets by already-emitted readiness evidence."""
    target_summaries = _payload_mapping_rows(
        readiness_summary.get("target_summaries")
    )
    if not target_summaries:
        return None
    target_limit_state = bounded_report_limit(
        target_limit,
        default_limit=DEFAULT_FINGERPRINT_READINESS_RISK_TARGET_LIMIT,
    )
    section_limit_state = bounded_report_limit(
        section_limit,
        default_limit=DEFAULT_FINGERPRINT_READINESS_RISK_SECTION_LIMIT,
    )
    reason_limit_state = bounded_report_limit(
        reason_limit,
        default_limit=DEFAULT_FINGERPRINT_READINESS_RISK_REASON_LIMIT,
    )
    regime_by_axis = _regime_summary_by_axis(regime_summary or {})
    target_risks = [
        _fingerprint_readiness_risk_target(
            target,
            regime_by_axis.get(
                _fingerprint_target_axis_key(
                    _payload_mapping(target.get("target_axis"))
                )
            ),
            section_limit=section_limit_state,
            reason_limit=reason_limit_state,
        )
        for target in target_summaries
    ]
    risk_targets = [
        item for item in target_risks if _int_payload(item.get("risk_score"))
    ]
    risk_targets.sort(key=_fingerprint_readiness_risk_sort_key)
    ranked_targets = [
        {**item, "rank": rank}
        for rank, item in enumerate(risk_targets, start=1)
    ]
    included = [dict(item) for item in target_limit_state.slice(ranked_targets)]
    omitted_count = max(0, len(ranked_targets) - len(included))
    risk_level_counts = Counter(
        _summary_key(item.get("risk_level")) for item in ranked_targets
    )
    reason_counts: Counter[str] = Counter()
    section_risk_counts: Counter[str] = Counter()
    for item in ranked_targets:
        reason_counts.update(
            _counter_from_mapping(_payload_mapping(item.get("reason_counts")))
        )
        for section in _payload_mapping_rows(item.get("section_risks")):
            section_risk_counts[_summary_key(section.get("section"))] += 1
    return {
        "schema_version": TIME_SERIES_FINGERPRINT_READINESS_RISK_SCHEMA_VERSION,
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "source_schema_version": _summary_key(
            readiness_summary.get("schema_version")
        ),
        "target_count": len(target_summaries),
        "risk_target_count": len(ranked_targets),
        "clean_target_count": max(
            0, len(target_summaries) - len(ranked_targets)
        ),
        "included_target_count": len(included),
        "omitted_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {
            "targets": target_limit_state.limit_payload(),
            "sections": section_limit_state.limit_payload(),
            "reasons": reason_limit_state.limit_payload(),
        },
        "risk_level_counts": _counter_payload(risk_level_counts),
        "reason_counts": _bounded_counter_payload(
            reason_counts,
            limit=reason_limit_state,
        ),
        "section_risk_counts": _bounded_counter_payload(
            section_risk_counts,
            limit=section_limit_state,
        ),
        "section_status_counts": dict(
            _payload_mapping(readiness_summary.get("section_status_counts"))
        ),
        "report_surface_evidence": _report_surface_risk_summary(
            report_surface_evidence or {}
        ),
        "target_risks": cast(JSONValue, included),
    }


def _fingerprint_readiness_risk_target(
    target: Mapping[str, JSONValue],
    regime: Mapping[str, JSONValue] | None,
    *,
    section_limit: BoundedReportLimit,
    reason_limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    axis = _payload_mapping(target.get("target_axis"))
    reason_counts: Counter[str] = Counter()
    section_risks: list[dict[str, JSONValue]] = []
    for section, status_value in _payload_mapping(
        target.get("section_statuses")
    ).items():
        if _fingerprint_section_is_non_applicable(section, axis):
            continue
        if section in {
            "calendar_regimes",
            "conditional_distributions",
            "dependence",
            "stationarity_diagnostics",
            "decomposition",
            "temporal_topology",
            *FINGERPRINT_DYNAMICS_SECTIONS,
        }:
            continue
        status = _summary_key(status_value)
        if status == "valid":
            continue
        reasons = _section_status_reasons(section, status, target)
        _add_section_risk(
            section_risks,
            reason_counts,
            section=_summary_key(section),
            status=status,
            reasons=reasons,
            base_score=_fingerprint_status_risk_score(status),
        )
    _add_topology_risks(section_risks, reason_counts, target)
    _add_dynamics_risks(section_risks, reason_counts, target, axis)
    _add_dependence_risks(section_risks, reason_counts, target)
    _add_stationarity_risks(section_risks, reason_counts, target)
    _add_decomposition_risks(section_risks, reason_counts, target)
    _add_regime_risks(section_risks, reason_counts, regime or {})
    compact_sections = _bounded_section_risks(
        section_risks,
        limit=section_limit,
        reason_limit=reason_limit,
    )
    compact_reasons = _bounded_counter_payload(
        reason_counts,
        limit=reason_limit,
    )
    risk_score = sum(_int_payload(item.get("score")) for item in section_risks)
    return {
        "target_axis": dict(axis),
        "risk_score": risk_score,
        "risk_level": _fingerprint_risk_level(risk_score),
        "reason_counts": compact_reasons,
        "reason_codes": list(compact_reasons),
        "section_risk_count": len(section_risks),
        "included_section_risk_count": len(compact_sections),
        "omitted_section_risk_count": max(
            0,
            len(section_risks) - len(compact_sections),
        ),
        "section_risks_truncated": len(compact_sections) < len(section_risks),
        "section_risks": compact_sections,
    }


def _add_topology_risks(
    section_risks: list[dict[str, JSONValue]],
    reason_counts: Counter[str],
    target: Mapping[str, JSONValue],
) -> None:
    reasons = [
        _summary_key(reason)
        for reason in _string_list(target.get("topology_limitations"))
    ]
    if reasons:
        _add_section_risk(
            section_risks,
            reason_counts,
            section="temporal_topology",
            status="limited",
            reasons=reasons,
            base_score=_fingerprint_status_risk_score("limited"),
        )


def _add_dynamics_risks(
    section_risks: list[dict[str, JSONValue]],
    reason_counts: Counter[str],
    target: Mapping[str, JSONValue],
    axis: Mapping[str, JSONValue],
) -> None:
    for section in FINGERPRINT_DYNAMICS_SECTIONS:
        if _fingerprint_section_is_non_applicable(section, axis):
            continue
        dynamics = _payload_mapping(target.get(section))
        status = _summary_key(dynamics.get("status"))
        if status in {"valid", "ok"}:
            continue
        reasons: list[str] = []
        primary_reason = _optional_summary_key(dynamics.get("reason"))
        if primary_reason:
            reasons.append(primary_reason)
        reasons.extend(
            _summary_key(reason)
            for reason in _string_list(dynamics.get("limitations"))
        )
        if (
            status == "unavailable"
            and _int_payload(dynamics.get("row_count")) == 0
        ):
            reasons.append("insufficient_rows")
        if not reasons and status not in {"unknown", "valid", "ok"}:
            reasons.append(status)
        _add_section_risk(
            section_risks,
            reason_counts,
            section=section,
            status=status,
            reasons=tuple(_ordered_unique(reasons)),
            base_score=_fingerprint_status_risk_score(status),
        )


def _fingerprint_section_is_non_applicable(
    section: str,
    axis: Mapping[str, JSONValue],
) -> bool:
    timeframe = _summary_key(axis.get("timeframe"))
    return (
        section == "microstructure_dynamics"
        and timeframe != TICK
        or section == "stationarity_diagnostics"
        and timeframe not in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES
        or section == "decomposition"
        and timeframe not in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES
    )


def _add_dependence_risks(
    section_risks: list[dict[str, JSONValue]],
    reason_counts: Counter[str],
    target: Mapping[str, JSONValue],
) -> None:
    dependence = _payload_mapping(target.get("dependence"))
    status = _summary_key(dependence.get("status"))
    reasons: list[str] = []
    primary_reason = _optional_summary_key(dependence.get("reason"))
    if primary_reason:
        reasons.append(primary_reason)
    reasons.extend(
        _summary_key(reason)
        for reason in _string_list(dependence.get("limitations"))
    )
    skipped_lag_count = _int_payload(dependence.get("skipped_lag_count"))
    if skipped_lag_count:
        reasons.append("skipped_dependence_lags")
        reasons.extend(
            _counter_from_mapping(
                _payload_mapping(dependence.get("skipped_lag_reason_counts"))
            ).elements()
        )
    if status in {"valid", "ok"} and not reasons:
        return
    if status == "skipped" and not reasons:
        reasons.append("not_emitted")
    _add_section_risk(
        section_risks,
        reason_counts,
        section="dependence",
        status=status,
        reasons=tuple(_ordered_unique(reasons)),
        base_score=(
            _fingerprint_status_risk_score(status)
            + min(20, skipped_lag_count * 2)
        ),
    )


def _add_stationarity_risks(
    section_risks: list[dict[str, JSONValue]],
    reason_counts: Counter[str],
    target: Mapping[str, JSONValue],
) -> None:
    section_statuses = _payload_mapping(target.get("section_statuses"))
    if "stationarity_diagnostics" not in section_statuses:
        return
    stationarity = _payload_mapping(target.get("stationarity_diagnostics"))
    status = _summary_key(stationarity.get("status"))
    reasons: list[str] = []
    primary_reason = _optional_summary_key(stationarity.get("reason"))
    if primary_reason:
        reasons.append(primary_reason)
    reasons.extend(
        _summary_key(reason)
        for reason in _string_list(stationarity.get("limitations"))
    )
    skipped_window_count = _int_payload(
        stationarity.get("skipped_window_count")
    )
    if skipped_window_count:
        reasons.append("skipped_rolling_windows")
        reasons.extend(
            _counter_from_mapping(
                _payload_mapping(
                    stationarity.get("skipped_window_reason_counts")
                )
            ).elements()
        )
    if status in {"valid", "ok"} and not reasons:
        return
    if status == "skipped" and not reasons:
        reasons.append("not_emitted")
    _add_section_risk(
        section_risks,
        reason_counts,
        section="stationarity_diagnostics",
        status=status,
        reasons=tuple(_ordered_unique(reasons)),
        base_score=(
            _fingerprint_status_risk_score(status)
            + min(20, skipped_window_count * 2)
        ),
    )


def _add_decomposition_risks(
    section_risks: list[dict[str, JSONValue]],
    reason_counts: Counter[str],
    target: Mapping[str, JSONValue],
) -> None:
    section_statuses = _payload_mapping(target.get("section_statuses"))
    if "decomposition" not in section_statuses:
        return
    decomposition = _payload_mapping(target.get("decomposition"))
    status = _summary_key(decomposition.get("status"))
    reasons: list[str] = []
    primary_reason = _optional_summary_key(decomposition.get("reason"))
    if primary_reason:
        reasons.append(primary_reason)
    reasons.extend(
        _summary_key(reason)
        for reason in _string_list(decomposition.get("limitations"))
    )
    skipped_window_count = _int_payload(
        decomposition.get("skipped_window_count")
    )
    if skipped_window_count:
        reasons.append("skipped_rolling_windows")
        reasons.extend(
            _counter_from_mapping(
                _payload_mapping(
                    decomposition.get("skipped_window_reason_counts")
                )
            ).elements()
        )
    if status in {"valid", "ok"} and not reasons:
        return
    if status == "skipped" and not reasons:
        reasons.append("not_emitted")
    _add_section_risk(
        section_risks,
        reason_counts,
        section="decomposition",
        status=status,
        reasons=tuple(_ordered_unique(reasons)),
        base_score=(
            _fingerprint_status_risk_score(status)
            + min(20, skipped_window_count * 2)
        ),
    )


def _add_regime_risks(
    section_risks: list[dict[str, JSONValue]],
    reason_counts: Counter[str],
    regime: Mapping[str, JSONValue],
) -> None:
    calendar = _payload_mapping(regime.get("calendar_regimes"))
    status = _summary_key(calendar.get("status"))
    if status in {"missing", "unavailable"}:
        _add_section_risk(
            section_risks,
            reason_counts,
            section="calendar_regimes",
            status=status,
            reasons=(f"{status}_regime_summary",),
            base_score=25 if status == "missing" else 35,
        )
    profile = _payload_mapping(calendar.get("calendar_profile"))
    if profile.get("static_advisory") is True:
        _add_section_risk(
            section_risks,
            reason_counts,
            section="calendar_profile",
            status="limited",
            reasons=("static_calendar_profile_advisory",),
            base_score=5,
        )
    cache_source = _optional_summary_key(calendar.get("cache_source"))
    if cache_source in {"none", "sibling"}:
        _add_section_risk(
            section_risks,
            reason_counts,
            section="calendar_regimes",
            status="limited",
            reasons=(f"cache_source_{cache_source}",),
            base_score=3,
        )
    conditional = _payload_mapping(regime.get("conditional_distributions"))
    conditional_status = _summary_key(conditional.get("status"))
    if conditional_status in {"absent", "unavailable"}:
        _add_section_risk(
            section_risks,
            reason_counts,
            section="conditional_distributions",
            status=conditional_status,
            reasons=(f"conditional_distribution_{conditional_status}",),
            base_score=12,
        )


def _add_section_risk(
    section_risks: list[dict[str, JSONValue]],
    reason_counts: Counter[str],
    *,
    section: str,
    status: str,
    reasons: Iterable[str],
    base_score: int,
) -> None:
    reason_tuple = tuple(
        _ordered_unique(_summary_key(reason) for reason in reasons)
    )
    if not reason_tuple and not base_score:
        return
    for reason in reason_tuple:
        reason_counts[reason] += 1
    score = base_score + sum(
        _fingerprint_reason_score(reason) for reason in reason_tuple
    )
    section_risks.append(
        {
            "section": section,
            "status": status,
            "score": score,
            "reasons": list(reason_tuple),
        }
    )


def _section_status_reasons(
    section: str,
    status: str,
    target: Mapping[str, JSONValue],
) -> tuple[str, ...]:
    if status == "skipped":
        return tuple(
            _summary_key(reason)
            for reason in _string_list(target.get("section_skip_reasons"))
        ) or ("not_emitted",)
    if status == "unavailable":
        return ("section_unavailable",)
    if status == "limited":
        return (f"{section}_limited",)
    return (status,)


def _bounded_section_risks(
    section_risks: list[dict[str, JSONValue]],
    *,
    limit: BoundedReportLimit,
    reason_limit: BoundedReportLimit,
) -> list[JSONValue]:
    ordered = sorted(
        section_risks,
        key=lambda item: (
            -_int_payload(item.get("score")),
            _summary_key(item.get("section")),
            _summary_key(item.get("status")),
        ),
    )
    bounded: list[JSONValue] = []
    for item in limit.slice(ordered):
        reasons = [
            _summary_key(reason) for reason in _string_list(item.get("reasons"))
        ]
        bounded.append(
            {
                **item,
                "reasons": reason_limit.slice(reasons),
                "reason_count": len(reasons),
                "included_reason_count": len(reason_limit.slice(reasons)),
                "omitted_reason_count": max(
                    0,
                    len(reasons) - len(reason_limit.slice(reasons)),
                ),
            }
        )
    return bounded


def _bounded_counter_payload(
    counter: Counter[str],
    *,
    limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    ordered = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    return {key: count for key, count in limit.slice(ordered)}


def _fingerprint_readiness_risk_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[object, ...]:
    axis = _payload_mapping(target.get("target_axis"))
    return (
        -_int_payload(target.get("risk_score")),
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
    )


def _fingerprint_status_risk_score(status: str) -> int:
    scores = {
        "unavailable": 40,
        "missing": 35,
        "skipped": 25,
        "limited": 15,
        "absent": 10,
    }
    return scores.get(status, 0)


def _fingerprint_reason_score(reason: str) -> int:
    scores = {
        "missing_required_columns": 40,
        "source_unreadable": 40,
        "unsupported_target_kind": 35,
        "unsupported_timeframe": 30,
        "no_parsed_timestamps": 30,
        "invalid_timestamps_skipped": 25,
        "non_monotonic_timestamp_order": 25,
        "duplicate_timestamps": 20,
        "suspicious_gaps": 20,
        "insufficient_rows": 18,
        "insufficient_sequence_rows": 18,
        "insufficient_sample_count": 15,
        "zero_variance": 15,
        "skipped_dependence_lags": 12,
        "skipped_rolling_windows": 12,
        "not_emitted": 10,
        "missing_regime_summary": 10,
        "unavailable_regime_summary": 20,
        "conditional_distribution_absent": 8,
        "conditional_distribution_unavailable": 15,
        "static_calendar_profile_advisory": 5,
        "cache_source_sibling": 3,
        "cache_source_none": 5,
    }
    return scores.get(reason, 3)


def _fingerprint_risk_level(score: int) -> str:
    if score >= 80:
        return "high"
    if score >= 35:
        return "medium"
    if score > 0:
        return "low"
    return "clean"


def _regime_summary_by_axis(
    regime_summary: Mapping[str, JSONValue],
) -> dict[tuple[str, str, str, str, str], Mapping[str, JSONValue]]:
    return {
        _fingerprint_target_axis_key(
            _payload_mapping(row.get("target_axis"))
        ): row
        for row in _payload_mapping_rows(regime_summary.get("target_summaries"))
    }


def _fingerprint_target_axis_key(
    axis: Mapping[str, JSONValue],
) -> tuple[str, str, str, str, str]:
    return (
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
    )


def _report_surface_risk_summary(
    evidence: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    rows = _payload_mapping_rows(evidence.get("surface_matrix"))
    metadata_counts: Counter[str] = Counter()
    bounded_counts: Counter[str] = Counter()
    cli_counts: Counter[str] = Counter()
    for row in rows:
        metadata_counts[_summary_key(row.get("report_metadata_state"))] += 1
        bounded_counts[_summary_key(row.get("bounded_payload_state"))] += 1
        cli_counts[_summary_key(row.get("cli_summary_state"))] += 1
    return {
        "schema_version": _optional_summary_key(evidence.get("schema_version")),
        "surface_count": len(rows),
        "report_metadata_state_counts": _counter_payload(metadata_counts),
        "bounded_payload_state_counts": _counter_payload(bounded_counts),
        "cli_summary_state_counts": _counter_payload(cli_counts),
    }


def _series_fingerprint_readiness_target_summaries(
    findings: Iterable[QualityFinding],
) -> list[dict[str, JSONValue]]:
    target_summaries: list[dict[str, JSONValue]] = []
    for finding in findings:
        if finding.rule_id != SERIES_FINGERPRINT_RULE_ID:
            continue
        payload = finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        if not isinstance(payload, Mapping):
            continue
        target_summaries.append(
            _fingerprint_readiness_target_summary(finding, payload)
        )
    target_summaries.sort(key=_fingerprint_readiness_target_sort_key)
    return target_summaries


def _fingerprint_readiness_target_summary(
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    target_axis = _topology_target_axis(
        finding,
        _payload_mapping(payload.get("target_axis")),
    )
    audit = _payload_mapping(payload.get("fingerprint_audit"))
    section_statuses = _fingerprint_readiness_section_statuses(
        finding,
        payload,
        audit,
    )
    expected_sections = _fingerprint_audit_string_list(
        audit.get("sections_expected"),
        fallback=_fingerprint_expected_sections(finding.target),
    )
    emitted_sections = _fingerprint_audit_string_list(
        audit.get("sections_emitted"),
        fallback=tuple(
            section
            for section in FINGERPRINT_AUDIT_SECTIONS
            if section in payload
        ),
    )
    skipped_sections = _payload_mapping(audit.get("sections_skipped"))
    section_skip_reasons: list[JSONValue] = [
        _summary_key(_payload_mapping(value).get("reason"))
        for value in skipped_sections.values()
        if isinstance(value, Mapping)
    ]
    microstructure_readiness = _fingerprint_readiness_for_section(
        "microstructure_dynamics",
        finding,
        payload,
        audit,
    )
    applicable_section = _applicable_dynamics_section(
        _summary_key(target_axis.get("timeframe"))
    )
    applicable_readiness = (
        microstructure_readiness
        if applicable_section == "microstructure_dynamics"
        else {
            "status": "unavailable",
            "reason": "unsupported_timeframe",
        }
    )
    topology = _payload_mapping(payload.get("temporal_topology"))
    dependence_readiness = _fingerprint_readiness_dependence_summary(
        finding,
        payload,
        section_statuses=section_statuses,
        skipped_sections=skipped_sections,
    )
    stationarity_readiness = _fingerprint_readiness_stationarity_summary(
        finding,
        payload,
        section_statuses=section_statuses,
        skipped_sections=skipped_sections,
    )
    decomposition_readiness = _fingerprint_readiness_decomposition_summary(
        finding,
        payload,
        section_statuses=section_statuses,
        skipped_sections=skipped_sections,
    )

    return {
        "target_axis": target_axis,
        "source_kind": _summary_key(
            _payload_mapping(payload.get("source")).get("kind")
        ),
        "source_reason": _optional_summary_key(
            _payload_mapping(payload.get("source")).get("reason")
        ),
        "sections_expected_count": len(expected_sections),
        "sections_emitted_count": len(emitted_sections),
        "sections_skipped_count": len(skipped_sections),
        "section_skip_reasons": section_skip_reasons,
        "section_statuses": section_statuses,
        "applicable_dynamics_section": applicable_section or "none",
        "applicable_dynamics_status": _summary_key(
            applicable_readiness.get("status")
        ),
        "applicable_dynamics_reason": _optional_summary_key(
            applicable_readiness.get("reason")
        ),
        "topology": _fingerprint_readiness_topology_summary(topology),
        "topology_limitations": [
            value for value in _sequence_dynamics_limitations(topology)
        ],
        "profile_completeness": _fingerprint_readiness_profile_summary(audit),
        "tick_spread_conditioning": (
            _fingerprint_readiness_tick_spread_conditioning(audit)
        ),
        "microstructure_dynamics": (
            _fingerprint_readiness_microstructure_summary(
                payload,
                microstructure_readiness,
            )
        ),
        "dependence": dependence_readiness,
        "stationarity_diagnostics": stationarity_readiness,
        "decomposition": decomposition_readiness,
    }


def _fingerprint_readiness_section_statuses(
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
    audit: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    statuses = _payload_mapping(audit.get("section_statuses"))
    if statuses:
        return {
            section: _summary_key(statuses.get(section))
            for section in FINGERPRINT_AUDIT_SECTIONS
            if section in statuses
        }
    sections = _ordered_unique(
        (
            *_fingerprint_expected_sections(finding.target),
            *(
                section
                for section in FINGERPRINT_AUDIT_SECTIONS
                if section in payload
            ),
        )
    )
    return {
        section: _fingerprint_section_status(section, payload)
        for section in sections
    }


def _fingerprint_audit_string_list(
    value: JSONValue,
    *,
    fallback: tuple[str, ...],
) -> tuple[str, ...]:
    values = tuple(str(item) for item in _string_list(value))
    return values or fallback


def _fingerprint_readiness_for_section(
    section: str,
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
    audit: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    readiness = _payload_mapping(
        _payload_mapping(audit.get("dynamics_readiness")).get(section)
    )
    if not readiness:
        readiness = _fingerprint_dynamics_readiness(
            section,
            payload,
            target=finding.target,
        )
    return {
        "status": _summary_key(readiness.get("status")),
        "reason": _optional_summary_key(readiness.get("reason")),
        "basis": _summary_key(readiness.get("basis")),
        "row_order": _summary_key(readiness.get("row_order")),
        "computed_from": _summary_key(readiness.get("computed_from")),
        "cache_source": _optional_summary_key(readiness.get("cache_source")),
        "regular_grid": readiness.get("regular_grid") is True,
        "limitations": _string_list(readiness.get("limitations")),
        "row_count": _int_payload(readiness.get("row_count")),
        "sampled_row_count": _int_payload(readiness.get("sampled_row_count")),
        "usable_row_count": _int_payload(readiness.get("usable_row_count")),
        "invalid_row_count": _int_payload(readiness.get("invalid_row_count")),
        "partial_row_count": _int_payload(readiness.get("partial_row_count")),
        "truncated": readiness.get("truncated") is True,
    }


def _applicable_dynamics_section(timeframe: str) -> str:
    if timeframe == TICK:
        return "microstructure_dynamics"
    return ""


def _fingerprint_readiness_topology_summary(
    topology: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        "row_count": _int_payload(topology.get("row_count")),
        "parsed_row_count": _optional_int_payload(
            topology.get("parsed_row_count")
        ),
        "invalid_timestamp_count": _int_payload(
            topology.get("invalid_timestamp_count")
        ),
        "duplicate_timestamp_count": _int_payload(
            topology.get("duplicate_timestamp_count")
        ),
        "non_monotonic_count": _int_payload(
            topology.get("non_monotonic_count")
        ),
        "suspicious_gap_count": _int_payload(
            topology.get("suspicious_gap_count")
        ),
        "expected_session_closure_count": _int_payload(
            topology.get("expected_session_closure_count")
        ),
        "weekend_activity_count": _int_payload(
            topology.get("weekend_activity_count")
        ),
        "sampling_basis": _summary_key(topology.get("sampling_basis")),
        "computed_from": _summary_key(topology.get("computed_from")),
        "cache_source": _optional_summary_key(topology.get("cache_source")),
    }


def _fingerprint_readiness_profile_summary(
    audit: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    profile = _payload_mapping(audit.get("profile_completeness"))
    return {
        "source": _summary_key(profile.get("source")),
        "calendar_profile_complete": (
            profile.get("calendar_profile_complete") is True
        ),
        "missing_optional_calendar_data": (
            profile.get("missing_optional_calendar_data") is True
        ),
        "calendar_profile_name": _summary_key(
            profile.get("calendar_profile_name")
        ),
        "calendar_profile_source": _summary_key(
            profile.get("calendar_profile_source")
        ),
        "calendar_profile_version": _summary_key(
            profile.get("calendar_profile_version")
        ),
        "calendar_profile_static_advisory": (
            profile.get("calendar_profile_static_advisory") is True
        ),
    }


def _fingerprint_readiness_tick_spread_conditioning(
    audit: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    eligibility = _payload_mapping(
        _payload_mapping(audit.get("conditional_distribution_eligibility")).get(
            "tick_spread"
        )
    )
    return {
        "eligible": eligibility.get("eligible") is True,
        "status": _summary_key(eligibility.get("status")),
        "reason": _optional_summary_key(eligibility.get("reason")),
        "emitted": eligibility.get("emitted") is True,
    }


def _fingerprint_readiness_microstructure_summary(
    payload: Mapping[str, JSONValue],
    readiness: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    summary = dict(readiness)
    dynamics = _payload_mapping(payload.get("microstructure_dynamics"))
    if not dynamics:
        return summary
    summary.update(
        {
            "interarrival_ms": _compact_numeric_summary(
                dynamics.get("interarrival_ms")
            ),
            "spread": _compact_numeric_summary(dynamics.get("spread")),
            "spread_change": _compact_numeric_summary(
                dynamics.get("spread_change")
            ),
            "absolute_spread_change": _compact_numeric_summary(
                dynamics.get("absolute_spread_change")
            ),
            "zero_spread_count": _int_payload(
                dynamics.get("zero_spread_count")
            ),
            "negative_spread_count": _int_payload(
                dynamics.get("negative_spread_count")
            ),
            "zero_spread_rate": _optional_float_payload(
                dynamics.get("zero_spread_rate")
            ),
            "negative_spread_rate": _optional_float_payload(
                dynamics.get("negative_spread_rate")
            ),
            "spread_jump": _compact_event_summary(
                dynamics.get("spread_jump"),
                count_key="count",
                rate_key="rate",
            ),
            "stale_quote": _compact_event_summary(
                dynamics.get("stale_quote"),
                count_key="repeat_count",
                rate_key="repeat_rate",
                extra_count_keys=("run_count", "affected_row_count"),
            ),
            "burst": _compact_event_summary(
                dynamics.get("burst"),
                count_key="interval_count",
                rate_key="burst_rate",
                extra_count_keys=("run_count", "tick_count"),
            ),
            "one_sided_movement": _compact_event_summary(
                dynamics.get("one_sided_movement"),
                count_key="count",
                rate_key="rate",
                extra_count_keys=(
                    "bid_only_count",
                    "ask_only_count",
                    "run_count",
                ),
            ),
        }
    )
    return summary


def _fingerprint_readiness_dependence_summary(
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
    *,
    section_statuses: Mapping[str, JSONValue],
    skipped_sections: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    dependence = _payload_mapping(payload.get("dependence"))
    if not dependence:
        status = _summary_key(section_statuses.get("dependence") or "skipped")
        if status not in {"skipped", "unavailable"}:
            status = "skipped"
        skipped = _payload_mapping(skipped_sections.get("dependence"))
        reason = _optional_summary_key(skipped.get("reason"))
        if reason is None:
            reason = _fingerprint_section_skip_reason(
                "dependence",
                payload,
                target=finding.target,
            )
        return _empty_dependence_readiness(status=status, reason=reason)

    series = {
        name: _compact_acf_series_summary(value)
        for name, value in sorted(dependence.items())
        if name.endswith("_acf") and isinstance(value, Mapping)
    }
    skipped_reason_counts: Counter[str] = Counter()
    for summary in series.values():
        skipped_reason_counts.update(
            _counter_from_mapping(
                _payload_mapping(summary.get("skipped_lag_reason_counts"))
            )
        )
    computed_lag_count = _int_payload(dependence.get("computed_lag_count"))
    if computed_lag_count <= 0:
        computed_lag_count = sum(
            _int_payload(summary.get("computed_lag_count"))
            for summary in series.values()
        )
    skipped_lag_count = _int_payload(dependence.get("skipped_lag_count"))
    if skipped_lag_count <= 0:
        skipped_lag_count = sum(
            _int_payload(summary.get("skipped_lag_count"))
            for summary in series.values()
        )
    lag_limit_state = bounded_report_limit(
        None,
        default_limit=DEFAULT_FINGERPRINT_READINESS_LAG_LIMIT,
        minimum_limit=0,
        allow_unbounded=True,
    )
    lags = _int_sequence_payload(dependence.get("lags"))
    included_lags = lag_limit_state.slice(lags)
    omitted_lag_count = max(0, len(lags) - len(included_lags))
    result: dict[str, JSONValue] = {
        "status": _dependence_section_status(dependence),
        "reason": _optional_summary_key(dependence.get("reason")),
        "basis": _summary_key(dependence.get("basis")),
        "acf_basis": _summary_key(dependence.get("acf_basis")),
        "row_order": _summary_key(dependence.get("row_order")),
        "computed_from": _summary_key(dependence.get("computed_from")),
        "cache_source": _optional_summary_key(dependence.get("cache_source")),
        "regular_grid": dependence.get("regular_grid") is True,
        "limitations": _string_list(dependence.get("limitations")),
        "row_count": _int_payload(dependence.get("row_count")),
        "sampled_row_count": _int_payload(dependence.get("sampled_row_count")),
        "usable_row_count": _int_payload(dependence.get("usable_row_count")),
        "invalid_row_count": _int_payload(dependence.get("invalid_row_count")),
        "partial_row_count": _int_payload(dependence.get("partial_row_count")),
        "truncated": dependence.get("truncated") is True,
        "lag_count": len(lags),
        "lag_limit": lag_limit_state.effective_limit,
        "lags": list(included_lags),
        "included_lag_count": len(included_lags),
        "omitted_lag_count": omitted_lag_count,
        "lags_truncated": omitted_lag_count > 0,
        "limit_metadata": {"lags": lag_limit_state.limit_payload()},
        "computed_lag_count": computed_lag_count,
        "skipped_lag_count": skipped_lag_count,
        "skipped_lag_reason_counts": _counter_payload(skipped_reason_counts),
        "series_count": len(series),
    }
    result["series"] = cast(JSONValue, series)
    return result


def _empty_dependence_readiness(
    *,
    status: str,
    reason: str | None,
) -> dict[str, JSONValue]:
    lag_limit_state = bounded_report_limit(
        None,
        default_limit=DEFAULT_FINGERPRINT_READINESS_LAG_LIMIT,
        minimum_limit=0,
        allow_unbounded=True,
    )
    return {
        "status": status,
        "reason": reason,
        "basis": "unknown",
        "acf_basis": "unknown",
        "row_order": "unknown",
        "computed_from": "unknown",
        "cache_source": None,
        "regular_grid": False,
        "limitations": [],
        "row_count": 0,
        "sampled_row_count": 0,
        "usable_row_count": 0,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
        "lag_count": 0,
        "lag_limit": lag_limit_state.effective_limit,
        "lags": [],
        "included_lag_count": 0,
        "omitted_lag_count": 0,
        "lags_truncated": False,
        "limit_metadata": {"lags": lag_limit_state.limit_payload()},
        "computed_lag_count": 0,
        "skipped_lag_count": 0,
        "skipped_lag_reason_counts": {},
        "series_count": 0,
        "series": {},
    }


def _fingerprint_readiness_stationarity_summary(
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
    *,
    section_statuses: Mapping[str, JSONValue],
    skipped_sections: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    stationarity = _payload_mapping(payload.get("stationarity_diagnostics"))
    if not stationarity:
        status = _summary_key(
            section_statuses.get("stationarity_diagnostics") or "skipped"
        )
        if status not in {"skipped", "unavailable"}:
            status = "skipped"
        skipped = _payload_mapping(
            skipped_sections.get("stationarity_diagnostics")
        )
        reason = _optional_summary_key(skipped.get("reason"))
        if reason is None:
            reason = _fingerprint_section_skip_reason(
                "stationarity_diagnostics",
                payload,
                target=finding.target,
            )
        return _empty_stationarity_readiness(status=status, reason=reason)

    sample_counts = _payload_mapping(stationarity.get("sample_counts"))
    window_payloads: dict[str, JSONValue] = {
        str(window): _compact_stationarity_window_summary(window_payload)
        for window, window_payload in sorted(
            _payload_mapping(stationarity.get("rolling_windows")).items()
        )
    }
    skipped_reason_counts = _counter_from_mapping(
        _payload_mapping(stationarity.get("skipped_window_reason_counts"))
    )
    result: dict[str, JSONValue] = {
        "status": _stationarity_section_status(stationarity),
        "reason": _optional_summary_key(stationarity.get("reason")),
        "basis": _summary_key(stationarity.get("basis")),
        "calculation_basis": _summary_key(
            stationarity.get("calculation_basis")
        ),
        "row_order": _summary_key(stationarity.get("row_order")),
        "computed_from": _summary_key(stationarity.get("computed_from")),
        "cache_source": _optional_summary_key(stationarity.get("cache_source")),
        "regular_grid": stationarity.get("regular_grid") is True,
        "metric": _summary_key(stationarity.get("metric")),
        "limitations": _string_list(stationarity.get("limitations")),
        "row_count": _int_payload(stationarity.get("row_count")),
        "sampled_row_count": _int_payload(
            stationarity.get("sampled_row_count")
        ),
        "usable_row_count": _int_payload(stationarity.get("usable_row_count")),
        "invalid_row_count": _int_payload(
            stationarity.get("invalid_row_count")
        ),
        "partial_row_count": _int_payload(
            stationarity.get("partial_row_count")
        ),
        "truncated": stationarity.get("truncated") is True,
        "level_sample_count": _int_payload(sample_counts.get("level")),
        "return_sample_count": _int_payload(sample_counts.get("return")),
        "windows": list(_int_sequence_payload(stationarity.get("windows"))),
        "rounding_digits": _int_payload(stationarity.get("rounding_digits")),
        "computed_window_count": _int_payload(
            stationarity.get("computed_window_count")
        ),
        "skipped_window_count": _int_payload(
            stationarity.get("skipped_window_count")
        ),
        "skipped_window_reason_counts": _counter_payload(skipped_reason_counts),
        "recommended_transforms": _string_list(
            stationarity.get("recommended_transforms")
        ),
        "zero_variance_metrics": _string_list(
            stationarity.get("zero_variance_metrics")
        ),
        "distribution_shift": (
            _compact_stationarity_distribution_shift_summary(
                stationarity.get("first_middle_last_distribution_shift")
            )
        ),
        "rolling_windows": window_payloads,
    }
    return result


def _empty_stationarity_readiness(
    *,
    status: str,
    reason: str | None,
) -> dict[str, JSONValue]:
    return {
        "status": status,
        "reason": reason,
        "basis": "unknown",
        "calculation_basis": "unknown",
        "row_order": "unknown",
        "computed_from": "unknown",
        "cache_source": None,
        "regular_grid": False,
        "metric": "unknown",
        "limitations": [],
        "row_count": 0,
        "sampled_row_count": 0,
        "usable_row_count": 0,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
        "level_sample_count": 0,
        "return_sample_count": 0,
        "windows": [],
        "rounding_digits": 0,
        "computed_window_count": 0,
        "skipped_window_count": 0,
        "skipped_window_reason_counts": {},
        "recommended_transforms": [],
        "zero_variance_metrics": [],
        "distribution_shift": {},
        "rolling_windows": {},
    }


def _fingerprint_readiness_decomposition_summary(
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
    *,
    section_statuses: Mapping[str, JSONValue],
    skipped_sections: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    decomposition = _payload_mapping(payload.get("decomposition"))
    if not decomposition:
        status = _summary_key(
            section_statuses.get("decomposition") or "skipped"
        )
        if status not in {"skipped", "unavailable"}:
            status = "skipped"
        skipped = _payload_mapping(skipped_sections.get("decomposition"))
        reason = _optional_summary_key(skipped.get("reason"))
        if reason is None:
            reason = _fingerprint_section_skip_reason(
                "decomposition",
                payload,
                target=finding.target,
            )
        return _empty_decomposition_readiness(status=status, reason=reason)

    sample_counts = _payload_mapping(decomposition.get("sample_counts"))
    stationarity_basis = _payload_mapping(
        decomposition.get("stationarity_basis")
    )
    structural_break = _payload_mapping(
        decomposition.get("structural_break_proxy")
    )
    trend = _payload_mapping(decomposition.get("trend_proxy"))
    strongest = _payload_mapping(structural_break.get("strongest_candidate"))
    return {
        "status": _decomposition_section_status(decomposition),
        "reason": _optional_summary_key(decomposition.get("reason")),
        "basis": _summary_key(decomposition.get("basis")),
        "calculation_basis": _summary_key(
            decomposition.get("calculation_basis")
        ),
        "row_order": _summary_key(decomposition.get("row_order")),
        "computed_from": _summary_key(decomposition.get("computed_from")),
        "cache_source": _optional_summary_key(
            decomposition.get("cache_source")
        ),
        "regular_grid": decomposition.get("regular_grid") is True,
        "metric": _summary_key(decomposition.get("metric")),
        "limitations": _string_list(decomposition.get("limitations")),
        "row_count": _int_payload(decomposition.get("row_count")),
        "sampled_row_count": _int_payload(
            decomposition.get("sampled_row_count")
        ),
        "usable_row_count": _int_payload(decomposition.get("usable_row_count")),
        "invalid_row_count": _int_payload(
            decomposition.get("invalid_row_count")
        ),
        "partial_row_count": _int_payload(
            decomposition.get("partial_row_count")
        ),
        "truncated": decomposition.get("truncated") is True,
        "level_sample_count": _int_payload(sample_counts.get("level")),
        "return_sample_count": _int_payload(sample_counts.get("return")),
        "windows": list(_int_sequence_payload(decomposition.get("windows"))),
        "rounding_digits": _int_payload(decomposition.get("rounding_digits")),
        "computed_window_count": _int_payload(
            decomposition.get("computed_window_count")
        ),
        "skipped_window_count": _int_payload(
            decomposition.get("skipped_window_count")
        ),
        "skipped_window_reason_counts": _counter_payload(
            _counter_from_mapping(
                _payload_mapping(
                    decomposition.get("skipped_window_reason_counts")
                )
            )
        ),
        "stationarity": {
            "status": _summary_key(stationarity_basis.get("status")),
            "reason": _optional_summary_key(stationarity_basis.get("reason")),
            "stationarity_status": _summary_key(
                stationarity_basis.get("stationarity_status")
            ),
            "zero_variance_metrics": _string_list(
                stationarity_basis.get("zero_variance_metrics")
            ),
            "recommended_transforms": _string_list(
                stationarity_basis.get("recommended_transforms")
            ),
        },
        "trend": {
            "status": _summary_key(trend.get("status")),
            "direction": _summary_key(trend.get("direction")),
            "trend_strength": _optional_float_payload(
                trend.get("trend_strength")
            ),
        },
        "structural_break": {
            "status": _summary_key(structural_break.get("status")),
            "candidate_count": _int_payload(
                structural_break.get("candidate_count")
            ),
            "strongest_score": (
                _optional_float_payload(strongest.get("score"))
                if strongest
                else None
            ),
        },
        "training_projection": dict(
            _payload_mapping(decomposition.get("training_projection"))
        ),
    }


def _empty_decomposition_readiness(
    *,
    status: str,
    reason: str | None,
) -> dict[str, JSONValue]:
    return {
        "status": status,
        "reason": reason,
        "basis": "unknown",
        "calculation_basis": "unknown",
        "row_order": "unknown",
        "computed_from": "unknown",
        "cache_source": None,
        "regular_grid": False,
        "metric": "unknown",
        "limitations": [],
        "row_count": 0,
        "sampled_row_count": 0,
        "usable_row_count": 0,
        "invalid_row_count": 0,
        "partial_row_count": 0,
        "truncated": False,
        "level_sample_count": 0,
        "return_sample_count": 0,
        "windows": [],
        "rounding_digits": 0,
        "computed_window_count": 0,
        "skipped_window_count": 0,
        "skipped_window_reason_counts": {},
        "stationarity": {},
        "trend": {},
        "structural_break": {},
        "training_projection": {},
    }


def _compact_stationarity_window_summary(
    value: JSONValue,
) -> dict[str, JSONValue]:
    window = _payload_mapping(value)
    result: dict[str, JSONValue] = {
        "status": _summary_key(window.get("status")),
        "reason": _optional_summary_key(window.get("reason")),
        "window": _int_payload(window.get("window")),
        "sample_counts": dict(_payload_mapping(window.get("sample_counts"))),
    }
    if result["status"] != "computed":
        result["required_sample_count"] = _int_payload(
            window.get("required_sample_count")
        )
        return result
    for key in (
        "level_rolling_mean_drift",
        "level_rolling_variance_drift",
        "return_rolling_mean_drift",
        "return_rolling_variance_drift",
    ):
        result[key] = _compact_stationarity_change_summary(window.get(key))
    return result


def _compact_stationarity_distribution_shift_summary(
    value: JSONValue,
) -> dict[str, JSONValue]:
    shift = _payload_mapping(value)
    if not shift:
        return {}
    return {
        "status": _summary_key(shift.get("status")),
        "reason": _optional_summary_key(shift.get("reason")),
        "level": _compact_stationarity_segment_shift_summary(
            shift.get("level")
        ),
        "return": _compact_stationarity_segment_shift_summary(
            shift.get("return")
        ),
    }


def _compact_stationarity_segment_shift_summary(
    value: JSONValue,
) -> dict[str, JSONValue]:
    shift = _payload_mapping(value)
    if not shift:
        return {}
    result: dict[str, JSONValue] = {
        "status": _summary_key(shift.get("status")),
        "reason": _optional_summary_key(shift.get("reason")),
        "sample_count": _int_payload(shift.get("sample_count")),
        "segment_size": _int_payload(shift.get("segment_size")),
    }
    for key in (
        "mean_shift_first_to_last",
        "median_shift_first_to_last",
        "variance_shift_first_to_last",
    ):
        result[key] = _compact_stationarity_change_summary(shift.get(key))
    return result


def _compact_stationarity_change_summary(
    value: JSONValue,
) -> dict[str, JSONValue]:
    change = _payload_mapping(value)
    if not change:
        return {}
    return {
        "first": _optional_float_payload(change.get("first")),
        "last": _optional_float_payload(change.get("last")),
        "signed_change": _optional_float_payload(change.get("signed_change")),
        "absolute_change": _optional_float_payload(
            change.get("absolute_change")
        ),
        "relative_change": _optional_float_payload(
            change.get("relative_change")
        ),
    }


def _compact_acf_series_summary(value: JSONValue) -> dict[str, JSONValue]:
    acf = _payload_mapping(value)
    skipped_reason_counts = _acf_skipped_lag_reason_counts(acf)
    computed_lag_count = _int_payload(acf.get("computed_lag_count"))
    if computed_lag_count <= 0:
        computed_lag_count = len(_payload_mapping(acf.get("lag_acf")))
    skipped_lag_count = _int_payload(acf.get("skipped_lag_count"))
    if skipped_lag_count <= 0:
        skipped_lag_count = sum(skipped_reason_counts.values())
    return {
        "sample_count": _int_payload(acf.get("sample_count")),
        "computed_lag_count": computed_lag_count,
        "skipped_lag_count": skipped_lag_count,
        "skipped_lag_reason_counts": _counter_payload(skipped_reason_counts),
    }


def _acf_skipped_lag_reason_counts(
    acf: Mapping[str, JSONValue],
) -> Counter[str]:
    counts: Counter[str] = Counter()
    for skipped in _payload_mapping(acf.get("skipped_lags")).values():
        reason = _optional_summary_key(_payload_mapping(skipped).get("reason"))
        if reason:
            counts[reason] += 1
    return counts


def _compact_numeric_summary(value: JSONValue) -> dict[str, JSONValue]:
    numeric = _payload_mapping(value)
    if not numeric:
        return {}
    quantiles = _payload_mapping(numeric.get("quantiles"))
    return {
        "count": _int_payload(numeric.get("count")),
        "min": _optional_float_payload(numeric.get("min")),
        "max": _optional_float_payload(numeric.get("max")),
        "mean": _optional_float_payload(numeric.get("mean")),
        "median": _optional_float_payload(numeric.get("median")),
        "mad": _optional_float_payload(numeric.get("mad")),
        "p95": _optional_float_payload(quantiles.get("0.95")),
        "p99": _optional_float_payload(quantiles.get("0.99")),
    }


def _compact_event_summary(
    value: JSONValue,
    *,
    count_key: str,
    rate_key: str,
    extra_count_keys: tuple[str, ...] = (),
) -> dict[str, JSONValue]:
    event = _payload_mapping(value)
    if not event:
        return {}
    summary: dict[str, JSONValue] = {
        count_key: _int_payload(event.get(count_key)),
        rate_key: _optional_float_payload(event.get(rate_key)),
    }
    for key in extra_count_keys:
        summary[key] = _int_payload(event.get(key))
    threshold = event.get("threshold")
    if threshold is not None:
        summary["threshold"] = _optional_float_payload(threshold)
    return summary


def _fingerprint_readiness_target_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[object, ...]:
    axis = _payload_mapping(target.get("target_axis"))
    dependence = _payload_mapping(target.get("dependence"))
    stationarity = _payload_mapping(target.get("stationarity_diagnostics"))
    decomposition = _payload_mapping(target.get("decomposition"))
    readiness_rank = min(
        _fingerprint_readiness_status_rank(
            _summary_key(target.get("applicable_dynamics_status"))
        ),
        _fingerprint_readiness_status_rank(
            _summary_key(dependence.get("status"))
        ),
        _fingerprint_readiness_status_rank(
            _summary_key(stationarity.get("status"))
        ),
        _fingerprint_readiness_status_rank(
            _summary_key(decomposition.get("status"))
        ),
    )
    return (
        readiness_rank,
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
    )


def _fingerprint_readiness_status_rank(status: str) -> int:
    ranks = {
        "unavailable": 0,
        "limited": 1,
        "skipped": 2,
        "valid": 3,
    }
    return ranks.get(status, 99)


def _series_fingerprint_regime_target_summaries(
    findings: Iterable[QualityFinding],
    *,
    count_limit: int,
) -> list[dict[str, JSONValue]]:
    target_summaries: list[dict[str, JSONValue]] = []
    for finding in findings:
        if finding.rule_id != SERIES_FINGERPRINT_RULE_ID:
            continue
        payload = finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        if not isinstance(payload, Mapping):
            continue
        target_summaries.append(
            _regime_target_summary(
                finding,
                cast(Mapping[str, JSONValue], payload),
                count_limit=count_limit,
            )
        )
    target_summaries.sort(key=_regime_target_sort_key)
    return target_summaries


def _regime_target_summary(
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
    *,
    count_limit: int,
) -> dict[str, JSONValue]:
    target_axis = _topology_target_axis(
        finding,
        _payload_mapping(payload.get("target_axis")),
    )
    source = _payload_mapping(payload.get("source"))
    return {
        "target_axis": target_axis,
        "source_kind": _summary_key(source.get("kind")),
        "calendar_regimes": _calendar_regime_summary(
            _payload_mapping(payload.get("calendar_regimes")),
            count_limit=count_limit,
        ),
        "conditional_distributions": _conditional_distribution_summary(
            _payload_mapping(payload.get("conditional_distributions")),
            timeframe=_summary_key(target_axis.get("timeframe")),
            count_limit=count_limit,
        ),
    }


def _calendar_regime_summary(
    calendar: Mapping[str, JSONValue],
    *,
    count_limit: int,
) -> dict[str, JSONValue]:
    if not calendar:
        return {
            "status": "missing",
            "raw_status": "missing",
            "computed_from": "unknown",
            "cache_source": None,
            "row_count": 0,
            "parsed_row_count": 0,
            "invalid_timestamp_count": 0,
            "calendar_profile": _calendar_profile_summary(calendar),
            "session_state_counts": {},
            "active_session_counts": {},
            "special_tag_counts": {},
            "holiday_tag_counts": {},
            "event_tag_counts": {},
            "hour_of_day_counts": {},
            "day_of_week_counts": {},
        }
    raw_status = _summary_key(calendar.get("status"))
    status = "unavailable" if raw_status == "unavailable" else "available"
    summary: dict[str, JSONValue] = {
        "status": status,
        "raw_status": raw_status,
        "computed_from": _summary_key(calendar.get("computed_from")),
        "cache_source": _optional_summary_key(calendar.get("cache_source")),
        "row_count": _int_payload(calendar.get("row_count")),
        "parsed_row_count": _int_payload(calendar.get("parsed_row_count")),
        "invalid_timestamp_count": _int_payload(
            calendar.get("invalid_timestamp_count")
        ),
        "calendar_profile": _calendar_profile_summary(calendar),
    }
    count_fields = (
        "session_state_counts",
        "active_session_counts",
        "special_tag_counts",
        "holiday_tag_counts",
        "event_tag_counts",
        "hour_of_day_counts",
        "day_of_week_counts",
    )
    for count_field in count_fields:
        summary[count_field] = _bounded_count_mapping(
            _counter_from_mapping(_payload_mapping(calendar.get(count_field))),
            limit=count_limit,
        )
    return summary


def _calendar_profile_summary(
    calendar: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    policy = _payload_mapping(calendar.get("calendar_policy"))
    profile = _payload_mapping(policy.get("calendar_profile"))
    return {
        "name": _summary_key(profile.get("name")),
        "source": _summary_key(
            profile.get("source") or policy.get("holiday_calendar_source")
        ),
        "version": _summary_key(profile.get("version")),
        "complete": (
            calendar.get("calendar_profile_complete") is True
            or policy.get("holiday_calendar_complete") is True
        ),
        "missing_optional_calendar_data": (
            calendar.get("missing_optional_calendar_data") is True
        ),
        "static_advisory": (
            profile.get("static_advisory") is True
            or policy.get("holiday_calendar_static_advisory") is True
        ),
    }


def _conditional_distribution_summary(
    conditional: Mapping[str, JSONValue],
    *,
    timeframe: str,
    count_limit: int,
) -> dict[str, JSONValue]:
    if timeframe != TICK:
        return {"status": "not_applicable", "metric": "tick_spread"}
    if not conditional:
        return {"status": "absent", "metric": "tick_spread"}
    return {
        "status": "available",
        "basis": _summary_key(conditional.get("basis")),
        "metric": _summary_key(conditional.get("metric")),
        "row_count": _int_payload(conditional.get("row_count")),
        "sampled_row_count": _int_payload(conditional.get("sampled_row_count")),
        "usable_row_count": _int_payload(conditional.get("usable_row_count")),
        "invalid_row_count": _int_payload(conditional.get("invalid_row_count")),
        "truncated": conditional.get("truncated") is True,
        "by_active_session": _conditioned_spread_rows(
            _payload_mapping(conditional.get("by_active_session")),
            limit=count_limit,
        ),
        "by_special_tag": _conditioned_spread_rows(
            _payload_mapping(conditional.get("by_special_tag")),
            limit=count_limit,
        ),
    }


def _conditioned_spread_rows(
    buckets: Mapping[str, JSONValue],
    *,
    limit: int,
) -> list[JSONValue]:
    limit_state = bounded_report_limit(
        limit,
        default_limit=limit,
        minimum_limit=1,
        allow_unbounded=False,
    )
    rows: list[dict[str, JSONValue]] = []
    for bucket, payload in buckets.items():
        spread_payload = dict(
            _payload_mapping(_payload_mapping(payload).get("spread"))
        )
        spread = _compact_numeric_summary(spread_payload)
        rows.append(
            {
                "bucket": _summary_key(bucket),
                "count": _int_payload(spread.get("count")),
                "spread": spread,
            }
        )
    rows.sort(
        key=lambda item: (
            -_int_payload(item.get("count")),
            _summary_key(item.get("bucket")),
        )
    )
    return cast(list[JSONValue], limit_state.slice(rows))  # type: ignore[redundant-cast]


def _bounded_count_mapping(
    counter: Counter[str],
    *,
    limit: int,
) -> dict[str, JSONValue]:
    if not counter:
        return {}
    limit_state = bounded_report_limit(
        limit,
        default_limit=limit,
        minimum_limit=1,
        allow_unbounded=False,
    )
    included = limit_state.slice(
        sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    )
    return {key: count for key, count in included}


def _bounded_count_rows(
    counter: Counter[str],
    *,
    limit: int,
) -> list[JSONValue]:
    limit_state = bounded_report_limit(
        limit,
        default_limit=limit,
        minimum_limit=1,
        allow_unbounded=False,
    )
    return [
        {"value": key, "count": count}
        for key, count in limit_state.slice(
            sorted(counter.items(), key=lambda item: (-item[1], item[0]))
        )
    ]


def _regime_target_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[object, ...]:
    axis = _payload_mapping(target.get("target_axis"))
    calendar = _payload_mapping(target.get("calendar_regimes"))
    conditional = _payload_mapping(target.get("conditional_distributions"))
    return (
        _regime_status_rank(_summary_key(calendar.get("status"))),
        _conditional_status_rank(_summary_key(conditional.get("status"))),
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
    )


def _regime_status_rank(status: str) -> int:
    ranks = {"unavailable": 0, "missing": 1, "available": 2}
    return ranks.get(status, 99)


def _conditional_status_rank(status: str) -> int:
    ranks = {"available": 0, "absent": 1, "not_applicable": 2}
    return ranks.get(status, 99)


def _series_fingerprint_distribution_target_summaries(
    findings: Iterable[QualityFinding],
) -> list[dict[str, JSONValue]]:
    target_summaries: list[dict[str, JSONValue]] = []
    for finding in findings:
        if finding.rule_id != SERIES_FINGERPRINT_RULE_ID:
            continue
        payload = finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        if not isinstance(payload, Mapping):
            continue
        target_summaries.append(
            _distribution_target_summary(
                finding,
                cast(Mapping[str, JSONValue], payload),
            )
        )
    target_summaries.sort(key=_distribution_target_sort_key)
    return target_summaries


def _distribution_target_summary(
    finding: QualityFinding,
    payload: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    target_axis = _payload_mapping(payload.get("target_axis"))
    source = _payload_mapping(payload.get("source"))
    tick_distribution = _payload_mapping(payload.get("tick_distribution"))
    distribution_kind, distribution = _distribution_kind_and_payload(
        target_axis,
        source,
        tick_distribution=tick_distribution,
    )
    precision = _payload_mapping(distribution.get("precision"))
    precision_counts = _payload_mapping(precision.get("decimal_place_counts"))
    source_kind = _summary_key(source.get("kind"))
    distribution_source = _distribution_source(source_kind, distribution_kind)
    zero_spread_count = _int_payload(distribution.get("zero_spread_count"))
    negative_spread_count = _int_payload(
        distribution.get("negative_spread_count")
    )
    return {
        "target_axis": _topology_target_axis(finding, target_axis),
        "distribution_kind": distribution_kind,
        "status": _distribution_status(
            target_axis,
            source,
            distribution_kind=distribution_kind,
        ),
        "row_count": _int_payload(
            distribution.get("row_count")
            if distribution
            else _payload_mapping(payload.get("coverage")).get("row_count")
        ),
        "sampled_row_count": _int_payload(
            distribution.get("sampled_row_count")
        ),
        "usable_row_count": _int_payload(distribution.get("usable_row_count")),
        "invalid_row_count": _int_payload(
            distribution.get("invalid_row_count")
        ),
        "partial_row_count": _int_payload(
            distribution.get("partial_row_count")
        ),
        "invalid_row_rate": _distribution_rate(
            _int_payload(distribution.get("invalid_row_count")),
            _int_payload(distribution.get("row_count")),
        ),
        "truncated": distribution.get("truncated") is True,
        "source_kind": source_kind,
        "distribution_source": distribution_source,
        "cache_source": _optional_summary_key(source.get("cache_source")),
        "precision_source": _distribution_precision_source(
            distribution_kind,
            precision,
        ),
        "precision_decimal_place_count": len(precision_counts),
        "zero_spread_count": zero_spread_count,
        "negative_spread_count": negative_spread_count,
        "zero_spread_rate": _optional_float_payload(
            distribution.get("zero_spread_rate")
        ),
        "negative_spread_rate": _optional_float_payload(
            distribution.get("negative_spread_rate")
        ),
    }


def _distribution_kind_and_payload(
    target_axis: Mapping[str, JSONValue],
    source: Mapping[str, JSONValue],
    *,
    tick_distribution: Mapping[str, JSONValue],
) -> tuple[str, Mapping[str, JSONValue]]:
    timeframe = _summary_key(target_axis.get("timeframe"))
    if timeframe == TICK and tick_distribution:
        return "tick", tick_distribution
    if _supported_readable_distribution_axis(target_axis, source):
        return "missing", {}
    return "missing", {}


def _supported_readable_distribution_axis(
    target_axis: Mapping[str, JSONValue],
    source: Mapping[str, JSONValue],
) -> bool:
    timeframe = _summary_key(target_axis.get("timeframe"))
    source_kind = _summary_key(source.get("kind"))
    return timeframe in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES and (
        source_kind not in {"unavailable", "unknown"}
    )


def _distribution_status(
    target_axis: Mapping[str, JSONValue],
    source: Mapping[str, JSONValue],
    *,
    distribution_kind: str,
) -> str:
    if distribution_kind != "missing":
        return "available"
    if _supported_readable_distribution_axis(target_axis, source):
        return "missing"
    return "unavailable"


def _distribution_source(source_kind: str, distribution_kind: str) -> str:
    if distribution_kind == "missing":
        return "unavailable"
    if source_kind == "cache":
        return "cache"
    if source_kind in {"csv_text", "zip_member"}:
        return "text"
    return "unavailable"


def _distribution_precision_source(
    distribution_kind: str,
    precision: Mapping[str, JSONValue],
) -> str:
    if distribution_kind != "tick" or not precision:
        return "unavailable"
    return _summary_key(precision.get("precision_source"))


def _distribution_rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, DEFAULT_FINGERPRINT_ROUNDING_DIGITS)


def _optional_float_payload(value: object) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except ValueError:
        return None


def _distribution_attention_target_summary(
    target: Mapping[str, JSONValue],
    profile: HistDataFingerprintDistributionAttentionProfile,
) -> dict[str, JSONValue] | None:
    flags = _distribution_attention_flags(target, profile)
    if not flags:
        return None
    return {
        "target_axis": dict(_payload_mapping(target.get("target_axis"))),
        "attention_level": _distribution_attention_level(flags),
        "attention_flags": list(flags),
        "distribution_kind": _summary_key(target.get("distribution_kind")),
        "status": _summary_key(target.get("status")),
        "row_count": _int_payload(target.get("row_count")),
        "sampled_row_count": _int_payload(target.get("sampled_row_count")),
        "usable_row_count": _int_payload(target.get("usable_row_count")),
        "invalid_row_count": _int_payload(target.get("invalid_row_count")),
        "partial_row_count": _int_payload(target.get("partial_row_count")),
        "invalid_row_rate": _optional_float_payload(
            target.get("invalid_row_rate")
        ),
        "truncated": target.get("truncated") is True,
        "source_kind": _summary_key(target.get("source_kind")),
        "distribution_source": _summary_key(target.get("distribution_source")),
        "cache_source": _optional_summary_key(target.get("cache_source")),
        "precision_source": _summary_key(target.get("precision_source")),
        "precision_decimal_place_count": _int_payload(
            target.get("precision_decimal_place_count")
        ),
        "zero_spread_count": _int_payload(target.get("zero_spread_count")),
        "negative_spread_count": _int_payload(
            target.get("negative_spread_count")
        ),
        "zero_spread_rate": _optional_float_payload(
            target.get("zero_spread_rate")
        ),
        "negative_spread_rate": _optional_float_payload(
            target.get("negative_spread_rate")
        ),
    }


def _distribution_attention_flags(
    target: Mapping[str, JSONValue],
    profile: HistDataFingerprintDistributionAttentionProfile,
) -> tuple[str, ...]:
    flags: list[str] = []
    status = _summary_key(target.get("status"))
    distribution_kind = _summary_key(target.get("distribution_kind"))
    if status == "missing":
        flags.append("missing_distribution")
    if (
        distribution_kind != "missing"
        and _int_payload(target.get("row_count")) == 0
    ):
        flags.append("empty_distribution")
    if _distribution_threshold_met(
        _int_payload(target.get("invalid_row_count")),
        _optional_float_payload(target.get("invalid_row_rate")),
        min_count=profile.invalid_row_min_count,
        min_rate=profile.invalid_row_min_rate,
    ):
        flags.append("high_invalid_row_rate")
    if _int_payload(target.get("partial_row_count")) > 0:
        flags.append("partial_rows_present")
    if distribution_kind == "tick":
        if _distribution_threshold_met(
            _int_payload(target.get("negative_spread_count")),
            _optional_float_payload(target.get("negative_spread_rate")),
            min_count=profile.negative_spread_min_count,
            min_rate=profile.negative_spread_min_rate,
        ):
            flags.append("negative_tick_spreads_present")
        if _distribution_threshold_met(
            _int_payload(target.get("zero_spread_count")),
            _optional_float_payload(target.get("zero_spread_rate")),
            min_count=profile.zero_spread_min_count,
            min_rate=profile.zero_spread_min_rate,
        ):
            flags.append("zero_tick_spread_rate_present")
    if profile.flag_truncated_distribution and target.get("truncated") is True:
        flags.append("truncated_distribution")
    if (
        profile.flag_cache_float_precision
        and _summary_key(target.get("precision_source")) == "cache_float"
    ):
        flags.append("cache_float_precision_basis")
    ordered = [
        flag for flag in DISTRIBUTION_ATTENTION_FLAGS if flag in set(flags)
    ]
    return tuple(ordered)


def _distribution_threshold_met(
    count: int,
    rate: float | None,
    *,
    min_count: int,
    min_rate: float,
) -> bool:
    if count < min_count:
        return False
    if rate is None:
        return True
    return rate >= min_rate


def _distribution_attention_level(flags: tuple[str, ...]) -> str:
    flag_set = set(flags)
    if "missing_distribution" in flag_set:
        return "missing"
    if flag_set & {
        "empty_distribution",
        "high_invalid_row_rate",
        "partial_rows_present",
        "negative_tick_spreads_present",
    }:
        return "defect"
    if "zero_tick_spread_rate_present" in flag_set:
        return "microstructure"
    if "truncated_distribution" in flag_set:
        return "sample"
    return "precision"


def _distribution_attention_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[object, ...]:
    axis = _payload_mapping(target.get("target_axis"))
    return (
        _distribution_attention_level_rank(
            _summary_key(target.get("attention_level"))
        ),
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
        _summary_key(target.get("distribution_kind")),
        -_int_payload(target.get("invalid_row_count")),
        -_int_payload(target.get("negative_spread_count")),
        -_int_payload(target.get("zero_spread_count")),
    )


def _distribution_attention_level_rank(level: str) -> int:
    ranks = {
        "missing": 0,
        "defect": 1,
        "microstructure": 2,
        "sample": 3,
        "precision": 4,
    }
    return ranks.get(level, 99)


def _distribution_target_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[str, str, str, str, str, str]:
    axis = _payload_mapping(target.get("target_axis"))
    return (
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
        _summary_key(target.get("distribution_kind")),
    )


def _series_fingerprint_payload(
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "schema_version": TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
        "target_axis": _target_axis(target),
        "coverage": _empty_coverage(parsed_row_count=None),
        "temporal_topology": timestamp_topology_payload_for_target(
            target,
            inspection_sample_limit=profile.topology_inspection_sample_limit,
        ),
        "source": _unavailable_source(
            target,
            reason=_unsupported_reason(target),
        ),
    }
    if _unsupported_reason(target):
        return _finalize_fingerprint_payload(
            payload,
            target=target,
            profile=profile,
        )

    columns = columns_for_timeframe(target.timeframe)
    cache = read_quality_polars_cache(target, required_columns=columns)
    if cache is not None:
        payload["coverage"] = _coverage_from_frame(cache.frame)
        payload["calendar_regimes"] = calendar_regime_payload_for_target(
            target,
            calendar_profile=profile.calendar_profile,
        )
        _add_distribution_payload(
            payload,
            timeframe=target.timeframe,
            distribution=_distribution_from_frame(
                cache.frame,
                timeframe=target.timeframe,
                profile=profile,
            ),
        )
        _add_dynamics_payload(
            payload,
            target=target,
            frame=cache.frame,
            text=None,
            profile=profile,
        )
        _add_conditional_distribution_payload(
            payload,
            target=target,
            frame=cache.frame,
            text=None,
            profile=profile,
        )
        payload["source"] = {
            "kind": "cache",
            "cache_source": cache.source,
            "path": publish_safe_path(str(cache.path)),
        }
        return _finalize_fingerprint_payload(
            payload,
            target=target,
            profile=profile,
            training_frame=cache.frame,
        )

    if target.kind is QualityTargetKind.CACHE:
        payload["coverage"] = _empty_coverage(parsed_row_count=None)
        payload["source"] = _unavailable_source(
            target,
            reason="cache_unavailable",
        )
        return _finalize_fingerprint_payload(
            payload,
            target=target,
            profile=profile,
        )

    try:
        text_payload = _read_text_payload(target)
    except (OSError, UnicodeDecodeError, zipfile.BadZipFile) as exc:
        payload["source"] = _unavailable_source(
            target,
            reason="source_unreadable",
            error=exc,
        )
        return _finalize_fingerprint_payload(
            payload,
            target=target,
            profile=profile,
        )
    except ValueError as exc:
        payload["source"] = _unavailable_source(
            target,
            reason=str(exc),
        )
        return _finalize_fingerprint_payload(
            payload,
            target=target,
            profile=profile,
        )

    payload["coverage"] = _coverage_from_text(
        text_payload.text,
        timeframe=target.timeframe,
    )
    payload["calendar_regimes"] = calendar_regime_payload_for_target(
        target,
        calendar_profile=profile.calendar_profile,
    )
    _add_distribution_payload(
        payload,
        timeframe=target.timeframe,
        distribution=_distribution_from_text(
            text_payload.text,
            timeframe=target.timeframe,
            profile=profile,
        ),
    )
    _add_dynamics_payload(
        payload,
        target=target,
        frame=None,
        text=text_payload.text,
        profile=profile,
    )
    _add_conditional_distribution_payload(
        payload,
        target=target,
        frame=None,
        text=text_payload.text,
        profile=profile,
    )
    if target.kind is QualityTargetKind.ZIP:
        payload["source"] = {
            "kind": "zip_member",
            "path": publish_safe_path(target.path),
            "member": text_payload.source_member,
        }
    else:
        payload["source"] = {
            "kind": "csv_text",
            "path": publish_safe_path(target.path),
        }
    training_frame = _training_frame_from_text(
        text_payload.text,
        target=target,
        profile=profile,
    )
    return _finalize_fingerprint_payload(
        payload,
        target=target,
        profile=profile,
        training_frame=training_frame,
    )


def _finalize_fingerprint_payload(
    payload: dict[str, JSONValue],
    *,
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
    training_frame: Any | None = None,
) -> dict[str, JSONValue]:
    if profile.cache_source_parity.enabled:
        payload["cache_source_parity"] = _cache_source_parity_payload(
            target,
            profile=profile,
        )
    payload["fingerprint_audit"] = _fingerprint_audit_payload(
        payload,
        target=target,
        profile=profile,
    )
    if not _unsupported_reason(target):
        payload["synthetic_constraints"] = (
            synthetic_constraints_from_fingerprint(
                payload,
                training_frame=training_frame,
                target=target,
            )
        )
        if profile.classical_baselines.enabled:
            payload["classical_baselines"] = (
                classical_baseline_diagnostics_from_training_frame(
                    training_frame,
                    payload,
                    profile=profile.classical_baselines,
                    target=target,
                )
            )
        model_input_result = None
        if (
            profile.classical_model_input.enabled
            or profile.exponential_smoothing.enabled
            or profile.autoregressive.enabled
        ):
            model_input_result = build_classical_model_input(
                training_frame,
                payload,
                profile=profile.classical_model_input,
                target=target,
            )
        if (
            profile.classical_model_input.enabled
            and model_input_result is not None
        ):
            payload["classical_model_input"] = dict(model_input_result.contract)
        if (
            profile.exponential_smoothing.enabled
            and model_input_result is not None
        ):
            payload["exponential_smoothing"] = dict(
                exponential_smoothing_from_model_input(
                    training_frame,
                    model_input_result,
                    payload,
                    input_profile=profile.classical_model_input,
                    profile=profile.exponential_smoothing,
                    target=target,
                ).diagnostics
            )
        if profile.autoregressive.enabled and model_input_result is not None:
            payload["autoregressive"] = dict(
                autoregressive_from_model_input(
                    training_frame,
                    model_input_result,
                    payload,
                    input_profile=profile.classical_model_input,
                    profile=profile.autoregressive,
                    exponential_smoothing=_payload_mapping(
                        payload.get("exponential_smoothing")
                    ),
                    target=target,
                ).diagnostics
            )
        payload["fingerprint_audit"] = _fingerprint_audit_payload(
            payload,
            target=target,
            profile=profile,
        )
    payload["fingerprint_id"] = _fingerprint_id(payload)
    return payload


def _training_frame_from_text(
    text: str,
    *,
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> Any | None:
    try:
        batch = parse_ascii_lines(
            target.timeframe,
            islice(StringIO(text), max(1, _profile_max_rows(profile))),
        )
        return to_polars_frame(batch)
    except (OSError, TypeError, ValueError):
        return None


def _cache_source_parity_payload(
    target: QualityTarget,
    *,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    mismatch_limit = bounded_report_limit(
        profile.cache_source_parity.mismatch_limit,
        default_limit=DEFAULT_FINGERPRINT_PARITY_MISMATCH_LIMIT,
    )
    bases: dict[str, JSONValue] = {
        "raw_source": _parity_unavailable_basis("source_unavailable"),
        "raw_cache": _parity_unavailable_basis("cache_unavailable"),
        "enriched_cache": _parity_unavailable_basis(
            "cache_enrichment_unavailable"
        ),
        "quality_report": _parity_unavailable_basis(
            "quality_report_projection_unavailable"
        ),
        "influx_projection": _parity_unavailable_basis(
            "influx_projection_unavailable"
        ),
    }
    unsupported_reason = _unsupported_reason(target)
    if unsupported_reason:
        return _finalize_parity_payload(
            target,
            bases=bases,
            comparisons=[],
            skipped_reasons=[unsupported_reason],
            mismatch_limit=mismatch_limit,
        )
    required_columns = columns_for_timeframe(target.timeframe)
    cache = read_fingerprint_parity_polars_cache(
        target,
        required_columns=required_columns,
    )
    if cache is not None:
        raw_cache_columns = set(getattr(cache.frame, "columns", ()))
        bases["raw_cache"] = {
            "status": "available",
            "path": publish_safe_path(str(cache.path)),
            "cache_source": cache.source,
            "fresh": cache.fresh,
            "freshness": _parity_freshness_status(cache.fresh),
            "row_count": int(getattr(cache.frame, "height", 0) or 0),
            "column_count": len(raw_cache_columns),
            "training_schema_present": (
                "training_schema_version" in raw_cache_columns
            ),
            "training_required_columns_present": set(
                TRAINING_REQUIRED_COLUMNS
            ).issubset(raw_cache_columns),
        }
    if target.kind not in {QualityTargetKind.CSV, QualityTargetKind.ZIP}:
        return _finalize_parity_payload(
            target,
            bases=bases,
            comparisons=[],
            skipped_reasons=["source_target_unavailable"],
            mismatch_limit=mismatch_limit,
        )
    try:
        text_payload = _read_text_payload(target)
    except (OSError, UnicodeDecodeError, ValueError, zipfile.BadZipFile) as exc:
        bases["raw_source"] = _parity_unavailable_basis(
            "source_unreadable",
            detail=type(exc).__name__,
        )
        return _finalize_parity_payload(
            target,
            bases=bases,
            comparisons=[],
            skipped_reasons=["source_unreadable"],
            mismatch_limit=mismatch_limit,
        )

    source_coverage = _coverage_from_text(
        text_payload.text,
        timeframe=target.timeframe,
    )
    bases["raw_source"] = {
        "status": "available",
        "kind": (
            "zip_member" if target.kind is QualityTargetKind.ZIP else "csv_text"
        ),
        "path": publish_safe_path(target.path),
        "member": text_payload.source_member or None,
        "row_count": _int_payload(source_coverage.get("row_count")),
    }
    if cache is None:
        return _finalize_parity_payload(
            target,
            bases=bases,
            comparisons=[],
            skipped_reasons=["cache_unavailable"],
            mismatch_limit=mismatch_limit,
        )

    cache_target = QualityTarget(
        path=str(cache.path),
        kind=QualityTargetKind.CACHE,
        data_format=target.data_format,
        timeframe=target.timeframe,
        symbol=target.symbol,
        period=target.period,
        metadata=dict(target.metadata),
    )
    source_sections: dict[str, Mapping[str, JSONValue]] = {
        "coverage": source_coverage,
        "temporal_topology": timestamp_topology_payload_for_target(
            target,
            inspection_sample_limit=0,
            prefer_cache=False,
        ),
        "calendar_regimes": calendar_regime_payload_for_target(
            target,
            calendar_profile=profile.calendar_profile,
            prefer_cache=False,
        ),
        "conditional_distributions": _conditional_distributions(
            target,
            frame=None,
            text=text_payload.text,
            profile=profile,
        ),
    }
    cache_sections: dict[str, Mapping[str, JSONValue]] = {
        "coverage": _coverage_from_frame(cache.frame),
        "temporal_topology": timestamp_topology_payload_for_target(
            cache_target,
            inspection_sample_limit=0,
        ),
        "calendar_regimes": calendar_regime_payload_for_target(
            cache_target,
            calendar_profile=profile.calendar_profile,
        ),
        "conditional_distributions": _conditional_distributions(
            cache_target,
            frame=cache.frame,
            text=None,
            profile=profile,
        ),
    }
    comparisons = _fingerprint_section_parity_comparisons(
        source_sections,
        cache_sections,
        mismatch_limit=mismatch_limit,
    )
    training_comparisons, training_bases = _training_projection_parity(
        target,
        cache_target=cache_target,
        source_text=text_payload.text,
        cache_frame=cache.frame,
        profile=profile,
        mismatch_limit=mismatch_limit,
    )
    comparisons.extend(training_comparisons)
    bases.update(training_bases)
    if cache.fresh is False:
        comparisons.insert(
            0,
            {
                "section": "cache_freshness",
                "status": "mismatch",
                "compared_field_count": 1,
                "mismatch_field_count": 1,
                "mismatch_fields": ["mtime_ns"],
                "mismatch_codes": ["fingerprint_cache_source_stale_cache"],
            },
        )
    return _finalize_parity_payload(
        target,
        bases=bases,
        comparisons=comparisons,
        skipped_reasons=[],
        mismatch_limit=mismatch_limit,
    )


def _fingerprint_section_parity_comparisons(
    source: Mapping[str, Mapping[str, JSONValue]],
    cache: Mapping[str, Mapping[str, JSONValue]],
    *,
    mismatch_limit: BoundedReportLimit,
) -> list[dict[str, JSONValue]]:
    specifications = (
        (
            "coverage",
            (
                "row_count",
                "parsed_row_count",
                "start_timestamp_utc_ms",
                "end_timestamp_utc_ms",
            ),
        ),
        (
            "temporal_topology",
            (
                "row_count",
                "parsed_row_count",
                "invalid_timestamp_count",
                "non_monotonic_count",
                "duplicate_timestamp_count",
                "suspicious_gap_count",
                "expected_session_closure_count",
                "weekend_activity_count",
                "gap_bucket_counts",
            ),
        ),
        (
            "calendar_regimes",
            (
                "row_count",
                "parsed_row_count",
                "invalid_timestamp_count",
                "session_state_counts",
                "active_session_counts",
                "clock_session_counts",
                "overlap_counts",
                "special_tag_counts",
                "holiday_tag_counts",
                "event_tag_counts",
                "hour_of_day_counts",
                "day_of_week_counts",
            ),
        ),
        (
            "conditional_distributions",
            (
                "row_count",
                "sampled_row_count",
                "usable_row_count",
                "invalid_row_count",
                "by_active_session",
                "by_special_tag",
            ),
        ),
    )
    return [
        _parity_section_comparison(
            section,
            source.get(section, {}),
            cache.get(section, {}),
            fields=fields,
            mismatch_limit=mismatch_limit,
        )
        for section, fields in specifications
    ]


def _parity_section_comparison(
    section: str,
    source: Mapping[str, JSONValue],
    cache: Mapping[str, JSONValue],
    *,
    fields: tuple[str, ...],
    mismatch_limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    if not source or not cache:
        return {
            "section": section,
            "status": "skipped",
            "reason": "section_unavailable",
            "compared_field_count": 0,
            "mismatch_field_count": 0,
            "mismatch_fields": [],
            "mismatch_codes": [],
        }
    mismatches = [
        field for field in fields if source.get(field) != cache.get(field)
    ]
    codes = _parity_section_mismatch_codes(section, mismatches)
    included = mismatch_limit.slice(mismatches)
    return {
        "section": section,
        "status": "mismatch" if mismatches else "match",
        "compared_field_count": len(fields),
        "mismatch_field_count": len(mismatches),
        "included_mismatch_field_count": len(included),
        "omitted_mismatch_field_count": max(0, len(mismatches) - len(included)),
        "truncated": len(included) < len(mismatches),
        "mismatch_fields": cast(JSONValue, included),
        "mismatch_codes": cast(JSONValue, codes),
    }


def _parity_section_mismatch_codes(
    section: str,
    fields: list[str],
) -> list[str]:
    if not fields:
        return []
    if section == "coverage":
        codes = []
        if set(fields) & {"row_count", "parsed_row_count"}:
            codes.append("fingerprint_cache_source_row_count_mismatch")
        if set(fields) & {"start_timestamp_utc_ms", "end_timestamp_utc_ms"}:
            codes.append("fingerprint_cache_source_coverage_bounds_mismatch")
        return codes
    return [
        {
            "temporal_topology": "fingerprint_cache_source_topology_mismatch",
            "calendar_regimes": "fingerprint_cache_source_calendar_mismatch",
            "conditional_distributions": (
                "fingerprint_cache_source_conditioned_spread_mismatch"
            ),
        }[section]
    ]


def _training_projection_parity(
    target: QualityTarget,
    *,
    cache_target: QualityTarget,
    source_text: str,
    cache_frame: Any,
    profile: HistDataFingerprintProfile,
    mismatch_limit: BoundedReportLimit,
) -> tuple[list[dict[str, JSONValue]], dict[str, JSONValue]]:
    sample_limit = max(1, _profile_max_rows(profile))
    try:
        source_batch = parse_ascii_lines(
            target.timeframe,
            islice(StringIO(source_text), sample_limit),
        )
        source_frame = to_polars_frame(source_batch)
        cache_sample = cache_frame.head(sample_limit)
        enriched_source = ensure_tick_training_features(
            source_frame,
            target=target,
        )
        enriched_cache = ensure_tick_training_features(
            cache_sample,
            target=target,
        )
    except (OSError, TypeError, ValueError) as exc:
        comparison: dict[str, JSONValue] = {
            "section": "training_substrate",
            "status": "skipped",
            "reason": "training_enrichment_unavailable",
            "error_type": type(exc).__name__,
            "compared_field_count": 0,
            "mismatch_field_count": 0,
            "mismatch_fields": [],
            "mismatch_codes": [],
        }
        return [comparison], {}

    required = set(TRAINING_REQUIRED_COLUMNS)
    source_columns = set(enriched_source.columns)
    cache_columns = set(enriched_cache.columns)
    missing_columns = sorted(
        (required - source_columns) | (required - cache_columns)
    )
    column_comparison = _bounded_training_comparison(
        "training_columns",
        missing_columns,
        code="fingerprint_cache_source_training_columns_mismatch",
        compared_count=len(required),
        mismatch_limit=mismatch_limit,
    )
    identity_mismatches = _identity_mismatch_columns(
        enriched_source,
        enriched_cache,
    )
    identity_comparison = _bounded_training_comparison(
        "row_identity",
        identity_mismatches,
        code="fingerprint_cache_source_row_identity_mismatch",
        compared_count=5,
        mismatch_limit=mismatch_limit,
    )
    source_duplicates = _duplicate_timestamp_row_count(enriched_source)
    cache_duplicates = _duplicate_timestamp_row_count(enriched_cache)
    duplicate_fields = (
        []
        if source_duplicates == cache_duplicates
        else ["duplicate_timestamp_row_count"]
    )
    duplicate_comparison = _bounded_training_comparison(
        "duplicate_timestamps",
        duplicate_fields,
        code="fingerprint_cache_source_duplicate_timestamp_mismatch",
        compared_count=1,
        mismatch_limit=mismatch_limit,
    )

    report = quality_report_from_training_features(
        enriched_cache,
        target=cache_target,
    )
    report_fields = []
    if (
        report.metadata.get("training_schema_version")
        != TRAINING_SCHEMA_VERSION
    ):
        report_fields.append("training_schema_version")
    if _int_payload(report.metadata.get("row_count")) != enriched_cache.height:
        report_fields.append("row_count")
    report_comparison = _bounded_training_comparison(
        "quality_report_projection",
        report_fields,
        code="fingerprint_cache_source_quality_report_projection_mismatch",
        compared_count=2,
        mismatch_limit=mismatch_limit,
    )
    influx_fields = _influx_projection_missing_fields(
        enriched_cache,
        target=target,
    )
    influx_comparison = _bounded_training_comparison(
        "influx_projection",
        influx_fields,
        code="fingerprint_cache_source_influx_projection_mismatch",
        compared_count=6,
        mismatch_limit=mismatch_limit,
    )
    raw_cache_columns = set(getattr(cache_frame, "columns", ()))
    cache_was_enriched = required.issubset(raw_cache_columns)
    sampled_count = min(int(cache_frame.height), sample_limit)
    bases: dict[str, JSONValue] = {
        "enriched_cache": {
            "status": "available",
            "training_schema_version": TRAINING_SCHEMA_VERSION,
            "cache_was_enriched": cache_was_enriched,
            "legacy_cache_enriched_on_read": not cache_was_enriched,
            "required_column_count": len(required),
            "column_count": len(cache_columns),
            "sampled_row_count": sampled_count,
            "source_row_count": int(cache_frame.height),
            "truncated": sampled_count < int(cache_frame.height),
        },
        "quality_report": {
            "status": "available",
            "projection_kind": "audit_from_enriched_rows",
            "training_schema_version": report.metadata.get(
                "training_schema_version"
            ),
            "row_count": report.metadata.get("row_count"),
        },
        "influx_projection": {
            "status": "available" if not influx_fields else "limited",
            "projection_kind": "same_point_enriched_fields",
            "missing_required_field_count": len(influx_fields),
        },
    }
    return (
        [
            column_comparison,
            identity_comparison,
            duplicate_comparison,
            report_comparison,
            influx_comparison,
        ],
        bases,
    )


def _bounded_training_comparison(
    section: str,
    mismatches: list[str],
    *,
    code: str,
    compared_count: int,
    mismatch_limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    included = mismatch_limit.slice(mismatches)
    return {
        "section": section,
        "status": "mismatch" if mismatches else "match",
        "compared_field_count": compared_count,
        "mismatch_field_count": len(mismatches),
        "included_mismatch_field_count": len(included),
        "omitted_mismatch_field_count": max(0, len(mismatches) - len(included)),
        "truncated": len(included) < len(mismatches),
        "mismatch_fields": cast(JSONValue, included),
        "mismatch_codes": cast(JSONValue, [code] if mismatches else []),
    }


def _identity_mismatch_columns(source: Any, cache: Any) -> list[str]:
    identity_columns = (
        "series_id",
        "period",
        "row_id",
        "source_row_number",
        "event_seq",
    )
    mismatches = []
    for column in identity_columns:
        if column not in source.columns or column not in cache.columns:
            mismatches.append(column)
            continue
        source_values = source.get_column(column).to_list()
        cache_values = cache.get_column(column).to_list()
        if source_values != cache_values:
            mismatches.append(column)
    return mismatches


def _duplicate_timestamp_row_count(frame: Any) -> int:
    if "datetime" not in frame.columns or frame.is_empty():
        return 0
    return max(
        0,
        int(frame.height) - int(frame.get_column("datetime").n_unique()),
    )


def _influx_projection_missing_fields(
    frame: Any,
    *,
    target: QualityTarget,
) -> list[str]:
    if frame.is_empty():
        return ["sample_row"]
    columns = tuple(frame.columns)
    row = frame.row(0)
    line = format_influx_line(
        target.symbol,
        target.data_format,
        target.timeframe,
        row,
        columns=columns,
    )
    try:
        field_text = line.split(" ", 2)[1]
    except IndexError:
        return ["line_protocol_fields"]
    emitted = {
        item.split("=", 1)[0] for item in field_text.split(",") if "=" in item
    }
    required = (
        "source_row_number",
        "event_seq",
        "quality_status_code",
        "quality_finding_count",
        "training_usable",
        "training_weight",
    )
    return [field for field in required if field not in emitted]


def _finalize_parity_payload(
    target: QualityTarget,
    *,
    bases: Mapping[str, JSONValue],
    comparisons: list[dict[str, JSONValue]],
    skipped_reasons: list[str],
    mismatch_limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    mismatch_codes = sorted(
        {
            code
            for comparison in comparisons
            for code in (
                str(item)
                for item in _string_list(comparison.get("mismatch_codes"))
            )
        }
    )
    included_codes = mismatch_limit.slice(mismatch_codes)
    mismatch_count = sum(
        1 for item in comparisons if item.get("status") == "mismatch"
    )
    compared_count = sum(
        1 for item in comparisons if item.get("status") in {"match", "mismatch"}
    )
    status = "not_compared"
    if compared_count:
        status = "mismatch" if mismatch_count else "match"
    return {
        "schema_version": TIME_SERIES_FINGERPRINT_PARITY_SCHEMA_VERSION,
        "status": status,
        "advisory": True,
        "target_axis": _target_axis(target),
        "base_grain": {
            "data_format": target.data_format,
            "timeframe": target.timeframe,
        },
        "compared_section_count": compared_count,
        "matching_section_count": sum(
            1 for item in comparisons if item.get("status") == "match"
        ),
        "mismatched_section_count": mismatch_count,
        "skipped_section_count": sum(
            1 for item in comparisons if item.get("status") == "skipped"
        ),
        "mismatch_code_count": len(mismatch_codes),
        "included_mismatch_code_count": len(included_codes),
        "omitted_mismatch_code_count": max(
            0, len(mismatch_codes) - len(included_codes)
        ),
        "truncated": len(included_codes) < len(mismatch_codes),
        "limit_metadata": {"mismatches": mismatch_limit.limit_payload()},
        "mismatch_codes": cast(JSONValue, included_codes),
        "skipped_reasons": cast(JSONValue, sorted(set(skipped_reasons))),
        "bases": dict(bases),
        "comparisons": cast(JSONValue, comparisons),
    }


def _parity_unavailable_basis(
    reason: str,
    *,
    detail: str = "",
) -> dict[str, JSONValue]:
    payload: dict[str, JSONValue] = {
        "status": "unavailable",
        "reason": reason,
    }
    if detail:
        payload["detail"] = detail
    return payload


def _parity_freshness_status(fresh: bool | None) -> str:
    if fresh is True:
        return "fresh"
    if fresh is False:
        return "stale"
    return "not_applicable"


def _fingerprint_audit_payload(
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    expected = _fingerprint_expected_sections(target)
    if profile.cache_source_parity.enabled:
        expected = (*expected, "cache_source_parity")
    if profile.classical_baselines.enabled:
        expected = (*expected, "classical_baselines")
    if profile.classical_model_input.enabled:
        expected = (*expected, "classical_model_input")
    if profile.exponential_smoothing.enabled:
        expected = (*expected, "exponential_smoothing")
    if profile.autoregressive.enabled:
        expected = (*expected, "autoregressive")
    emitted = [
        section for section in FINGERPRINT_AUDIT_SECTIONS if section in payload
    ]
    status_sections = _ordered_unique((*expected, *emitted))
    skipped: dict[str, JSONValue] = {}
    for section in expected:
        if section not in payload:
            skipped[section] = _fingerprint_section_skip_payload(
                section,
                payload,
                target=target,
            )
    section_statuses: dict[str, JSONValue] = {}
    for section in status_sections:
        section_statuses[section] = _fingerprint_section_status(
            section,
            payload,
        )
    unsupported_reason = _unsupported_reason(target)
    source = _payload_mapping(payload.get("source"))
    source_reason = _optional_summary_key(source.get("reason"))
    target_capability: dict[str, JSONValue] = {
        "supported": unsupported_reason == "",
        "unsupported_reason": unsupported_reason or None,
    }
    source_status: dict[str, JSONValue] = {
        "kind": _summary_key(source.get("kind")),
        "readable": source.get("kind") != "unavailable",
        "reason": source_reason,
    }
    conditional_distribution_eligibility: dict[str, JSONValue] = {
        "tick_spread": _conditional_tick_spread_eligibility(
            payload,
            target=target,
        )
    }
    dynamics_readiness: dict[str, JSONValue] = {
        "microstructure_dynamics": _fingerprint_dynamics_readiness(
            "microstructure_dynamics",
            payload,
            target=target,
        ),
    }
    stationarity_readiness = _fingerprint_stationarity_readiness(
        payload,
        target=target,
    )
    decomposition_readiness = _fingerprint_decomposition_readiness(
        payload,
        target=target,
    )
    audit_payload: dict[str, JSONValue] = {
        "schema_version": TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION,
        "sections_expected": [section for section in expected],
        "sections_emitted": [section for section in emitted],
        "sections_skipped": skipped,
        "section_statuses": section_statuses,
        "target_capability": target_capability,
        "source_status": source_status,
        "conditional_distribution_eligibility": (
            conditional_distribution_eligibility
        ),
        "profile_completeness": _fingerprint_profile_completeness(
            payload,
            profile=profile,
        ),
        "dynamics_readiness": dynamics_readiness,
        "stationarity_readiness": stationarity_readiness,
        "decomposition_readiness": decomposition_readiness,
    }
    return audit_payload


def _fingerprint_expected_sections(
    target: QualityTarget,
) -> tuple[str, ...]:
    sections = ["coverage", "temporal_topology"]
    if target.timeframe in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES:
        sections.append("calendar_regimes")
    if target.timeframe == TICK:
        sections.extend(
            (
                "tick_distribution",
                "conditional_distributions",
                "microstructure_dynamics",
                "dependence",
                "stationarity_diagnostics",
                "decomposition",
                "synthetic_constraints",
            )
        )
    return tuple(sections)


def _ordered_unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _fingerprint_section_skip_payload(
    section: str,
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
) -> dict[str, JSONValue]:
    reason = _fingerprint_section_skip_reason(
        section,
        payload,
        target=target,
    )
    skipped: dict[str, JSONValue] = {"reason": reason}
    details = _fingerprint_section_skip_details(section, target)
    if details:
        skipped["details"] = details
    return skipped


def _fingerprint_section_skip_details(
    section: str,
    target: QualityTarget,
) -> dict[str, JSONValue]:
    if section == "conditional_distributions":
        return {
            "metric": "tick_spread",
            "grouped_by": ["active_session", "special_tag"],
        }
    if section in {
        "tick_distribution",
        "microstructure_dynamics",
        "dependence",
        "stationarity_diagnostics",
        "decomposition",
        "classical_baselines",
        "classical_model_input",
        "exponential_smoothing",
        "autoregressive",
        "synthetic_constraints",
    }:
        return {"timeframe": target.timeframe}
    return {}


def _fingerprint_section_skip_reason(
    section: str,
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
) -> str:
    if _section_timeframe_mismatch(section, target):
        return "unsupported_timeframe"
    unsupported_reason = _unsupported_reason(target)
    if unsupported_reason:
        return unsupported_reason
    source = _payload_mapping(payload.get("source"))
    if source.get("kind") == "unavailable":
        return _summary_key(source.get("reason") or "source_unavailable")
    if section == "conditional_distributions":
        eligibility = _conditional_tick_spread_eligibility(
            payload,
            target=target,
        )
        return _summary_key(eligibility.get("reason") or "not_emitted")
    if (
        _int_payload(_payload_mapping(payload.get("coverage")).get("row_count"))
        <= 0
    ):
        return "insufficient_rows"
    return "not_emitted"


def _section_timeframe_mismatch(
    section: str,
    target: QualityTarget,
) -> bool:
    return (
        (
            section
            in {
                "tick_distribution",
                "conditional_distributions",
                "microstructure_dynamics",
                "synthetic_constraints",
                "classical_baselines",
                "classical_model_input",
                "exponential_smoothing",
                "autoregressive",
            }
            and target.timeframe != TICK
        )
        or (
            section == "dependence"
            and target.timeframe not in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES
        )
        or (
            section == "stationarity_diagnostics"
            and target.timeframe not in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES
        )
        or (
            section == "decomposition"
            and target.timeframe not in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES
        )
    )


def _fingerprint_section_status(
    section: str,
    payload: Mapping[str, JSONValue],
) -> str:
    if section not in payload:
        return "skipped"
    source = _payload_mapping(payload.get("source"))
    if source.get("kind") == "unavailable" and section in {
        "coverage",
        "temporal_topology",
    }:
        return "unavailable"
    if section == "coverage":
        return _coverage_section_status(
            _payload_mapping(payload.get("coverage"))
        )
    if section == "temporal_topology":
        return _topology_section_status(
            _payload_mapping(payload.get("temporal_topology"))
        )
    if section == "calendar_regimes":
        return _calendar_section_status(
            _payload_mapping(payload.get("calendar_regimes"))
        )
    if section == "tick_distribution":
        return _distribution_section_status(_payload_mapping(payload[section]))
    if section == "conditional_distributions":
        return _conditional_distribution_section_status(
            _payload_mapping(payload.get("conditional_distributions"))
        )
    if section == "microstructure_dynamics":
        return _dynamics_section_status(_payload_mapping(payload[section]))
    if section == "dependence":
        return _dependence_section_status(_payload_mapping(payload[section]))
    if section == "stationarity_diagnostics":
        return _stationarity_section_status(_payload_mapping(payload[section]))
    if section == "decomposition":
        return _decomposition_section_status(_payload_mapping(payload[section]))
    if section == "cache_source_parity":
        parity_status = _summary_key(
            _payload_mapping(payload[section]).get("status")
        )
        if parity_status == "match":
            return "valid"
        if parity_status == "mismatch":
            return "limited"
        return "unavailable"
    if section == "synthetic_constraints":
        constraint_status = _summary_key(
            _payload_mapping(payload[section]).get("status")
        )
        if constraint_status == "ready":
            return "valid"
        if constraint_status == "limited":
            return "limited"
        return "unavailable"
    if section == "classical_baselines":
        baseline_status = _summary_key(
            _payload_mapping(payload[section]).get("status")
        )
        if baseline_status == "ready":
            return "valid"
        if baseline_status == "limited":
            return "limited"
        return "unavailable"
    if section == "classical_model_input":
        input_status = _summary_key(
            _payload_mapping(payload[section]).get("status")
        )
        if input_status == "ready":
            return "valid"
        if input_status == "limited":
            return "limited"
        return "unavailable"
    if section == "exponential_smoothing":
        model_status = _summary_key(
            _payload_mapping(payload[section]).get("status")
        )
        if model_status == "ready":
            return "valid"
        if model_status == "limited":
            return "limited"
        return "unavailable"
    if section == "autoregressive":
        model_status = _summary_key(
            _payload_mapping(payload[section]).get("status")
        )
        if model_status == "ready":
            return "valid"
        if model_status == "limited":
            return "limited"
        return "unavailable"
    return "valid"


def _coverage_section_status(
    coverage: Mapping[str, JSONValue],
) -> str:
    parsed = _optional_int_payload(coverage.get("parsed_row_count"))
    if parsed is None:
        return "unavailable"
    if _int_payload(coverage.get("row_count")) <= 0:
        return "limited"
    return "valid"


def _topology_section_status(
    topology: Mapping[str, JSONValue],
) -> str:
    parsed = _optional_int_payload(topology.get("parsed_row_count"))
    if parsed is None:
        return "unavailable"
    if _sequence_dynamics_limitations(topology):
        return "limited"
    return "valid"


def _calendar_section_status(
    calendar: Mapping[str, JSONValue],
) -> str:
    status = _summary_key(calendar.get("status"))
    if status == "ok":
        return "valid"
    if status == "unavailable":
        return "unavailable"
    return "limited"


def _distribution_section_status(
    distribution: Mapping[str, JSONValue],
) -> str:
    if _int_payload(distribution.get("usable_row_count")) <= 0:
        return "limited"
    if (
        _int_payload(distribution.get("invalid_row_count")) > 0
        or _int_payload(distribution.get("partial_row_count")) > 0
        or distribution.get("truncated") is True
    ):
        return "limited"
    return "valid"


def _conditional_distribution_section_status(
    conditional: Mapping[str, JSONValue],
) -> str:
    if _int_payload(conditional.get("usable_row_count")) <= 0:
        return "limited"
    return "valid"


def _dynamics_section_status(
    dynamics: Mapping[str, JSONValue],
) -> str:
    status = _summary_key(dynamics.get("sequence_status"))
    if status == "ok":
        return "valid"
    if status in {"limited", "unavailable"}:
        return status
    return "limited"


def _dependence_section_status(
    dependence: Mapping[str, JSONValue],
) -> str:
    status = _summary_key(dependence.get("dependence_status"))
    if status == "ok":
        return "valid"
    if status == "unavailable":
        return "unavailable"
    return "limited"


def _stationarity_section_status(
    stationarity: Mapping[str, JSONValue],
) -> str:
    status = _summary_key(stationarity.get("stationarity_status"))
    if status == "ok":
        return "valid"
    if status == "unavailable":
        return "unavailable"
    return "limited"


def _decomposition_section_status(
    decomposition: Mapping[str, JSONValue],
) -> str:
    status = _summary_key(decomposition.get("decomposition_status"))
    if status == "ok":
        return "valid"
    if status == "unavailable":
        return "unavailable"
    return "limited"


def _conditional_tick_spread_eligibility(
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
) -> dict[str, JSONValue]:
    if target.timeframe != TICK:
        return {
            "eligible": False,
            "status": "ineligible",
            "reason": "unsupported_timeframe",
        }
    unsupported_reason = _unsupported_reason(target)
    if unsupported_reason:
        return {
            "eligible": False,
            "status": "ineligible",
            "reason": unsupported_reason,
        }
    source = _payload_mapping(payload.get("source"))
    if source.get("kind") == "unavailable":
        return {
            "eligible": False,
            "status": "ineligible",
            "reason": _summary_key(source.get("reason")),
        }
    distribution = _payload_mapping(payload.get("tick_distribution"))
    if not distribution:
        return {
            "eligible": False,
            "status": "ineligible",
            "reason": "missing_required_columns",
        }
    if _int_payload(distribution.get("row_count")) <= 0:
        return {
            "eligible": False,
            "status": "ineligible",
            "reason": "insufficient_rows",
        }
    if _int_payload(distribution.get("usable_row_count")) <= 0:
        return {
            "eligible": False,
            "status": "ineligible",
            "reason": "metric_not_available",
        }
    return {
        "eligible": True,
        "status": "eligible",
        "metric": "tick_spread",
        "grouped_by": ["active_session", "special_tag"],
        "emitted": "conditional_distributions" in payload,
    }


def _fingerprint_profile_completeness(
    payload: Mapping[str, JSONValue],
    *,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    calendar = _payload_mapping(payload.get("calendar_regimes"))
    policy = _payload_mapping(calendar.get("calendar_policy"))
    profile_metadata = _payload_mapping(policy.get("calendar_profile"))
    source = "calendar_regimes"
    if not profile_metadata:
        profile_metadata = profile.calendar_profile.to_metadata()
        source = "quality_profile"
    complete = bool(
        calendar.get("calendar_profile_complete")
        if calendar
        else profile_metadata.get("complete")
    )
    missing_optional = bool(
        calendar.get("missing_optional_calendar_data")
        if calendar
        else not profile.calendar_profile.complete
    )
    return {
        "source": source,
        "calendar_profile_complete": complete,
        "missing_optional_calendar_data": missing_optional,
        "calendar_profile_name": _summary_key(profile_metadata.get("name")),
        "calendar_profile_source": _summary_key(profile_metadata.get("source")),
        "calendar_profile_version": _summary_key(
            profile_metadata.get("version")
        ),
        "calendar_profile_static_advisory": bool(
            profile_metadata.get("static_advisory")
        ),
    }


def _fingerprint_dynamics_readiness(
    section: str,
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
) -> dict[str, JSONValue]:
    if section not in payload:
        return {
            "status": "skipped",
            "reason": _fingerprint_section_skip_reason(
                section,
                payload,
                target=target,
            ),
        }
    dynamics = _payload_mapping(payload.get(section))
    readiness: dict[str, JSONValue] = {
        "status": _dynamics_section_status(dynamics),
        "basis": _summary_key(dynamics.get("basis")),
        "row_order": _summary_key(dynamics.get("row_order")),
        "computed_from": _summary_key(dynamics.get("computed_from")),
        "cache_source": _optional_summary_key(dynamics.get("cache_source")),
        "regular_grid": dynamics.get("regular_grid") is True,
        "limitations": _string_list(dynamics.get("limitations")),
        "row_count": _int_payload(dynamics.get("row_count")),
        "sampled_row_count": _int_payload(dynamics.get("sampled_row_count")),
        "usable_row_count": _int_payload(dynamics.get("usable_row_count")),
        "invalid_row_count": _int_payload(dynamics.get("invalid_row_count")),
        "partial_row_count": _int_payload(dynamics.get("partial_row_count")),
        "truncated": dynamics.get("truncated") is True,
    }
    if readiness["status"] in {"limited", "unavailable"}:
        readiness["reason"] = _summary_key(
            _string_list(dynamics.get("limitations"))[0]
            if _string_list(dynamics.get("limitations"))
            else dynamics.get("sequence_status")
        )
    return readiness


def _fingerprint_stationarity_readiness(
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
) -> dict[str, JSONValue]:
    section = "stationarity_diagnostics"
    if section not in payload:
        return {
            "status": "skipped",
            "reason": _fingerprint_section_skip_reason(
                section,
                payload,
                target=target,
            ),
        }
    stationarity = _payload_mapping(payload.get(section))
    sample_counts = _payload_mapping(stationarity.get("sample_counts"))
    readiness: dict[str, JSONValue] = {
        "status": _stationarity_section_status(stationarity),
        "reason": _optional_summary_key(stationarity.get("reason")),
        "basis": _summary_key(stationarity.get("basis")),
        "calculation_basis": _summary_key(
            stationarity.get("calculation_basis")
        ),
        "row_order": _summary_key(stationarity.get("row_order")),
        "computed_from": _summary_key(stationarity.get("computed_from")),
        "cache_source": _optional_summary_key(stationarity.get("cache_source")),
        "regular_grid": stationarity.get("regular_grid") is True,
        "metric": _summary_key(stationarity.get("metric")),
        "limitations": _string_list(stationarity.get("limitations")),
        "row_count": _int_payload(stationarity.get("row_count")),
        "sampled_row_count": _int_payload(
            stationarity.get("sampled_row_count")
        ),
        "usable_row_count": _int_payload(stationarity.get("usable_row_count")),
        "invalid_row_count": _int_payload(
            stationarity.get("invalid_row_count")
        ),
        "partial_row_count": _int_payload(
            stationarity.get("partial_row_count")
        ),
        "truncated": stationarity.get("truncated") is True,
        "level_sample_count": _int_payload(sample_counts.get("level")),
        "return_sample_count": _int_payload(sample_counts.get("return")),
        "windows": list(_int_sequence_payload(stationarity.get("windows"))),
        "rounding_digits": _int_payload(stationarity.get("rounding_digits")),
        "computed_window_count": _int_payload(
            stationarity.get("computed_window_count")
        ),
        "skipped_window_count": _int_payload(
            stationarity.get("skipped_window_count")
        ),
        "skipped_window_reason_counts": _counter_payload(
            _counter_from_mapping(
                _payload_mapping(
                    stationarity.get("skipped_window_reason_counts")
                )
            )
        ),
        "zero_variance_metrics": _string_list(
            stationarity.get("zero_variance_metrics")
        ),
        "recommended_transforms": _string_list(
            stationarity.get("recommended_transforms")
        ),
    }
    if readiness["status"] in {"limited", "unavailable"} and not readiness.get(
        "reason"
    ):
        readiness["reason"] = _summary_key(
            _string_list(stationarity.get("limitations"))[0]
            if _string_list(stationarity.get("limitations"))
            else stationarity.get("stationarity_status")
        )
    return readiness


def _fingerprint_decomposition_readiness(
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
) -> dict[str, JSONValue]:
    section = "decomposition"
    if section not in payload:
        return {
            "status": "skipped",
            "reason": _fingerprint_section_skip_reason(
                section,
                payload,
                target=target,
            ),
        }
    decomposition = _payload_mapping(payload.get(section))
    sample_counts = _payload_mapping(decomposition.get("sample_counts"))
    stationarity = _payload_mapping(decomposition.get("stationarity_basis"))
    structural_break = _payload_mapping(
        decomposition.get("structural_break_proxy")
    )
    trend = _payload_mapping(decomposition.get("trend_proxy"))
    readiness: dict[str, JSONValue] = {
        "status": _decomposition_section_status(decomposition),
        "reason": _optional_summary_key(decomposition.get("reason")),
        "basis": _summary_key(decomposition.get("basis")),
        "calculation_basis": _summary_key(
            decomposition.get("calculation_basis")
        ),
        "row_order": _summary_key(decomposition.get("row_order")),
        "computed_from": _summary_key(decomposition.get("computed_from")),
        "cache_source": _optional_summary_key(
            decomposition.get("cache_source")
        ),
        "regular_grid": decomposition.get("regular_grid") is True,
        "metric": _summary_key(decomposition.get("metric")),
        "limitations": _string_list(decomposition.get("limitations")),
        "row_count": _int_payload(decomposition.get("row_count")),
        "sampled_row_count": _int_payload(
            decomposition.get("sampled_row_count")
        ),
        "usable_row_count": _int_payload(decomposition.get("usable_row_count")),
        "invalid_row_count": _int_payload(
            decomposition.get("invalid_row_count")
        ),
        "partial_row_count": _int_payload(
            decomposition.get("partial_row_count")
        ),
        "truncated": decomposition.get("truncated") is True,
        "level_sample_count": _int_payload(sample_counts.get("level")),
        "return_sample_count": _int_payload(sample_counts.get("return")),
        "windows": list(_int_sequence_payload(decomposition.get("windows"))),
        "rounding_digits": _int_payload(decomposition.get("rounding_digits")),
        "computed_window_count": _int_payload(
            decomposition.get("computed_window_count")
        ),
        "skipped_window_count": _int_payload(
            decomposition.get("skipped_window_count")
        ),
        "skipped_window_reason_counts": _counter_payload(
            _counter_from_mapping(
                _payload_mapping(
                    decomposition.get("skipped_window_reason_counts")
                )
            )
        ),
        "stationarity": {
            "status": _summary_key(stationarity.get("status")),
            "stationarity_status": _summary_key(
                stationarity.get("stationarity_status")
            ),
            "computed_window_count": _int_payload(
                stationarity.get("computed_window_count")
            ),
            "skipped_window_count": _int_payload(
                stationarity.get("skipped_window_count")
            ),
            "zero_variance_metrics": _string_list(
                stationarity.get("zero_variance_metrics")
            ),
            "recommended_transforms": _string_list(
                stationarity.get("recommended_transforms")
            ),
        },
        "trend": {
            "status": _summary_key(trend.get("status")),
            "direction": _summary_key(trend.get("direction")),
            "slope_per_observation": trend.get("slope_per_observation"),
            "trend_strength": trend.get("trend_strength"),
        },
        "structural_break": {
            "status": _summary_key(structural_break.get("status")),
            "candidate_count": _int_payload(
                structural_break.get("candidate_count")
            ),
            "strongest_candidate": dict(
                _payload_mapping(structural_break.get("strongest_candidate"))
            ),
        },
        "training_projection": dict(
            _payload_mapping(decomposition.get("training_projection"))
        ),
    }
    if readiness["status"] in {"limited", "unavailable"} and not readiness.get(
        "reason"
    ):
        readiness["reason"] = _summary_key(
            _string_list(decomposition.get("limitations"))[0]
            if _string_list(decomposition.get("limitations"))
            else decomposition.get("decomposition_status")
        )
    return readiness


def _string_list(value: object) -> list[JSONValue]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item or "").strip()]


def _payload_mapping(value: object) -> Mapping[str, JSONValue]:
    if isinstance(value, Mapping):
        return cast(Mapping[str, JSONValue], value)
    return {}


def _payload_mapping_rows(value: object) -> list[Mapping[str, JSONValue]]:
    if not isinstance(value, list):
        return []
    return [
        cast(Mapping[str, JSONValue], item)
        for item in value
        if isinstance(item, Mapping)
    ]


def _summary_key(value: object) -> str:
    text = str(value or "").strip()
    return text or "unknown"


def _counter_payload(counter: Counter[str]) -> dict[str, JSONValue]:
    return {key: counter[key] for key in sorted(counter)}


def _counter_from_mapping(mapping: Mapping[str, JSONValue]) -> Counter[str]:
    counter: Counter[str] = Counter()
    for key, value in mapping.items():
        count = _int_payload(value)
        if count > 0:
            counter[_summary_key(key)] += count
    return counter


def _int_sequence_payload(value: JSONValue) -> tuple[int, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    parsed: list[int] = []
    for item in value:
        if isinstance(item, bool):
            continue
        try:
            parsed.append(int(item))  # type: ignore[arg-type]
        except (TypeError, ValueError):
            continue
    return tuple(parsed)


def _has_parsed_non_empty_coverage(
    coverage: Mapping[str, JSONValue],
) -> bool:
    parsed_row_count = coverage.get("parsed_row_count")
    if parsed_row_count is not None:
        return _positive_count(parsed_row_count)
    return _positive_count(coverage.get("row_count"))


def _positive_count(value: object) -> bool:
    if isinstance(value, bool) or value is None:
        return False
    if isinstance(value, (int, float)):
        return value > 0
    try:
        return int(str(value)) > 0
    except ValueError:
        return False


def _series_fingerprint_topology_target_summaries(
    findings: Iterable[QualityFinding],
) -> list[dict[str, JSONValue]]:
    target_summaries: list[dict[str, JSONValue]] = []
    for finding in findings:
        if finding.rule_id != SERIES_FINGERPRINT_RULE_ID:
            continue
        payload = finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
        if not isinstance(payload, Mapping):
            continue
        topology = _payload_mapping(payload.get("temporal_topology"))
        if not topology:
            continue
        target_axis = _payload_mapping(payload.get("target_axis"))
        calendar = _payload_mapping(payload.get("calendar_regimes"))
        calendar_policy = _compact_calendar_policy(
            _payload_mapping(calendar.get("calendar_policy"))
        )
        target_summaries.append(
            _topology_target_summary(
                finding,
                target_axis,
                topology,
                calendar_policy=calendar_policy,
            )
        )
    return target_summaries


def _topology_target_summary(
    finding: QualityFinding,
    target_axis: Mapping[str, JSONValue],
    topology: Mapping[str, JSONValue],
    *,
    calendar_policy: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    flags = _topology_flags(topology)
    summary: dict[str, JSONValue] = {
        "target_axis": _topology_target_axis(finding, target_axis),
        "row_count": _int_payload(topology.get("row_count")),
        "parsed_row_count": _optional_int_payload(
            topology.get("parsed_row_count")
        ),
        "invalid_timestamp_count": _int_payload(
            topology.get("invalid_timestamp_count")
        ),
        "duplicate_timestamp_count": _int_payload(
            topology.get("duplicate_timestamp_count")
        ),
        "non_monotonic_count": _int_payload(
            topology.get("non_monotonic_count")
        ),
        "median_interval_ms": _optional_int_payload(
            topology.get("median_interval_ms")
        ),
        "max_gap_ms": _optional_int_payload(topology.get("max_gap_ms")),
        "suspicious_gap_count": _int_payload(
            topology.get("suspicious_gap_count")
        ),
        "expected_session_closure_count": _int_payload(
            topology.get("expected_session_closure_count")
        ),
        "weekend_activity_count": _int_payload(
            topology.get("weekend_activity_count")
        ),
        "sampling_basis": _summary_key(topology.get("sampling_basis")),
        "computed_from": _summary_key(topology.get("computed_from")),
        "cache_source": _optional_summary_key(topology.get("cache_source")),
        "status": _topology_status(topology),
        "flags": flags,
    }
    inspection_context = _payload_mapping(topology.get("inspection_context"))
    if inspection_context:
        summary["inspection_context"] = dict(inspection_context)
    if calendar_policy:
        summary["calendar_policy"] = dict(calendar_policy)
    return summary


def _compact_calendar_policy(
    policy: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    """Return bounded calendar policy fields needed by remediation guidance."""
    if not policy:
        return {}
    compact: dict[str, JSONValue] = {}
    for key in (
        "source_timezone",
        "canonical_timezone",
        "holiday_calendar_source",
        "holiday_calendar_complete",
        "holiday_calendar_static_advisory",
        "weekend_activity_policy",
        "expected_session_closure_policy",
    ):
        value = policy.get(key)
        if value is not None:
            compact[key] = _bounded_calendar_policy_value(value)
    profile = _payload_mapping(policy.get("calendar_profile"))
    compact_profile: dict[str, JSONValue] = {}
    for key in (
        "name",
        "source",
        "version",
        "complete",
        "static_advisory",
        "weekend_activity_policy",
        "expected_session_closure_policy",
    ):
        value = profile.get(key)
        if value is not None:
            compact_profile[key] = _bounded_calendar_policy_value(value)
    if compact_profile:
        compact["calendar_profile"] = compact_profile
    return compact


def _bounded_calendar_policy_value(value: JSONValue) -> JSONValue:
    if isinstance(value, str):
        return value[:_CALENDAR_POLICY_CONTEXT_TEXT_LIMIT]
    return value


def _topology_target_axis(
    finding: QualityFinding,
    target_axis: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        "data_format": _summary_key(
            target_axis.get("data_format") or finding.target.data_format
        ),
        "timeframe": _summary_key(
            target_axis.get("timeframe") or finding.target.timeframe
        ),
        "symbol": _summary_key(
            target_axis.get("symbol") or finding.target.symbol
        ),
        "period": _summary_key(
            target_axis.get("period") or finding.target.period
        ),
        "kind": _summary_key(
            target_axis.get("kind") or finding.target.kind.value
        ),
    }


def _topology_status(topology: Mapping[str, JSONValue]) -> str:
    if _summary_key(topology.get("sampling_basis")) == "unavailable":
        return "unavailable"
    if _summary_key(topology.get("computed_from")) == "unavailable":
        return "unavailable"
    if (
        _int_payload(topology.get("invalid_timestamp_count"))
        or _int_payload(topology.get("duplicate_timestamp_count"))
        or _int_payload(topology.get("non_monotonic_count"))
        or _int_payload(topology.get("suspicious_gap_count"))
        or _int_payload(topology.get("weekend_activity_count"))
    ):
        return "irregular"
    return "regular"


def _topology_flags(topology: Mapping[str, JSONValue]) -> list[JSONValue]:
    flags: list[JSONValue] = []
    computed_from = _summary_key(topology.get("computed_from"))
    if _summary_key(topology.get("sampling_basis")) == "unavailable":
        flags.append("unavailable_topology")
    if computed_from in {"direct_cache", "fresh_sibling_cache"}:
        flags.append("cache_backed")
    if _int_payload(topology.get("invalid_timestamp_count")):
        flags.append("invalid_timestamps")
    if _int_payload(topology.get("duplicate_timestamp_count")):
        flags.append("duplicate_timestamps")
    if _int_payload(topology.get("non_monotonic_count")):
        flags.append("non_monotonic_timestamps")
    if _int_payload(topology.get("suspicious_gap_count")):
        flags.append("suspicious_gaps")
    if _int_payload(topology.get("expected_session_closure_count")):
        flags.append("expected_session_closures")
    if _int_payload(topology.get("weekend_activity_count")):
        flags.append("weekend_activity")
    return flags


def _topology_attention_summary_from_targets(
    target_summaries: list[dict[str, JSONValue]],
    *,
    target_limit: BoundedReportLimit,
) -> dict[str, JSONValue]:
    attention_targets = [
        attention
        for target in target_summaries
        if (attention := _topology_attention_target_summary(target)) is not None
    ]
    attention_targets.sort(key=_topology_attention_sort_key)

    priority_counts = Counter(
        _summary_key(target.get("attention_level"))
        for target in attention_targets
    )
    flag_counts: Counter[str] = Counter()
    for target in attention_targets:
        flags = target.get("attention_flags")
        if isinstance(flags, list):
            flag_counts.update(_summary_key(flag) for flag in flags)

    included: list[JSONValue] = [
        dict(item) for item in target_limit.slice(attention_targets)
    ]
    omitted_count = max(0, len(attention_targets) - len(included))

    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_TOPOLOGY_ATTENTION_SCHEMA_VERSION
        ),
        "rule_id": SERIES_FINGERPRINT_RULE_ID,
        "topology_target_count": len(target_summaries),
        "attention_target_count": len(attention_targets),
        "included_attention_target_count": len(included),
        "omitted_attention_target_count": omitted_count,
        "truncated": omitted_count > 0,
        "limit_metadata": {"targets": target_limit.limit_payload()},
        "attention_level_counts": _counter_payload(priority_counts),
        "attention_flag_counts": _counter_payload(flag_counts),
        "target_summaries": included,
    }


def _topology_attention_target_summary(
    target: Mapping[str, JSONValue],
) -> dict[str, JSONValue] | None:
    flags = _topology_summary_flags(target.get("flags"))
    flag_set = set(flags)
    calendar_policy = _payload_mapping(target.get("calendar_policy"))
    attention_flags = [
        flag for flag in ACTIONABLE_TOPOLOGY_FLAGS if flag in flag_set
    ]
    if (
        "expected_session_closures" in flag_set
        and _summary_key(calendar_policy.get("expected_session_closure_policy"))
        == "unexpected"
    ):
        attention_flags.append("expected_session_closures")
    if not attention_flags:
        return None
    attention_level = _topology_attention_level(
        attention_flags,
        calendar_policy=calendar_policy,
    )
    summary: dict[str, JSONValue] = {
        "target_axis": _topology_attention_axis(target),
        "attention_level": attention_level,
        "attention_flags": list(attention_flags),
        "remediation_hints": remediation_hint_payloads_for_flags(
            attention_flags,
            calendar_policy=calendar_policy,
        ),
        "flags": list(flags),
        "status": _summary_key(target.get("status")),
        "invalid_timestamp_count": _int_payload(
            target.get("invalid_timestamp_count")
        ),
        "duplicate_timestamp_count": _int_payload(
            target.get("duplicate_timestamp_count")
        ),
        "non_monotonic_count": _int_payload(target.get("non_monotonic_count")),
        "suspicious_gap_count": _int_payload(
            target.get("suspicious_gap_count")
        ),
        "weekend_activity_count": _int_payload(
            target.get("weekend_activity_count")
        ),
        "expected_session_closure_count": _int_payload(
            target.get("expected_session_closure_count")
        ),
        "max_gap_ms": _optional_int_payload(target.get("max_gap_ms")),
        "computed_from": _summary_key(target.get("computed_from")),
        "cache_source": _optional_summary_key(target.get("cache_source")),
    }
    if calendar_policy:
        summary["calendar_policy"] = dict(calendar_policy)
    inspection_context = _topology_attention_inspection_context(
        target,
        attention_flags,
        calendar_policy=calendar_policy,
    )
    if inspection_context:
        summary["inspection_context"] = inspection_context
    return summary


def _topology_attention_inspection_context(
    target: Mapping[str, JSONValue],
    attention_flags: list[str],
    *,
    calendar_policy: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    raw = _payload_mapping(target.get("inspection_context"))
    if not raw:
        return {}
    context: dict[str, JSONValue] = {}
    schema_version = raw.get("schema_version")
    if schema_version is not None:
        context["schema_version"] = schema_version
    section_flags = (
        ("invalid_timestamps", "invalid_timestamps"),
        ("non_monotonic_timestamps", "non_monotonic_timestamps"),
        ("duplicate_timestamps", "duplicate_timestamps"),
        ("suspicious_gaps", "suspicious_gaps"),
        ("weekend_activity", "weekend_activity"),
    )
    attention_flag_set = set(attention_flags)
    target_axis = _topology_attention_axis(target)
    for section_name, flag in section_flags:
        section = _payload_mapping(raw.get(section_name))
        if flag not in attention_flag_set or not section:
            continue
        hints = remediation_hint_payloads_for_flags(
            (flag,),
            calendar_policy=calendar_policy,
        )
        linked = dict(section)
        hint = hints[0] if hints and isinstance(hints[0], Mapping) else {}
        policy_context = _payload_mapping(hint.get("policy_context"))
        actionable = policy_context.get("actionable") is not False
        linked["actionable"] = actionable
        linked["target_axis"] = target_axis
        if hints:
            linked["next_action" if actionable else "policy_note"] = hints[0]
        context[section_name] = linked
    expected_closures = _payload_mapping(raw.get("expected_session_closures"))
    if "expected_session_closures" in attention_flag_set and expected_closures:
        linked = dict(expected_closures)
        hints = remediation_hint_payloads_for_flags(
            ("expected_session_closures",),
            calendar_policy=calendar_policy,
        )
        linked["actionable"] = True
        linked["target_axis"] = target_axis
        if hints:
            linked["next_action"] = hints[0]
        context["expected_session_closures"] = linked
    elif "suspicious_gaps" in attention_flag_set and expected_closures:
        contextual = dict(expected_closures)
        contextual["actionable"] = False
        contextual["contextual_for"] = "suspicious_gaps"
        contextual["target_axis"] = target_axis
        context["expected_session_closures"] = contextual
    if len(context) == int("schema_version" in context):
        return {}
    return context


def _topology_attention_axis(
    target: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    axis = _payload_mapping(target.get("target_axis"))
    return {
        "data_format": _summary_key(axis.get("data_format")),
        "timeframe": _summary_key(axis.get("timeframe")),
        "symbol": _summary_key(axis.get("symbol")),
        "period": _summary_key(axis.get("period")),
        "kind": _summary_key(axis.get("kind")),
    }


def _topology_summary_flags(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    flags: list[str] = []
    for flag in value:
        text = str(flag or "").strip()
        if text:
            flags.append(text)
    return tuple(flags)


def _topology_attention_level(
    flags: list[str],
    *,
    calendar_policy: Mapping[str, JSONValue],
) -> str:
    flag_set = set(flags)
    if "unavailable_topology" in flag_set:
        return "unavailable"
    if flag_set & {"invalid_timestamps", "non_monotonic_timestamps"}:
        return "structural"
    if flag_set & {"duplicate_timestamps", "suspicious_gaps"}:
        return "sequence"
    if (
        flag_set == {"weekend_activity"}
        and _summary_key(calendar_policy.get("weekend_activity_policy"))
        == "allowed"
    ):
        return "contextual"
    return "session"


def _topology_attention_sort_key(
    target: Mapping[str, JSONValue],
) -> tuple[object, ...]:
    axis = _payload_mapping(target.get("target_axis"))
    return (
        _topology_attention_level_rank(
            _summary_key(target.get("attention_level"))
        ),
        _summary_key(axis.get("data_format")),
        _summary_key(axis.get("timeframe")),
        _summary_key(axis.get("symbol")),
        _summary_key(axis.get("period")),
        _summary_key(axis.get("kind")),
        -_int_payload(target.get("invalid_timestamp_count")),
        -_int_payload(target.get("non_monotonic_count")),
        -_int_payload(target.get("duplicate_timestamp_count")),
        -_int_payload(target.get("suspicious_gap_count")),
        -_int_payload(target.get("weekend_activity_count")),
        _summary_key(target.get("computed_from")),
    )


def _topology_attention_level_rank(level: str) -> int:
    ranks = {
        "unavailable": 0,
        "structural": 1,
        "sequence": 2,
        "session": 3,
        "contextual": 4,
    }
    return ranks.get(level, 99)


def _optional_summary_key(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int_payload(value: object) -> int:
    if isinstance(value, bool) or value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value))
    except ValueError:
        return 0


def _optional_int_payload(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(str(value))
    except ValueError:
        return None


@dataclass(frozen=True, slots=True)
class _TextPayload:
    text: str
    source_member: str = ""


@dataclass(frozen=True, slots=True)
class _TickDynamicsRow:
    timestamp_utc_ms: int
    bid: float
    ask: float

    @property
    def spread(self) -> float:
        return self.ask - self.bid


@dataclass(slots=True)
class _CoverageScan:
    row_count: int = 0
    parsed_row_count: int = 0
    start_timestamp_utc_ms: int | None = None
    end_timestamp_utc_ms: int | None = None

    def to_payload(self) -> dict[str, JSONValue]:
        """Return canonical coverage metadata."""
        return {
            "row_count": self.row_count,
            "parsed_row_count": self.parsed_row_count,
            "start_timestamp_utc_ms": self.start_timestamp_utc_ms,
            "end_timestamp_utc_ms": self.end_timestamp_utc_ms,
            "duration_ms": _duration_ms(
                self.start_timestamp_utc_ms,
                self.end_timestamp_utc_ms,
            ),
        }


def _target_axis(target: QualityTarget) -> dict[str, JSONValue]:
    return {
        "data_format": target.data_format,
        "timeframe": target.timeframe,
        "symbol": target.symbol,
        "period": target.period,
        "kind": target.kind.value,
    }


def _target_asset_class(target: QualityTarget) -> str:
    return str(symbol_metadata_for(target.symbol).asset_class)


def _empty_coverage(
    *,
    parsed_row_count: int | None,
) -> dict[str, JSONValue]:
    return {
        "row_count": 0,
        "parsed_row_count": parsed_row_count,
        "start_timestamp_utc_ms": None,
        "end_timestamp_utc_ms": None,
        "duration_ms": None,
    }


def _coverage_from_frame(frame: Any) -> dict[str, JSONValue]:
    row_count = int(getattr(frame, "height", 0) or 0)
    start = _cache_timestamp_at(frame, 0)
    end = _cache_timestamp_at(frame, row_count - 1)
    return {
        "row_count": row_count,
        "parsed_row_count": row_count,
        "start_timestamp_utc_ms": start,
        "end_timestamp_utc_ms": end,
        "duration_ms": _duration_ms(start, end),
    }


def _coverage_from_text(
    text: str,
    *,
    timeframe: str,
) -> dict[str, JSONValue]:
    scan = _CoverageScan()
    reader = csv.reader(
        text.splitlines(),
        delimiter=delimiter_for_timeframe(timeframe),
    )
    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        scan.row_count += 1
        try:
            parsed = normalize_ascii_row(timeframe, row)
        except ValueError:
            continue
        timestamp = int(parsed[0])
        scan.parsed_row_count += 1
        if scan.start_timestamp_utc_ms is None:
            scan.start_timestamp_utc_ms = timestamp
        scan.end_timestamp_utc_ms = timestamp
    return scan.to_payload()


def _add_distribution_payload(
    payload: dict[str, JSONValue],
    *,
    timeframe: str,
    distribution: dict[str, JSONValue],
) -> None:
    if timeframe == TICK:
        payload["tick_distribution"] = distribution


def _add_dynamics_payload(
    payload: dict[str, JSONValue],
    *,
    target: QualityTarget,
    frame: Any | None,
    text: str | None,
    profile: HistDataFingerprintProfile,
) -> None:
    if target.timeframe == TICK:
        microstructure_dynamics, dependence, stationarity, decomposition = (
            _tick_sequence_payloads(
                payload,
                target=target,
                frame=frame,
                text=text,
                profile=profile,
            )
        )
        payload["microstructure_dynamics"] = microstructure_dynamics
        payload["dependence"] = dependence
        payload["stationarity_diagnostics"] = stationarity
        payload["decomposition"] = decomposition


def _tick_sequence_payloads(
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
    frame: Any | None,
    text: str | None,
    profile: HistDataFingerprintProfile,
) -> tuple[
    dict[str, JSONValue],
    dict[str, JSONValue],
    dict[str, JSONValue],
    dict[str, JSONValue],
]:
    if frame is not None:
        rows, row_count, usable_row_count, invalid_row_count = (
            _tick_dynamics_rows_from_frame(frame, profile)
        )
        row_order = "cache_order"
        partial_row_count = 0
    elif text is not None:
        (
            rows,
            row_count,
            usable_row_count,
            invalid_row_count,
            partial_row_count,
        ) = _tick_dynamics_rows_from_text(text, profile)
        row_order = "source_text_order"
    else:
        rows = []
        row_count = 0
        usable_row_count = 0
        invalid_row_count = 0
        partial_row_count = 0
        row_order = "none"

    base = _sequence_dynamics_base(
        payload,
        row_order=row_order,
        row_count=row_count,
        sampled_row_count=len(rows),
        usable_row_count=usable_row_count,
        invalid_row_count=invalid_row_count,
        partial_row_count=partial_row_count,
        profile=profile,
    )
    stationarity = _tick_stationarity_payload(
        rows,
        base=base,
        profile=profile,
    )
    return (
        _tick_microstructure_dynamics_payload(
            rows,
            base=base,
            profile=profile,
        ),
        _tick_dependence_payload(rows, base=base, profile=profile),
        stationarity,
        _tick_decomposition_payload(
            rows,
            base=base,
            stationarity=stationarity,
            target=target,
            profile=profile,
        ),
    )


def _sequence_dynamics_base(
    payload: Mapping[str, JSONValue],
    *,
    row_order: str,
    row_count: int,
    sampled_row_count: int,
    usable_row_count: int,
    invalid_row_count: int,
    partial_row_count: int,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    topology = _payload_mapping(payload.get("temporal_topology"))
    limitation_values = list(_sequence_dynamics_limitations(topology))
    if usable_row_count < 2:
        limitation_values.append("insufficient_sequence_rows")
    sequence_status = _sequence_dynamics_status(limitation_values)
    limitations_payload: list[JSONValue] = [
        value for value in limitation_values
    ]
    return {
        "schema_version": TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION,
        "basis": "observed_sequence",
        "row_order": row_order,
        "computed_from": _summary_key(topology.get("computed_from")),
        "cache_source": _optional_summary_key(topology.get("cache_source")),
        "regular_grid": False,
        "sequence_status": sequence_status,
        "limitations": limitations_payload,
        "row_count": row_count,
        "sampled_row_count": sampled_row_count,
        "usable_row_count": usable_row_count,
        "invalid_row_count": invalid_row_count,
        "partial_row_count": partial_row_count,
        "truncated": usable_row_count > sampled_row_count,
        "max_rows": _profile_max_rows(profile),
    }


def _sequence_dynamics_limitations(
    topology: Mapping[str, JSONValue],
) -> tuple[str, ...]:
    limitations: list[str] = []
    row_count = _int_payload(topology.get("row_count"))
    parsed_row_count = _optional_int_payload(topology.get("parsed_row_count"))
    if parsed_row_count is None:
        limitations.append("timestamp_topology_unavailable")
    elif row_count > 0 and parsed_row_count <= 0:
        limitations.append("no_parsed_timestamps")
    if _int_payload(topology.get("invalid_timestamp_count")):
        limitations.append("invalid_timestamps_skipped")
    if _int_payload(topology.get("non_monotonic_count")):
        limitations.append("non_monotonic_timestamp_order")
    if _int_payload(topology.get("duplicate_timestamp_count")):
        limitations.append("duplicate_timestamps")
    if _int_payload(topology.get("suspicious_gap_count")):
        limitations.append("suspicious_gaps")
    if _int_payload(topology.get("expected_session_closure_count")):
        limitations.append("expected_session_closures")
    if _int_payload(topology.get("weekend_activity_count")):
        limitations.append("weekend_activity")
    return tuple(limitations)


def _sequence_dynamics_status(limitations: Iterable[str]) -> str:
    limitation_set = set(limitations)
    if limitation_set & {
        "timestamp_topology_unavailable",
        "no_parsed_timestamps",
        "insufficient_sequence_rows",
    }:
        return "unavailable"
    if limitation_set:
        return "limited"
    return "ok"


def _tick_dynamics_rows_from_frame(
    frame: Any,
    profile: HistDataFingerprintProfile,
) -> tuple[list[_TickDynamicsRow], int, int, int]:
    row_count = int(getattr(frame, "height", 0) or 0)
    sample_limit = min(row_count, _profile_max_rows(profile))
    usable_row_count = _frame_numeric_usable_row_count(
        frame,
        ("datetime", "bid", "ask"),
    )
    rows: list[_TickDynamicsRow] = []
    for row in _iter_frame_head_rows(frame, sample_limit):
        timestamp = _finite_int(row.get("datetime"))
        bid = _finite_float(row.get("bid"))
        ask = _finite_float(row.get("ask"))
        if timestamp is None or bid is None or ask is None:
            continue
        rows.append(
            _TickDynamicsRow(
                timestamp_utc_ms=timestamp,
                bid=bid,
                ask=ask,
            )
        )
    return (
        rows,
        row_count,
        usable_row_count,
        max(0, row_count - usable_row_count),
    )


def _tick_dynamics_rows_from_text(
    text: str,
    profile: HistDataFingerprintProfile,
) -> tuple[list[_TickDynamicsRow], int, int, int, int]:
    row_count = 0
    usable_row_count = 0
    invalid_row_count = 0
    partial_row_count = 0
    rows: list[_TickDynamicsRow] = []
    sample_limit = _profile_max_rows(profile)
    expected_field_count = len(columns_for_timeframe(TICK))
    reader = csv.reader(
        text.splitlines(),
        delimiter=delimiter_for_timeframe(TICK),
    )
    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        row_count += 1
        if len(row) != expected_field_count:
            invalid_row_count += 1
            partial_row_count += 1
            continue
        try:
            parsed = normalize_ascii_row(TICK, row)
        except (TypeError, ValueError, OverflowError):
            invalid_row_count += 1
            continue
        timestamp = int(parsed[0])
        bid = _finite_float(parsed[1])
        ask = _finite_float(parsed[2])
        if bid is None or ask is None:
            invalid_row_count += 1
            continue
        usable_row_count += 1
        if len(rows) >= sample_limit:
            continue
        rows.append(_TickDynamicsRow(timestamp, bid, ask))
    return (
        rows,
        row_count,
        usable_row_count,
        invalid_row_count,
        partial_row_count,
    )


def _iter_frame_head_rows(
    frame: Any,
    limit: int,
) -> Iterable[Mapping[str, Any]]:
    if limit <= 0:
        return
    try:
        for row in frame.head(limit).iter_rows(named=True):
            if isinstance(row, Mapping):
                yield row
        return
    except (AttributeError, TypeError, ValueError):
        pass
    columns = tuple(str(column) for column in getattr(frame, "columns", ()))
    column_values = {
        column: _frame_column_values(frame, column, limit) for column in columns
    }
    if not column_values:
        return
    scanned_count = min(
        limit, *(len(values) for values in column_values.values())
    )
    for index in range(scanned_count):
        yield {column: column_values[column][index] for column in columns}


def _tick_microstructure_dynamics_payload(
    rows: list[_TickDynamicsRow],
    *,
    base: dict[str, JSONValue],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    thresholds = DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS
    spread_thresholds = DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS
    spreads = [row.spread for row in rows]
    interarrival_ms: list[float] = []
    spread_changes: list[float] = []
    absolute_spread_changes: list[float] = []
    stale_repeat_count = 0
    stale_run_lengths: list[int] = []
    stale_run_length = 1 if rows else 0
    burst_interval_count = 0
    burst_run_lengths: list[int] = []
    burst_run_length = 1 if rows else 0
    one_sided_movement_count = 0
    bid_only_count = 0
    ask_only_count = 0
    one_sided_run_lengths: list[int] = []
    one_sided_run_length = 0
    one_sided_run_kind = ""
    previous: _TickDynamicsRow | None = None

    for row in rows:
        if previous is None:
            previous = row
            continue

        interval_ms = row.timestamp_utc_ms - previous.timestamp_utc_ms
        interarrival_ms.append(float(interval_ms))
        spread_change = row.spread - previous.spread
        spread_changes.append(spread_change)
        absolute_spread_changes.append(abs(spread_change))

        if (
            row.bid == previous.bid
            and row.ask == previous.ask
            and 0 <= interval_ms <= thresholds.stale_max_gap_ms
        ):
            stale_repeat_count += 1
            stale_run_length += 1
        else:
            _append_minimum_run(
                stale_run_lengths,
                stale_run_length,
                minimum=thresholds.stale_quote_run_length,
            )
            stale_run_length = 1

        if 0 <= interval_ms <= thresholds.burst_max_interval_ms:
            burst_interval_count += 1
            burst_run_length += 1
        else:
            _append_minimum_run(
                burst_run_lengths,
                burst_run_length,
                minimum=thresholds.burst_run_length,
            )
            burst_run_length = 1

        movement_kind = _one_sided_movement_kind(previous, row)
        if movement_kind:
            one_sided_movement_count += 1
            if movement_kind == "bid_only":
                bid_only_count += 1
            elif movement_kind == "ask_only":
                ask_only_count += 1
            if movement_kind == one_sided_run_kind:
                one_sided_run_length += 1
            else:
                _append_minimum_run(
                    one_sided_run_lengths,
                    one_sided_run_length,
                    minimum=thresholds.one_sided_run_length,
                )
                one_sided_run_kind = movement_kind
                one_sided_run_length = 1
        else:
            _append_minimum_run(
                one_sided_run_lengths,
                one_sided_run_length,
                minimum=thresholds.one_sided_run_length,
            )
            one_sided_run_kind = ""
            one_sided_run_length = 0

        previous = row

    _append_minimum_run(
        stale_run_lengths,
        stale_run_length,
        minimum=thresholds.stale_quote_run_length,
    )
    _append_minimum_run(
        burst_run_lengths,
        burst_run_length,
        minimum=thresholds.burst_run_length,
    )
    _append_minimum_run(
        one_sided_run_lengths,
        one_sided_run_length,
        minimum=thresholds.one_sided_run_length,
    )

    spread_jump_threshold = _spread_jump_threshold(spreads)
    spread_jump_count = (
        sum(
            1
            for change in absolute_spread_changes
            if spread_jump_threshold is not None
            and change > spread_jump_threshold
        )
        if spread_jump_threshold is not None
        else 0
    )
    zero_spread_count = sum(1 for spread in spreads if spread == 0.0)
    negative_spread_count = sum(1 for spread in spreads if spread < 0.0)

    result = dict(base)
    result.update(
        {
            "interarrival_ms": _numeric_summary(interarrival_ms, profile),
            "spread": _numeric_summary(spreads, profile),
            "spread_change": _numeric_summary(spread_changes, profile),
            "absolute_spread_change": _numeric_summary(
                absolute_spread_changes,
                profile,
            ),
            "zero_spread_count": zero_spread_count,
            "negative_spread_count": negative_spread_count,
            "zero_spread_rate": _rate(zero_spread_count, len(spreads), profile),
            "negative_spread_rate": _rate(
                negative_spread_count,
                len(spreads),
                profile,
            ),
            "spread_jump": {
                "threshold": (
                    _rounded(spread_jump_threshold, profile)
                    if spread_jump_threshold is not None
                    else None
                ),
                "threshold_basis": (
                    "median_nonnegative_spread_x_jump_multiplier"
                ),
                "jump_spread_multiplier": (
                    spread_thresholds.jump_spread_multiplier
                ),
                "minimum_spread_jump": spread_thresholds.minimum_spread_jump,
                "count": spread_jump_count,
                "rate": _rate(
                    spread_jump_count,
                    len(absolute_spread_changes),
                    profile,
                ),
            },
            "stale_quote": {
                "thresholds": {
                    "stale_quote_run_length": (
                        thresholds.stale_quote_run_length
                    ),
                    "stale_max_gap_ms": thresholds.stale_max_gap_ms,
                },
                "repeat_count": stale_repeat_count,
                "repeat_rate": _rate(
                    stale_repeat_count,
                    len(interarrival_ms),
                    profile,
                ),
                "run_count": len(stale_run_lengths),
                "affected_row_count": sum(stale_run_lengths),
                "run_length_counts": _run_length_counts_payload(
                    stale_run_lengths,
                    profile,
                ),
            },
            "burst": {
                "thresholds": {
                    "burst_max_interval_ms": (thresholds.burst_max_interval_ms),
                    "burst_run_length": thresholds.burst_run_length,
                },
                "interval_count": burst_interval_count,
                "burst_rate": _rate(
                    burst_interval_count,
                    len(interarrival_ms),
                    profile,
                ),
                "run_count": len(burst_run_lengths),
                "tick_count": sum(burst_run_lengths),
                "run_length_counts": _run_length_counts_payload(
                    burst_run_lengths,
                    profile,
                ),
            },
            "one_sided_movement": {
                "thresholds": {
                    "one_sided_run_length": thresholds.one_sided_run_length,
                },
                "count": one_sided_movement_count,
                "rate": _rate(
                    one_sided_movement_count,
                    len(interarrival_ms),
                    profile,
                ),
                "bid_only_count": bid_only_count,
                "ask_only_count": ask_only_count,
                "run_count": len(one_sided_run_lengths),
                "run_length_counts": _run_length_counts_payload(
                    one_sided_run_lengths,
                    profile,
                ),
            },
        }
    )
    return result


def _tick_dependence_payload(
    rows: list[_TickDynamicsRow],
    *,
    base: dict[str, JSONValue],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    spreads = [row.spread for row in rows]
    spread_changes: list[float] = []
    absolute_spread_changes: list[float] = []
    previous: _TickDynamicsRow | None = None

    for row in rows:
        if previous is None:
            previous = row
            continue
        spread_change = row.spread - previous.spread
        spread_changes.append(spread_change)
        absolute_spread_changes.append(abs(spread_change))
        previous = row

    return _dependence_payload(
        base,
        profile=profile,
        acf_series={
            "spread_acf": spreads,
            "spread_change_acf": spread_changes,
            "absolute_spread_change_acf": absolute_spread_changes,
        },
    )


def _tick_stationarity_payload(
    rows: list[_TickDynamicsRow],
    *,
    base: dict[str, JSONValue],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    mids = [(row.bid + row.ask) / 2.0 for row in rows]
    returns: list[float] = []
    previous_mid: float | None = None
    for mid in mids:
        if previous_mid is not None and previous_mid > 0.0 and mid > 0.0:
            returns.append(math.log(mid / previous_mid))
        previous_mid = mid
    return _stationarity_payload(
        base,
        profile=profile,
        metric="mid_price",
        level_values=mids,
        return_values=returns,
    )


def _tick_decomposition_payload(
    rows: list[_TickDynamicsRow],
    *,
    base: dict[str, JSONValue],
    stationarity: Mapping[str, JSONValue],
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    mids = [(row.bid + row.ask) / 2.0 for row in rows]
    returns: list[float] = []
    absolute_returns_by_row: list[float | None] = []
    previous_mid: float | None = None
    for mid in mids:
        row_return: float | None = None
        if previous_mid is not None and previous_mid > 0.0 and mid > 0.0:
            row_return = math.log(mid / previous_mid)
            returns.append(row_return)
        absolute_returns_by_row.append(
            abs(row_return) if row_return is not None else None
        )
        previous_mid = mid
    return _decomposition_payload(
        base,
        profile=profile,
        target=target,
        metric="mid_price",
        timestamps=[row.timestamp_utc_ms for row in rows],
        level_values=mids,
        return_values=returns,
        absolute_returns_by_row=absolute_returns_by_row,
        stationarity=stationarity,
    )


def _decomposition_payload(
    base: Mapping[str, JSONValue],
    *,
    profile: HistDataFingerprintProfile,
    target: QualityTarget,
    metric: str,
    timestamps: list[int],
    level_values: Iterable[float],
    return_values: Iterable[float],
    absolute_returns_by_row: list[float | None],
    stationarity: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    levels = _finite_values(level_values)
    returns = _finite_values(return_values)
    trend = _decomposition_trend_payload(levels, profile)
    residual = _decomposition_residual_payload(levels, profile)
    seasonality = _decomposition_seasonality_payload(
        timestamps,
        levels,
        absolute_returns_by_row,
        target=target,
        profile=profile,
    )
    smoothing_windows, computed_window_count, skipped_reason_counts = (
        _decomposition_smoothing_windows_payload(levels, returns, profile)
    )
    structural_break = _decomposition_structural_break_payload(levels, profile)
    stationarity_basis = _decomposition_stationarity_basis(stationarity)
    skipped_window_count = sum(skipped_reason_counts.values())
    limitations = _decomposition_limitations(
        base,
        stationarity_basis=stationarity_basis,
        level_count=len(levels),
        return_count=len(returns),
        computed_window_count=computed_window_count,
        skipped_window_count=skipped_window_count,
    )
    decomposition_status = _decomposition_status(
        base,
        stationarity_basis=stationarity_basis,
        level_count=len(levels),
        return_count=len(returns),
        limitations=limitations,
    )

    result = dict(base)
    result.update(
        {
            "schema_version": (
                TIME_SERIES_FINGERPRINT_DECOMPOSITION_SCHEMA_VERSION
            ),
            "decomposition_status": decomposition_status,
            "calculation_basis": "observed_sequence",
            "metric": metric,
            "sample_counts": {
                "level": len(levels),
                "return": len(returns),
            },
            "windows": [int(window) for window in profile.rolling_windows],
            "rounding_digits": int(profile.rounding_digits),
            "stationarity_basis": stationarity_basis,
            "trend_proxy": trend,
            "seasonality_proxy": seasonality,
            "residual_proxy": residual,
            "smoothing_windows": smoothing_windows,
            "computed_window_count": computed_window_count,
            "skipped_window_count": skipped_window_count,
            "skipped_window_reason_counts": _counter_payload(
                skipped_reason_counts
            ),
            "structural_break_proxy": structural_break,
            "limitations": [value for value in limitations],
        }
    )
    if decomposition_status in {"limited", "unavailable"}:
        result["reason"] = _decomposition_status_reason(
            limitations,
            stationarity_basis=stationarity_basis,
        )
    result["training_projection"] = decomposition_training_projection(result)
    return result


def _decomposition_stationarity_basis(
    stationarity: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    if not stationarity:
        return {
            "status": "unavailable",
            "reason": "stationarity_unavailable",
            "stationarity_status": "unavailable",
            "calculation_basis": "unknown",
            "computed_window_count": 0,
            "skipped_window_count": 0,
            "skipped_window_reason_counts": {},
            "zero_variance_metrics": [],
            "recommended_transforms": [],
            "limitations": ["stationarity_unavailable"],
        }
    return {
        "status": _stationarity_section_status(stationarity),
        "reason": _optional_summary_key(stationarity.get("reason")),
        "stationarity_status": _summary_key(
            stationarity.get("stationarity_status")
        ),
        "calculation_basis": _summary_key(
            stationarity.get("calculation_basis")
        ),
        "computed_window_count": _int_payload(
            stationarity.get("computed_window_count")
        ),
        "skipped_window_count": _int_payload(
            stationarity.get("skipped_window_count")
        ),
        "skipped_window_reason_counts": _counter_payload(
            _counter_from_mapping(
                _payload_mapping(
                    stationarity.get("skipped_window_reason_counts")
                )
            )
        ),
        "zero_variance_metrics": _string_list(
            stationarity.get("zero_variance_metrics")
        ),
        "recommended_transforms": _string_list(
            stationarity.get("recommended_transforms")
        ),
        "limitations": _string_list(stationarity.get("limitations")),
    }


def _decomposition_trend_payload(
    levels: list[float],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    components = _linear_trend_components(levels)
    if components is None:
        return {
            "status": "skipped",
            "reason": "insufficient_sample_count",
            "sample_count": len(levels),
            "required_sample_count": 2,
        }
    slope, intercept, fitted_values, _residuals, trend_strength = components
    first_fitted = fitted_values[0]
    last_fitted = fitted_values[-1]
    return {
        "status": "computed",
        "index_basis": "observation_index",
        "sample_count": len(levels),
        "slope_per_observation": _rounded(slope, profile),
        "intercept": _rounded(intercept, profile),
        "fitted_first": _rounded(first_fitted, profile),
        "fitted_last": _rounded(last_fitted, profile),
        "direction": _trend_direction(slope, profile),
        "trend_strength": _rounded(trend_strength, profile),
        "fitted_change_first_to_last": _stationarity_change_payload(
            first_fitted,
            last_fitted,
            profile,
        ),
    }


def _decomposition_residual_payload(
    levels: list[float],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    components = _linear_trend_components(levels)
    if components is None:
        return {
            "status": "skipped",
            "reason": "insufficient_sample_count",
            "sample_count": len(levels),
            "required_sample_count": 2,
        }
    _slope, _intercept, _fitted_values, residuals, _trend_strength = components
    level_variance = _population_variance(levels)
    residual_variance = _population_variance(residuals)
    return {
        "status": "computed",
        "basis": "linear_trend_residual",
        "sample_count": len(levels),
        "level_variance": _rounded(level_variance, profile),
        "residual_variance": _rounded(residual_variance, profile),
        "residual_to_level_variance_ratio": (
            _rounded(residual_variance / level_variance, profile)
            if level_variance > 0.0
            else None
        ),
        "residual": _numeric_summary(residuals, profile),
        "absolute_residual": _numeric_summary(
            [abs(value) for value in residuals],
            profile,
        ),
    }


def _linear_trend_components(
    values: list[float],
) -> tuple[float, float, list[float], list[float], float] | None:
    sample_count = len(values)
    if sample_count < 2:
        return None
    x_mean = (sample_count - 1) / 2.0
    y_mean = _mean(values)
    denominator = sum((index - x_mean) ** 2 for index in range(sample_count))
    if denominator <= 0.0:
        return None
    slope = (
        sum(
            (index - x_mean) * (value - y_mean)
            for index, value in enumerate(values)
        )
        / denominator
    )
    intercept = y_mean - slope * x_mean
    fitted_values = [intercept + slope * index for index in range(sample_count)]
    residuals = [
        value - fitted
        for value, fitted in zip(values, fitted_values, strict=True)
    ]
    total_variance = _population_variance(values)
    residual_variance = _population_variance(residuals)
    if total_variance > 0.0:
        trend_strength = max(
            0.0, min(1.0, 1.0 - residual_variance / total_variance)
        )
    else:
        trend_strength = 1.0 if residual_variance <= 0.0 else 0.0
    return slope, intercept, fitted_values, residuals, trend_strength


def _trend_direction(
    slope: float,
    profile: HistDataFingerprintProfile,
) -> str:
    tolerance = 10 ** (-max(0, int(profile.rounding_digits)))
    if slope > tolerance:
        return "increasing"
    if slope < -tolerance:
        return "decreasing"
    return "flat"


def _decomposition_seasonality_payload(
    timestamps: list[int],
    levels: list[float],
    absolute_returns_by_row: list[float | None],
    *,
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    by_hour: dict[str, list[float]] = {}
    returns_by_hour: dict[str, list[float]] = {}
    by_weekday: dict[str, list[float]] = {}
    returns_by_weekday: dict[str, list[float]] = {}
    by_session: dict[str, list[float]] = {}
    returns_by_session: dict[str, list[float]] = {}
    usable_count = min(
        len(timestamps), len(levels), len(absolute_returns_by_row)
    )

    for index in range(usable_count):
        classification = classify_histdata_timestamp(
            timestamps[index],
            calendar_profile=profile.calendar_profile,
            asset_class=_target_asset_class(target),
        )
        source_datetime = classification.source_datetime
        hour_key = f"{source_datetime.hour:02d}"
        weekday_key = SOURCE_WEEKDAY_NAMES[source_datetime.weekday()]
        active_sessions = tuple(classification.active_sessions) or (
            (SESSION_MARKET_CLOSED,)
            if classification.session_state == SESSION_STATE_WEEKEND_CLOSURE
            else (SESSION_NO_ACTIVE_WINDOW,)
        )
        _record_decomposition_bucket(
            by_hour,
            returns_by_hour,
            hour_key,
            level=levels[index],
            absolute_return=absolute_returns_by_row[index],
        )
        _record_decomposition_bucket(
            by_weekday,
            returns_by_weekday,
            weekday_key,
            level=levels[index],
            absolute_return=absolute_returns_by_row[index],
        )
        for session in active_sessions:
            _record_decomposition_bucket(
                by_session,
                returns_by_session,
                str(session),
                level=levels[index],
                absolute_return=absolute_returns_by_row[index],
            )

    status = "computed" if usable_count else "skipped"
    result: dict[str, JSONValue] = {
        "status": status,
        "sample_count": usable_count,
        "grouped_by": ["source_hour", "source_weekday", "active_session"],
        "by_source_hour": _decomposition_bucket_group_payload(
            by_hour,
            returns_by_hour,
            profile,
        ),
        "by_source_weekday": _decomposition_bucket_group_payload(
            by_weekday,
            returns_by_weekday,
            profile,
        ),
        "by_active_session": _decomposition_bucket_group_payload(
            by_session,
            returns_by_session,
            profile,
        ),
    }
    if status == "skipped":
        result["reason"] = "insufficient_sample_count"
    return result


def _record_decomposition_bucket(
    level_buckets: dict[str, list[float]],
    return_buckets: dict[str, list[float]],
    bucket: str,
    *,
    level: float,
    absolute_return: float | None,
) -> None:
    level_buckets.setdefault(bucket, []).append(level)
    if absolute_return is not None:
        return_buckets.setdefault(bucket, []).append(absolute_return)


def _decomposition_bucket_group_payload(
    level_buckets: Mapping[str, list[float]],
    return_buckets: Mapping[str, list[float]],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    limit = max(1, int(profile.histogram_bins))
    bucket_names = sorted(level_buckets)
    included_names = bucket_names[:limit]
    buckets: dict[str, JSONValue] = {}
    level_means: list[float] = []
    return_means: list[float] = []
    for bucket in included_names:
        bucket_levels = _finite_values(level_buckets.get(bucket, ()))
        bucket_returns = _finite_values(return_buckets.get(bucket, ()))
        level_mean = _mean(bucket_levels) if bucket_levels else None
        return_mean = _mean(bucket_returns) if bucket_returns else None
        if level_mean is not None:
            level_means.append(level_mean)
        if return_mean is not None:
            return_means.append(return_mean)
        buckets[bucket] = {
            "level_count": len(bucket_levels),
            "level_mean": (
                _rounded(level_mean, profile)
                if level_mean is not None
                else None
            ),
            "absolute_return_count": len(bucket_returns),
            "absolute_return_mean": (
                _rounded(return_mean, profile)
                if return_mean is not None
                else None
            ),
        }
    dominant_bucket = None
    if bucket_names:
        dominant_bucket = max(
            bucket_names,
            key=lambda item: (len(level_buckets.get(item, ())), item),
        )
    return {
        "bucket_count": len(bucket_names),
        "included_bucket_count": len(included_names),
        "omitted_bucket_count": max(0, len(bucket_names) - len(included_names)),
        "truncated": len(included_names) < len(bucket_names),
        "dominant_bucket": dominant_bucket,
        "buckets": buckets,
        "level_mean_dispersion": _numeric_summary(level_means, profile),
        "absolute_return_mean_dispersion": _numeric_summary(
            return_means,
            profile,
        ),
    }


def _decomposition_smoothing_windows_payload(
    levels: list[float],
    returns: list[float],
    profile: HistDataFingerprintProfile,
) -> tuple[dict[str, JSONValue], int, Counter[str]]:
    windows: dict[str, JSONValue] = {}
    skipped_reason_counts: Counter[str] = Counter()
    computed_window_count = 0
    for window in profile.rolling_windows:
        window_payload = _decomposition_smoothing_window_payload(
            levels,
            returns,
            window=int(window),
            profile=profile,
        )
        windows[str(window)] = window_payload
        if _summary_key(window_payload.get("status")) == "computed":
            computed_window_count += 1
        else:
            reason = _optional_summary_key(window_payload.get("reason"))
            if reason:
                skipped_reason_counts[reason] += 1
    return windows, computed_window_count, skipped_reason_counts


def _decomposition_smoothing_window_payload(
    levels: list[float],
    returns: list[float],
    *,
    window: int,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    sample_counts: dict[str, JSONValue] = {
        "level": len(levels),
        "return": len(returns),
    }
    required_sample_count = max(2, window * 2)
    if (
        window <= 0
        or len(levels) < required_sample_count
        or len(returns) < required_sample_count
    ):
        return {
            "status": "skipped",
            "reason": "insufficient_sample_count",
            "window": window,
            "sample_counts": sample_counts,
            "required_sample_count": required_sample_count,
        }
    level_means = _rolling_stat_values(levels, window, statistic="mean")
    level_variances = _rolling_stat_values(
        levels,
        window,
        statistic="variance",
    )
    absolute_return_means = _rolling_stat_values(
        [abs(value) for value in returns],
        window,
        statistic="mean",
    )
    return {
        "status": "computed",
        "window": window,
        "sample_counts": sample_counts,
        "required_sample_count": required_sample_count,
        "level_smoothed_mean": _numeric_summary(level_means, profile),
        "level_smoothed_variance": _numeric_summary(level_variances, profile),
        "level_smoothed_mean_drift": _stationarity_change_payload(
            level_means[0],
            level_means[-1],
            profile,
        ),
        "absolute_return_smoothed_mean": _numeric_summary(
            absolute_return_means,
            profile,
        ),
        "absolute_return_smoothed_mean_drift": _stationarity_change_payload(
            absolute_return_means[0],
            absolute_return_means[-1],
            profile,
        ),
    }


def _rolling_stat_values(
    values: list[float],
    window: int,
    *,
    statistic: str,
) -> list[float]:
    if window <= 0 or len(values) < window:
        return []
    return [
        _stationarity_statistic(values[index : index + window], statistic)
        for index in range(0, len(values) - window + 1)
    ]


def _decomposition_structural_break_payload(
    levels: list[float],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    sample_count = len(levels)
    minimum_segment_size = 2
    if sample_count < minimum_segment_size * 2:
        return {
            "status": "skipped",
            "reason": "insufficient_sample_count",
            "sample_count": sample_count,
            "required_sample_count": minimum_segment_size * 2,
            "minimum_segment_size": minimum_segment_size,
            "candidate_count": 0,
            "candidates": [],
        }
    candidates: list[dict[str, JSONValue]] = []
    for split_index in range(
        minimum_segment_size,
        sample_count - minimum_segment_size + 1,
    ):
        before = levels[:split_index]
        after = levels[split_index:]
        before_mean = _mean(before)
        after_mean = _mean(after)
        mean_shift = after_mean - before_mean
        pooled_variance = (
            _population_variance(before) + _population_variance(after)
        ) / 2.0
        if pooled_variance > 0.0:
            score = abs(mean_shift) / math.sqrt(pooled_variance)
            score_basis = "absolute_mean_shift_over_pooled_std"
        else:
            score = abs(mean_shift) * sample_count
            score_basis = "scaled_absolute_mean_shift_zero_variance"
        candidates.append(
            {
                "split_index": split_index,
                "before_count": len(before),
                "after_count": len(after),
                "before_mean": _rounded(before_mean, profile),
                "after_mean": _rounded(after_mean, profile),
                "mean_shift": _rounded(mean_shift, profile),
                "absolute_mean_shift": _rounded(abs(mean_shift), profile),
                "score": _rounded(score, profile),
                "score_basis": score_basis,
            }
        )
    ranked = sorted(
        candidates,
        key=lambda item: (
            -(_optional_float_payload(item.get("score")) or 0.0),
            _int_payload(item.get("split_index")),
        ),
    )
    limit = max(1, int(profile.histogram_bins))
    included = ranked[:limit]
    included_candidates: list[JSONValue] = [dict(item) for item in included]
    return {
        "status": "computed",
        "basis": "two_segment_mean_shift",
        "sample_count": sample_count,
        "minimum_segment_size": minimum_segment_size,
        "candidate_count": len(candidates),
        "included_candidate_count": len(included),
        "omitted_candidate_count": max(0, len(candidates) - len(included)),
        "truncated": len(included) < len(candidates),
        "strongest_candidate": (
            included_candidates[0] if included_candidates else None
        ),
        "candidates": included_candidates,
    }


def _decomposition_limitations(
    base: Mapping[str, JSONValue],
    *,
    stationarity_basis: Mapping[str, JSONValue],
    level_count: int,
    return_count: int,
    computed_window_count: int,
    skipped_window_count: int,
) -> tuple[str, ...]:
    limitations = [
        str(value) for value in _string_list(base.get("limitations"))
    ]
    if level_count < 3 or return_count < 1:
        limitations.append("insufficient_sample_count")
    stationarity_status = _summary_key(stationarity_basis.get("status"))
    if stationarity_status == "unavailable":
        limitations.append("stationarity_unavailable")
    elif stationarity_status == "limited":
        limitations.append("stationarity_limited")
    if _string_list(stationarity_basis.get("zero_variance_metrics")):
        limitations.append("zero_variance")
    if skipped_window_count > 0 or _int_payload(
        stationarity_basis.get("skipped_window_count")
    ):
        limitations.append("skipped_rolling_windows")
    if computed_window_count <= 0:
        limitations.append("insufficient_sample_count")
    return _ordered_unique(limitations)


def _decomposition_status(
    base: Mapping[str, JSONValue],
    *,
    stationarity_basis: Mapping[str, JSONValue],
    level_count: int,
    return_count: int,
    limitations: tuple[str, ...],
) -> str:
    if _summary_key(base.get("sequence_status")) == "unavailable":
        return "unavailable"
    if level_count < 3 or return_count < 1:
        return "unavailable"
    if _summary_key(stationarity_basis.get("status")) == "unavailable":
        return "unavailable"
    if limitations:
        return "limited"
    return "ok"


def _decomposition_status_reason(
    limitations: tuple[str, ...],
    *,
    stationarity_basis: Mapping[str, JSONValue],
) -> str:
    if limitations:
        return _summary_key(limitations[0])
    reason = _optional_summary_key(stationarity_basis.get("reason"))
    if reason:
        return reason
    return "limited"


def decomposition_training_projection(
    decomposition: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    """Return flat period-grain decomposition scalars for training rows."""
    trend = _payload_mapping(decomposition.get("trend_proxy"))
    residual = _payload_mapping(decomposition.get("residual_proxy"))
    structural = _payload_mapping(decomposition.get("structural_break_proxy"))
    strongest = _payload_mapping(structural.get("strongest_candidate"))
    stationarity = _payload_mapping(decomposition.get("stationarity_basis"))
    status = _summary_key(decomposition.get("decomposition_status"))
    direction = _summary_key(trend.get("direction"))
    stationarity_status = _summary_key(stationarity.get("status"))
    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_DECOMPOSITION_TRAINING_PROJECTION_SCHEMA_VERSION
        ),
        "grain": "period",
        "identity_fields": ["series_id", "period", "row_id"],
        "values": {
            "decomposition_status_code": {
                "unavailable": 1,
                "limited": 2,
                "ok": 3,
            }.get(status, 0),
            "decomposition_training_ready": status in {"limited", "ok"},
            "decomposition_trend_direction_code": {
                "decreasing": -1,
                "flat": 0,
                "increasing": 1,
            }.get(direction, 0),
            "decomposition_trend_slope": trend.get("slope_per_observation"),
            "decomposition_trend_strength": trend.get("trend_strength"),
            "decomposition_residual_variance_ratio": residual.get(
                "residual_to_level_variance_ratio"
            ),
            "decomposition_computed_window_count": _int_payload(
                decomposition.get("computed_window_count")
            ),
            "decomposition_structural_break_candidate_count": _int_payload(
                structural.get("candidate_count")
            ),
            "decomposition_structural_break_split_index": (
                _int_payload(strongest.get("split_index"))
                if strongest
                else None
            ),
            "decomposition_structural_break_score": strongest.get("score"),
            "decomposition_stationarity_status_code": {
                "unavailable": 1,
                "limited": 2,
                "valid": 3,
            }.get(stationarity_status, 0),
        },
    }


def project_decomposition_onto_training_frame(
    frame: Any,
    decomposition: Mapping[str, JSONValue],
) -> Any:
    """Project period-grain decomposition scalars onto enriched tick rows."""
    required = {"series_id", "period", "row_id"}
    columns = set(getattr(frame, "columns", ()))
    missing = sorted(required - columns)
    if missing:
        raise ValueError(
            "decomposition training projection requires enriched ASCII tick "
            f"identity columns: {', '.join(missing)}"
        )
    import polars as pl

    projection = decomposition_training_projection(decomposition)
    values = _payload_mapping(projection.get("values"))
    expressions = [pl.lit(value).alias(name) for name, value in values.items()]
    return frame.with_columns(expressions)


def _stationarity_payload(
    base: Mapping[str, JSONValue],
    *,
    profile: HistDataFingerprintProfile,
    metric: str,
    level_values: Iterable[float],
    return_values: Iterable[float],
) -> dict[str, JSONValue]:
    levels = _finite_values(level_values)
    returns = _finite_values(return_values)
    rolling_windows: dict[str, JSONValue] = {}
    skipped_window_reason_counts: Counter[str] = Counter()
    computed_window_count = 0
    for window in profile.rolling_windows:
        window_payload = _stationarity_window_payload(
            levels,
            returns,
            window=int(window),
            profile=profile,
        )
        rolling_windows[str(window)] = window_payload
        if _summary_key(window_payload.get("status")) == "computed":
            computed_window_count += 1
        else:
            reason = _optional_summary_key(window_payload.get("reason"))
            if reason:
                skipped_window_reason_counts[reason] += 1

    distribution_shift = _stationarity_distribution_shift_payload(
        levels,
        returns,
        profile=profile,
    )
    zero_variance_metrics = _stationarity_zero_variance_metrics(
        levels,
        returns,
    )
    skipped_window_count = sum(skipped_window_reason_counts.values())
    stationarity_status = _stationarity_status(
        base,
        level_count=len(levels),
        return_count=len(returns),
        skipped_window_count=skipped_window_count,
        distribution_shift=distribution_shift,
    )
    limitations = _stationarity_limitations(
        base,
        level_count=len(levels),
        return_count=len(returns),
        skipped_window_count=skipped_window_count,
        distribution_shift=distribution_shift,
        zero_variance_metrics=zero_variance_metrics,
    )
    result = dict(base)
    result.update(
        {
            "schema_version": (
                TIME_SERIES_FINGERPRINT_STATIONARITY_SCHEMA_VERSION
            ),
            "stationarity_status": stationarity_status,
            "calculation_basis": "observed_sequence",
            "metric": metric,
            "sample_counts": {
                "level": len(levels),
                "return": len(returns),
            },
            "windows": [int(window) for window in profile.rolling_windows],
            "rounding_digits": int(profile.rounding_digits),
            "rolling_windows": rolling_windows,
            "computed_window_count": computed_window_count,
            "skipped_window_count": skipped_window_count,
            "skipped_window_reason_counts": _counter_payload(
                skipped_window_reason_counts
            ),
            "first_middle_last_distribution_shift": distribution_shift,
            "zero_variance_metrics": [value for value in zero_variance_metrics],
            "recommended_transforms": _stationarity_recommended_transforms(
                levels,
                returns,
                rolling_windows=rolling_windows,
                distribution_shift=distribution_shift,
            ),
            "limitations": [value for value in limitations],
        }
    )
    if stationarity_status in {"limited", "unavailable"}:
        result["reason"] = _stationarity_status_reason(
            limitations,
            skipped_window_count=skipped_window_count,
            distribution_shift=distribution_shift,
        )
    return result


def _stationarity_window_payload(
    levels: list[float],
    returns: list[float],
    *,
    window: int,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    sample_counts: dict[str, JSONValue] = {
        "level": len(levels),
        "return": len(returns),
    }
    required_sample_count = max(2, window * 2)
    if window <= 0:
        return {
            "status": "skipped",
            "reason": "insufficient_sample_count",
            "window": window,
            "sample_counts": sample_counts,
            "required_sample_count": required_sample_count,
        }
    if (
        len(levels) < required_sample_count
        or len(returns) < required_sample_count
    ):
        return {
            "status": "skipped",
            "reason": "insufficient_sample_count",
            "window": window,
            "sample_counts": sample_counts,
            "required_sample_count": required_sample_count,
        }
    return {
        "status": "computed",
        "window": window,
        "sample_counts": sample_counts,
        "required_sample_count": required_sample_count,
        "level_rolling_mean_drift": _stationarity_stat_drift_payload(
            levels,
            window=window,
            statistic="mean",
            profile=profile,
        ),
        "level_rolling_variance_drift": _stationarity_stat_drift_payload(
            levels,
            window=window,
            statistic="variance",
            profile=profile,
        ),
        "return_rolling_mean_drift": _stationarity_stat_drift_payload(
            returns,
            window=window,
            statistic="mean",
            profile=profile,
        ),
        "return_rolling_variance_drift": _stationarity_stat_drift_payload(
            returns,
            window=window,
            statistic="variance",
            profile=profile,
        ),
    }


def _stationarity_stat_drift_payload(
    values: list[float],
    *,
    window: int,
    statistic: str,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    first_window = values[:window]
    last_window = values[-window:]
    first_value = _stationarity_statistic(first_window, statistic)
    last_value = _stationarity_statistic(last_window, statistic)
    payload = _stationarity_change_payload(first_value, last_value, profile)
    payload.update(
        {
            "statistic": statistic,
            "window": window,
            "sample_count": len(values),
        }
    )
    return payload


def _stationarity_distribution_shift_payload(
    levels: list[float],
    returns: list[float],
    *,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    level_shift = _stationarity_segment_shift_payload(levels, profile)
    return_shift = _stationarity_segment_shift_payload(returns, profile)
    status = (
        "computed"
        if (
            _summary_key(level_shift.get("status")) == "computed"
            or _summary_key(return_shift.get("status")) == "computed"
        )
        else "skipped"
    )
    result: dict[str, JSONValue] = {
        "status": status,
        "level": level_shift,
        "return": return_shift,
    }
    if status == "skipped":
        result["reason"] = "insufficient_sample_count"
    return result


def _stationarity_segment_shift_payload(
    values: list[float],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    sample_count = len(values)
    if sample_count < 3:
        return {
            "status": "skipped",
            "reason": "insufficient_sample_count",
            "sample_count": sample_count,
            "required_sample_count": 3,
        }
    segment_size = max(1, sample_count // 3)
    middle_start = max(0, (sample_count - segment_size) // 2)
    first_values = values[:segment_size]
    middle_values = values[middle_start : middle_start + segment_size]
    last_values = values[-segment_size:]
    first_stats = _stationarity_segment_stats(first_values, profile)
    middle_stats = _stationarity_segment_stats(middle_values, profile)
    last_stats = _stationarity_segment_stats(last_values, profile)
    return {
        "status": "computed",
        "sample_count": sample_count,
        "segment_size": segment_size,
        "first": first_stats,
        "middle": middle_stats,
        "last": last_stats,
        "mean_shift_first_to_last": _stationarity_change_payload(
            _optional_float_payload(first_stats.get("mean")) or 0.0,
            _optional_float_payload(last_stats.get("mean")) or 0.0,
            profile,
        ),
        "median_shift_first_to_last": _stationarity_change_payload(
            _optional_float_payload(first_stats.get("median")) or 0.0,
            _optional_float_payload(last_stats.get("median")) or 0.0,
            profile,
        ),
        "variance_shift_first_to_last": _stationarity_change_payload(
            _optional_float_payload(first_stats.get("variance")) or 0.0,
            _optional_float_payload(last_stats.get("variance")) or 0.0,
            profile,
        ),
    }


def _stationarity_segment_stats(
    values: list[float],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    sorted_values = sorted(values)
    return {
        "count": len(sorted_values),
        "min": _rounded(sorted_values[0], profile),
        "max": _rounded(sorted_values[-1], profile),
        "mean": _rounded(_mean(sorted_values), profile),
        "median": _rounded(_quantile(sorted_values, 0.5), profile),
        "variance": _rounded(_population_variance(sorted_values), profile),
    }


def _stationarity_change_payload(
    first_value: float,
    last_value: float,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    signed_change = last_value - first_value
    denominator = abs(first_value)
    return {
        "first": _rounded(first_value, profile),
        "last": _rounded(last_value, profile),
        "signed_change": _rounded(signed_change, profile),
        "absolute_change": _rounded(abs(signed_change), profile),
        "relative_change": (
            _rounded(abs(signed_change) / denominator, profile)
            if denominator > 0.0
            else None
        ),
    }


def _stationarity_statistic(
    values: list[float],
    statistic: str,
) -> float:
    if statistic == "variance":
        return _population_variance(values)
    return _mean(values)


def _stationarity_status(
    base: Mapping[str, JSONValue],
    *,
    level_count: int,
    return_count: int,
    skipped_window_count: int,
    distribution_shift: Mapping[str, JSONValue],
) -> str:
    if _summary_key(base.get("sequence_status")) == "unavailable":
        return "unavailable"
    if level_count < 3 or return_count < 1:
        return "unavailable"
    if (
        _summary_key(base.get("sequence_status")) == "limited"
        or skipped_window_count > 0
        or _summary_key(distribution_shift.get("status")) != "computed"
    ):
        return "limited"
    return "ok"


def _stationarity_limitations(
    base: Mapping[str, JSONValue],
    *,
    level_count: int,
    return_count: int,
    skipped_window_count: int,
    distribution_shift: Mapping[str, JSONValue],
    zero_variance_metrics: tuple[str, ...],
) -> tuple[str, ...]:
    limitations = [
        str(value) for value in _string_list(base.get("limitations"))
    ]
    if level_count < 3 or return_count < 1:
        limitations.append("insufficient_sample_count")
    if skipped_window_count > 0:
        limitations.append("skipped_rolling_windows")
    if _summary_key(distribution_shift.get("status")) != "computed":
        limitations.append("insufficient_sample_count")
    if zero_variance_metrics:
        limitations.append("zero_variance")
    return _ordered_unique(limitations)


def _stationarity_status_reason(
    limitations: tuple[str, ...],
    *,
    skipped_window_count: int,
    distribution_shift: Mapping[str, JSONValue],
) -> str:
    if limitations:
        return _summary_key(limitations[0])
    if skipped_window_count > 0:
        return "skipped_rolling_windows"
    if _summary_key(distribution_shift.get("status")) != "computed":
        return "insufficient_sample_count"
    return "limited"


def _stationarity_zero_variance_metrics(
    levels: list[float],
    returns: list[float],
) -> tuple[str, ...]:
    metrics: list[str] = []
    if len(levels) >= 2 and _population_variance(levels) <= 0.0:
        metrics.append("level")
    if len(returns) >= 2 and _population_variance(returns) <= 0.0:
        metrics.append("return")
    return tuple(metrics)


def _stationarity_recommended_transforms(
    levels: list[float],
    returns: list[float],
    *,
    rolling_windows: Mapping[str, JSONValue],
    distribution_shift: Mapping[str, JSONValue],
) -> list[JSONValue]:
    transforms: list[str] = []
    if returns and all(value > 0.0 for value in levels):
        transforms.append("log_return")
    if _stationarity_has_level_shift(
        rolling_windows=rolling_windows,
        distribution_shift=distribution_shift,
    ):
        transforms.append("differencing")
    if returns:
        transforms.append("session_conditioning")
    return [value for value in _ordered_unique(transforms)]


def _stationarity_has_level_shift(
    *,
    rolling_windows: Mapping[str, JSONValue],
    distribution_shift: Mapping[str, JSONValue],
) -> bool:
    level_shift = _payload_mapping(distribution_shift.get("level"))
    for key in (
        "mean_shift_first_to_last",
        "median_shift_first_to_last",
    ):
        if _stationarity_change_is_nonzero(
            _payload_mapping(level_shift.get(key))
        ):
            return True
    for window_payload in rolling_windows.values():
        window = _payload_mapping(window_payload)
        if _summary_key(window.get("status")) != "computed":
            continue
        if _stationarity_change_is_nonzero(
            _payload_mapping(window.get("level_rolling_mean_drift"))
        ):
            return True
    return False


def _stationarity_change_is_nonzero(
    change_payload: Mapping[str, JSONValue],
) -> bool:
    return (
        _optional_float_payload(change_payload.get("absolute_change")) or 0.0
    ) > 0.0


def _finite_values(values: Iterable[float]) -> list[float]:
    return [float(value) for value in values if _is_finite(float(value))]


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _population_variance(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = _mean(values)
    return sum((value - mean) * (value - mean) for value in values) / len(
        values
    )


def _dependence_payload(
    base: Mapping[str, JSONValue],
    *,
    profile: HistDataFingerprintProfile,
    acf_series: Mapping[str, Iterable[float]],
) -> dict[str, JSONValue]:
    series_payloads: dict[str, JSONValue] = {
        name: _acf_series_payload(values, profile)
        for name, values in acf_series.items()
    }
    computed_lag_count = sum(
        _int_payload(_payload_mapping(payload).get("computed_lag_count"))
        for payload in series_payloads.values()
    )
    skipped_lag_count = sum(
        _int_payload(_payload_mapping(payload).get("skipped_lag_count"))
        for payload in series_payloads.values()
    )
    dependence_status = _dependence_status(
        base,
        computed_lag_count=computed_lag_count,
        skipped_lag_count=skipped_lag_count,
    )
    result = dict(base)
    result.update(
        {
            "schema_version": TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION,
            "acf_basis": "observed_sequence",
            "lags": [lag for lag in profile.lags],
            "dependence_status": dependence_status,
            "computed_lag_count": computed_lag_count,
            "skipped_lag_count": skipped_lag_count,
            **series_payloads,
        }
    )
    if dependence_status in {"limited", "unavailable"}:
        result["reason"] = _dependence_status_reason(
            base,
            computed_lag_count=computed_lag_count,
            skipped_lag_count=skipped_lag_count,
        )
    return result


def _dependence_status(
    base: Mapping[str, JSONValue],
    *,
    computed_lag_count: int,
    skipped_lag_count: int,
) -> str:
    sequence_status = _summary_key(base.get("sequence_status"))
    if sequence_status == "unavailable":
        return "unavailable"
    if computed_lag_count <= 0:
        return "limited"
    if sequence_status == "limited" or skipped_lag_count > 0:
        return "limited"
    return "ok"


def _dependence_status_reason(
    base: Mapping[str, JSONValue],
    *,
    computed_lag_count: int,
    skipped_lag_count: int,
) -> str:
    limitations = _string_list(base.get("limitations"))
    if limitations:
        return _summary_key(limitations[0])
    if computed_lag_count <= 0:
        return "no_computable_lags"
    if skipped_lag_count > 0:
        return "skipped_lags"
    return "limited"


def _acf_series_payload(
    values: Iterable[float],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    finite_values = [value for value in values if _is_finite(value)]
    lag_acf: dict[str, JSONValue] = {}
    skipped_lags: dict[str, JSONValue] = {}
    for lag in profile.lags:
        lag_key = str(lag)
        if len(finite_values) <= lag:
            skipped_lags[lag_key] = {
                "reason": "insufficient_sample_count",
                "sample_count": len(finite_values),
                "required_sample_count": lag + 1,
            }
            continue
        acf = _autocorrelation(finite_values, lag)
        if acf is None:
            skipped_lags[lag_key] = {
                "reason": "zero_variance",
                "sample_count": len(finite_values),
            }
            continue
        lag_acf[lag_key] = _rounded(acf, profile)
    return {
        "sample_count": len(finite_values),
        "lag_acf": lag_acf,
        "computed_lag_count": len(lag_acf),
        "skipped_lags": skipped_lags,
        "skipped_lag_count": len(skipped_lags),
    }


def _autocorrelation(values: list[float], lag: int) -> float | None:
    if lag <= 0 or len(values) <= lag:
        return None
    mean = sum(values) / len(values)
    centered = [value - mean for value in values]
    denominator = sum(value * value for value in centered)
    if denominator <= 0.0:
        return None
    numerator = sum(
        centered[index] * centered[index - lag]
        for index in range(lag, len(centered))
    )
    return numerator / denominator


def _spread_jump_threshold(
    spreads: Iterable[float],
) -> float | None:
    nonnegative_spreads = sorted(
        spread for spread in spreads if _is_finite(spread) and spread >= 0.0
    )
    if not nonnegative_spreads:
        return None
    spread_thresholds = DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS
    jump_multiplier = float(spread_thresholds.jump_spread_multiplier)
    minimum_spread_jump = float(spread_thresholds.minimum_spread_jump)
    return float(
        max(
            _quantile(nonnegative_spreads, 0.5) * jump_multiplier,
            minimum_spread_jump,
        )
    )


def _one_sided_movement_kind(
    previous: _TickDynamicsRow,
    current: _TickDynamicsRow,
) -> str:
    bid_changed = current.bid != previous.bid
    ask_changed = current.ask != previous.ask
    if bid_changed and not ask_changed:
        return "bid_only"
    if ask_changed and not bid_changed:
        return "ask_only"
    return ""


def _append_minimum_run(
    lengths: list[int],
    length: int,
    *,
    minimum: int,
) -> None:
    if length >= minimum:
        lengths.append(length)


def _run_length_counts_payload(
    lengths: Iterable[int],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    counter = Counter(length for length in lengths if length > 0)
    limit = max(1, int(profile.histogram_bins))
    items = sorted(counter.items())
    included = items[:limit]
    payload: dict[str, JSONValue] = {
        str(length): count for length, count in included
    }
    overflow = sum(count for _, count in items[limit:])
    if overflow:
        payload["__other__"] = overflow
    return payload


def _add_conditional_distribution_payload(
    payload: dict[str, JSONValue],
    *,
    target: QualityTarget,
    frame: Any | None,
    text: str | None,
    profile: HistDataFingerprintProfile,
) -> None:
    conditional = _conditional_distributions(
        target,
        frame=frame,
        text=text,
        profile=profile,
    )
    if conditional:
        payload["conditional_distributions"] = conditional


def _conditional_distributions(
    target: QualityTarget,
    *,
    frame: Any | None,
    text: str | None,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    if target.timeframe != TICK:
        return {}

    by_active_session: dict[str, list[float]] = {}
    by_special_tag: dict[str, list[float]] = {}
    row_count = 0
    sampled_row_count = 0
    usable_row_count = 0
    invalid_row_count = 0
    sample_limit = _profile_max_rows(profile)
    basis = "none"

    if frame is not None:
        basis = "cache"
        row_count = int(getattr(frame, "height", 0) or 0)
        for row in frame.head(min(row_count, sample_limit)).iter_rows(
            named=True
        ):
            sampled_row_count += 1
            timestamp = _finite_int(row.get("datetime"))
            bid = _finite_float(row.get("bid"))
            ask = _finite_float(row.get("ask"))
            if timestamp is None or bid is None or ask is None:
                invalid_row_count += 1
                continue
            classification = classify_histdata_timestamp(
                timestamp,
                calendar_profile=profile.calendar_profile,
                asset_class=_target_asset_class(target),
            )
            _record_conditioned_spread(
                by_active_session,
                by_special_tag,
                classification=classification,
                spread=ask - bid,
            )
            usable_row_count += 1
    elif text is not None:
        basis = "text"
        reader = csv.reader(
            text.splitlines(),
            delimiter=delimiter_for_timeframe(target.timeframe),
        )
        for row in reader:
            if not row or not any(cell.strip() for cell in row):
                continue
            row_count += 1
            if sampled_row_count >= sample_limit:
                continue
            sampled_row_count += 1
            try:
                normalized = normalize_ascii_row(target.timeframe, row)
                classification = classify_histdata_source_timestamp(
                    row[0],
                    target.timeframe,
                    calendar_profile=profile.calendar_profile,
                    asset_class=_target_asset_class(target),
                )
            except ValueError:
                invalid_row_count += 1
                continue
            bid = _finite_float(normalized[1])
            ask = _finite_float(normalized[2])
            if bid is None or ask is None:
                invalid_row_count += 1
                continue
            _record_conditioned_spread(
                by_active_session,
                by_special_tag,
                classification=classification,
                spread=ask - bid,
            )
            usable_row_count += 1

    if row_count == 0:
        return {}

    return {
        "schema_version": (
            TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION
        ),
        "basis": basis,
        "metric": "tick_spread",
        "row_count": row_count,
        "sampled_row_count": sampled_row_count,
        "usable_row_count": usable_row_count,
        "invalid_row_count": invalid_row_count,
        "truncated": row_count > sampled_row_count,
        "by_active_session": _conditioned_numeric_summary(
            by_active_session,
            profile,
        ),
        "by_special_tag": _conditioned_numeric_summary(
            by_special_tag,
            profile,
        ),
    }


def _record_conditioned_spread(
    by_active_session: dict[str, list[float]],
    by_special_tag: dict[str, list[float]],
    *,
    classification: Any,
    spread: float,
) -> None:
    active_sessions = tuple(classification.active_sessions) or (
        (SESSION_MARKET_CLOSED,)
        if classification.session_state == SESSION_STATE_WEEKEND_CLOSURE
        else (SESSION_NO_ACTIVE_WINDOW,)
    )
    for session in active_sessions:
        by_active_session.setdefault(str(session), []).append(spread)
    for tag in tuple(classification.special_tags):
        by_special_tag.setdefault(str(tag), []).append(spread)


def _conditioned_numeric_summary(
    values_by_bucket: Mapping[str, list[float]],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    return {
        key: {"spread": _numeric_summary(values_by_bucket[key], profile)}
        for key in sorted(values_by_bucket)
    }


def _finite_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _distribution_from_frame(
    frame: Any,
    *,
    timeframe: str,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    if timeframe == TICK:
        return _tick_distribution_from_frame(frame, profile)
    return {}


def _distribution_from_text(
    text: str,
    *,
    timeframe: str,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    if timeframe == TICK:
        return _tick_distribution_from_text(text, profile)
    return {}


def _tick_distribution_from_frame(
    frame: Any,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    row_count = int(getattr(frame, "height", 0) or 0)
    sample_limit = min(row_count, _profile_max_rows(profile))
    usable_row_count = _frame_numeric_usable_row_count(frame, ("bid", "ask"))
    columns = _frame_sampled_valid_column_values(
        frame,
        ("bid", "ask"),
        sample_limit,
    )
    return _tick_distribution_from_column_values(
        columns["bid"],
        columns["ask"],
        row_count=row_count,
        usable_row_count=usable_row_count,
        sample_limit=sample_limit,
        profile=profile,
    )


def _tick_distribution_from_text(
    text: str,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    row_count = 0
    invalid_row_count = 0
    partial_row_count = 0
    usable_row_count = 0
    sample_limit = _profile_max_rows(profile)
    bids: list[float] = []
    asks: list[float] = []
    reader = csv.reader(
        text.splitlines(), delimiter=delimiter_for_timeframe(TICK)
    )
    expected_field_count = len(columns_for_timeframe(TICK))
    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        row_count += 1
        if len(row) != expected_field_count:
            invalid_row_count += 1
            partial_row_count += 1
            continue
        try:
            parsed = normalize_ascii_row(TICK, row)
        except (TypeError, ValueError, OverflowError):
            invalid_row_count += 1
            continue
        bid = _finite_float(parsed[1])
        ask = _finite_float(parsed[2])
        if bid is None or ask is None:
            invalid_row_count += 1
            continue
        usable_row_count += 1
        if len(bids) >= sample_limit:
            continue
        bids.append(bid)
        asks.append(ask)
    return _tick_distribution_payload(
        bids,
        asks,
        row_count=row_count,
        sampled_row_count=len(bids),
        usable_row_count=usable_row_count,
        invalid_row_count=invalid_row_count,
        partial_row_count=partial_row_count,
        truncated=usable_row_count > len(bids),
        profile=profile,
    )


def _tick_distribution_from_column_values(
    bids: list[Any],
    asks: list[Any],
    *,
    row_count: int,
    usable_row_count: int,
    sample_limit: int,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    sampled_bids: list[float] = []
    sampled_asks: list[float] = []
    sampled_invalid_row_count = 0
    sampled_count = min(sample_limit, len(bids), len(asks))
    for index in range(sampled_count):
        bid = _finite_float(bids[index])
        ask = _finite_float(asks[index])
        if bid is None or ask is None:
            sampled_invalid_row_count += 1
            continue
        sampled_bids.append(bid)
        sampled_asks.append(ask)
    return _tick_distribution_payload(
        sampled_bids,
        sampled_asks,
        row_count=row_count,
        sampled_row_count=len(sampled_bids),
        usable_row_count=usable_row_count,
        invalid_row_count=max(0, row_count - usable_row_count),
        truncated=usable_row_count > len(sampled_bids),
        profile=profile,
    )


def _tick_distribution_payload(
    bids: list[float],
    asks: list[float],
    *,
    row_count: int,
    sampled_row_count: int,
    usable_row_count: int,
    invalid_row_count: int,
    partial_row_count: int = 0,
    truncated: bool,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    spreads = [ask - bid for bid, ask in zip(bids, asks, strict=True)]
    zero_spread_count = sum(1 for spread in spreads if spread == 0.0)
    negative_spread_count = sum(1 for spread in spreads if spread < 0.0)
    return {
        "row_count": row_count,
        "sampled_row_count": sampled_row_count,
        "usable_row_count": usable_row_count,
        "invalid_row_count": invalid_row_count,
        "partial_row_count": partial_row_count,
        "truncated": truncated,
        "bid": _numeric_summary(bids, profile),
        "ask": _numeric_summary(asks, profile),
        "spread": _numeric_summary(spreads, profile),
        "zero_spread_count": zero_spread_count,
        "negative_spread_count": negative_spread_count,
        "zero_spread_rate": _rate(
            zero_spread_count, sampled_row_count, profile
        ),
        "negative_spread_rate": _rate(
            negative_spread_count,
            sampled_row_count,
            profile,
        ),
    }


def _numeric_summary(
    values: Iterable[float],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    finite_values = sorted(value for value in values if _is_finite(value))
    count = len(finite_values)
    if count == 0:
        return {
            "count": 0,
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
            "mad": None,
            "quantiles": {
                _quantile_label(quantile): None
                for quantile in profile.quantiles
            },
        }
    median = _quantile(finite_values, 0.5)
    mad = _quantile(
        sorted(abs(value - median) for value in finite_values),
        0.5,
    )
    return {
        "count": count,
        "min": _rounded(finite_values[0], profile),
        "max": _rounded(finite_values[-1], profile),
        "mean": _rounded(sum(finite_values) / count, profile),
        "median": _rounded(median, profile),
        "mad": _rounded(mad, profile),
        "quantiles": {
            _quantile_label(quantile): _rounded(
                _quantile(finite_values, quantile),
                profile,
            )
            for quantile in profile.quantiles
        },
    }


def _quantile(values: list[float], quantile: float) -> float:
    if not values:
        return math.nan
    clipped = min(max(float(quantile), 0.0), 1.0)
    index = (len(values) - 1) * clipped
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return values[lower]
    lower_value = values[lower]
    upper_value = values[upper]
    return lower_value + (upper_value - lower_value) * (index - lower)


def _quantile_label(quantile: float) -> str:
    return str(float(quantile))


def _rate(
    numerator: int,
    denominator: int,
    profile: HistDataFingerprintProfile,
) -> float | None:
    if denominator <= 0:
        return None
    return _rounded(numerator / denominator, profile)


def _rounded(
    value: float,
    profile: HistDataFingerprintProfile,
) -> float:
    digits = max(0, int(profile.rounding_digits))
    try:
        with localcontext() as context:
            context.prec = max(28, digits + 20)
            rounded_decimal = Decimal(str(float(value))).quantize(
                Decimal(1).scaleb(-digits)
            )
        rounded = float(rounded_decimal)
    except (InvalidOperation, ValueError, OverflowError):
        rounded = round(float(value), digits)
    return 0.0 if rounded == 0 else rounded


def _finite_float(value: object) -> float | None:
    try:
        normalized = float(cast(Any, value))
    except (TypeError, ValueError, OverflowError):
        return None
    if not _is_finite(normalized):
        return None
    return normalized


def _is_finite(value: float) -> bool:
    return math.isfinite(float(value))


def _frame_column_values(
    frame: Any,
    column: str,
    limit: int,
) -> list[Any]:
    try:
        series = frame.get_column(column)
    except (AttributeError, KeyError, TypeError, ValueError):
        return []
    try:
        return cast(list[Any], series.head(limit).to_list())
    except AttributeError:
        return list(series)[:limit]


def _frame_sampled_valid_column_values(
    frame: Any,
    columns: tuple[str, ...],
    limit: int,
) -> dict[str, list[Any]]:
    empty: dict[str, list[Any]] = {column: [] for column in columns}
    if not columns or limit <= 0:
        return empty
    try:
        import polars as pl
        from polars.exceptions import PolarsError
    except ImportError:
        return _frame_sampled_valid_column_values_fallback(
            frame, columns, limit
        )

    try:
        expressions = [
            pl.col(column).is_not_null() & pl.col(column).is_finite()
            for column in columns
        ]
        sample = (
            frame.select([pl.col(column) for column in columns])
            .filter(pl.all_horizontal(*expressions))
            .head(limit)
        )
        return {
            column: cast(list[Any], sample.get_column(column).to_list())
            for column in columns
        }
    except (AttributeError, TypeError, ValueError, PolarsError):
        return _frame_sampled_valid_column_values_fallback(
            frame, columns, limit
        )


def _frame_sampled_valid_column_values_fallback(
    frame: Any,
    columns: tuple[str, ...],
    limit: int,
) -> dict[str, list[Any]]:
    sampled: dict[str, list[Any]] = {column: [] for column in columns}
    row_count = int(getattr(frame, "height", 0) or 0)
    if not columns or row_count <= 0 or limit <= 0:
        return sampled
    column_values = {
        column: _frame_column_values(frame, column, row_count)
        for column in columns
    }
    scanned_count = min(
        row_count, *(len(values) for values in column_values.values())
    )
    for index in range(scanned_count):
        row_values: dict[str, Any] = {}
        for column in columns:
            value = column_values[column][index]
            if _finite_float(value) is None:
                row_values = {}
                break
            row_values[column] = value
        if not row_values:
            continue
        for column, value in row_values.items():
            sampled[column].append(value)
        if len(sampled[columns[0]]) >= limit:
            break
    return sampled


def _frame_numeric_usable_row_count(
    frame: Any,
    columns: tuple[str, ...],
) -> int:
    try:
        import polars as pl
        from polars.exceptions import PolarsError
    except ImportError:
        return _frame_numeric_usable_row_count_fallback(frame, columns)

    try:
        expressions = [
            pl.col(column).is_not_null() & pl.col(column).is_finite()
            for column in columns
        ]
        value = frame.select(
            pl.all_horizontal(*expressions).sum().alias("__usable_row_count")
        ).item()
    except (AttributeError, TypeError, ValueError, PolarsError):
        return _frame_numeric_usable_row_count_fallback(frame, columns)
    if isinstance(value, bool) or value is None:
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _frame_numeric_usable_row_count_fallback(
    frame: Any,
    columns: tuple[str, ...],
) -> int:
    row_count = int(getattr(frame, "height", 0) or 0)
    if row_count <= 0:
        return 0
    column_values = {
        column: _frame_column_values(frame, column, row_count)
        for column in columns
    }
    scanned_count = min(
        row_count, *(len(values) for values in column_values.values())
    )
    usable_count = 0
    for index in range(scanned_count):
        if all(
            _finite_float(column_values[column][index]) is not None
            for column in columns
        ):
            usable_count += 1
    return usable_count


def _profile_max_rows(profile: HistDataFingerprintProfile) -> int:
    return max(1, int(profile.max_rows))


def _decimal_places_from_text(value: str) -> int:
    normalized = value.strip().lower()
    mantissa = normalized.split("e", 1)[0]
    if "." not in mantissa:
        return 0
    return len(mantissa.split(".", 1)[1])


def _decimal_places_from_float(
    value: float,
    profile: HistDataFingerprintProfile,
) -> int:
    digits = max(0, int(profile.rounding_digits))
    text = f"{value:.{digits}f}".rstrip("0").rstrip(".")
    if "." not in text:
        return 0
    return len(text.split(".", 1)[1])


def _cache_timestamp_at(frame: Any, row_index: int) -> int | None:
    if row_index < 0:
        return None
    columns = getattr(frame, "columns", ())
    if "datetime" not in columns:
        return None
    try:
        value = frame.get_column("datetime")[row_index]
    except (IndexError, TypeError):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _duration_ms(
    start_timestamp_utc_ms: int | None,
    end_timestamp_utc_ms: int | None,
) -> int | None:
    if start_timestamp_utc_ms is None or end_timestamp_utc_ms is None:
        return None
    return end_timestamp_utc_ms - start_timestamp_utc_ms


def _read_text_payload(target: QualityTarget) -> _TextPayload:
    path = Path(target.path)
    if target.kind is QualityTargetKind.CSV:
        return _TextPayload(text=path.read_bytes().decode("utf-8"))

    with zipfile.ZipFile(path) as archive:
        members = tuple(
            name
            for name in archive.namelist()
            if not name.endswith("/") and Path(name).suffix.lower() == ".csv"
        )
        if len(members) != 1:
            raise ValueError("zip_csv_member_unavailable")
        member = members[0]
        return _TextPayload(
            text=archive.read(member).decode("utf-8"),
            source_member=member,
        )


def _unsupported_reason(target: QualityTarget) -> str:
    if target.data_format != "ascii":
        return "unsupported_data_format"
    if target.timeframe not in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES:
        return "unsupported_timeframe"
    if target.kind not in SUPPORTED_SERIES_FINGERPRINT_KINDS:
        return "unsupported_target_kind"
    return ""


def _unavailable_source(
    target: QualityTarget,
    *,
    reason: str,
    error: Exception | None = None,
) -> dict[str, JSONValue]:
    source: dict[str, JSONValue] = {
        "kind": "unavailable",
        "path": publish_safe_path(target.path),
        "reason": reason,
    }
    if error is not None:
        source["error_type"] = type(error).__name__
        source["error"] = str(error)[:240]
    return source


def _fingerprint_id(payload: dict[str, JSONValue]) -> str:
    encoded = json.dumps(
        _fingerprint_material(payload),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _fingerprint_material(
    payload: dict[str, JSONValue],
) -> dict[str, JSONValue]:
    material = dict(payload)
    material.pop("fingerprint_id", None)
    source = dict(cast(dict[str, JSONValue], material.get("source") or {}))
    source.pop("path", None)
    material["source"] = source
    return material
