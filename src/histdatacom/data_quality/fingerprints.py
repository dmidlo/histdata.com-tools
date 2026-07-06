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
    calendar_regime_payload_for_target,
    classify_histdata_source_timestamp,
    classify_histdata_timestamp,
)
from histdatacom.data_quality.calendar_profiles import (
    HistDataCalendarProfile,
    default_calendar_profile,
)
from histdatacom.data_quality.polars_cache import read_quality_polars_cache
from histdatacom.data_quality.remediation import (
    remediation_hint_payloads_for_flags,
)
from histdatacom.data_quality.symbols import symbol_metadata_for
from histdatacom.data_quality.time import timestamp_topology_payload_for_target
from histdatacom.data_quality.ticks import (
    DEFAULT_TICK_MICROSTRUCTURE_THRESHOLDS,
    DEFAULT_TICK_SPREAD_REGIME_THRESHOLDS,
)
from histdatacom.histdata_ascii import (
    M1,
    TICK,
    columns_for_timeframe,
    delimiter_for_timeframe,
    normalize_ascii_row,
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
TIME_SERIES_FINGERPRINT_CONDITIONAL_DISTRIBUTIONS_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-conditional-distributions.v1"
)
TIME_SERIES_FINGERPRINT_DYNAMICS_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-dynamics.v1"
)
TIME_SERIES_FINGERPRINT_DEPENDENCE_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-dependence.v1"
)
TIME_SERIES_FINGERPRINT_AUDIT_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-audit.v1"
)
TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.time-series-fingerprint-readiness-summary.v1"
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
TIME_SERIES_FINGERPRINT_READINESS_SUMMARY_METADATA_KEY = (
    "time_series_fingerprint_readiness_summary"
)
SERIES_FINGERPRINT_RULE_ID = "fingerprint.series"
CROSS_SERIES_FINGERPRINT_RULE_ID = "fingerprint.cross_series"
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
DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT = 128
DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT = 32
DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT = 128
DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT = 32
DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT = 16
DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_COUNT = 1
DEFAULT_FINGERPRINT_DISTRIBUTION_INVALID_ROW_MIN_RATE = 0.0
DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_COUNT = 1
DEFAULT_FINGERPRINT_DISTRIBUTION_ZERO_SPREAD_MIN_RATE = 0.0
DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_COUNT = 1
DEFAULT_FINGERPRINT_DISTRIBUTION_NEGATIVE_SPREAD_MIN_RATE = 0.0
DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_TRUNCATED = True
DEFAULT_FINGERPRINT_DISTRIBUTION_FLAG_CACHE_FLOAT_PRECISION = True
SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES = (M1, TICK)
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
    "m1_bar_distribution",
    "tick_distribution",
    "conditional_distributions",
    "return_dynamics",
    "microstructure_dynamics",
    "dependence",
)
FINGERPRINT_DYNAMICS_SECTIONS = ("return_dynamics", "microstructure_dynamics")


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
class HistDataFingerprintProfile:
    """Operator-tunable limits for deterministic fingerprint summaries."""

    quantiles: tuple[float, ...] = DEFAULT_FINGERPRINT_QUANTILES
    lags: tuple[int, ...] = DEFAULT_FINGERPRINT_LAGS
    rolling_windows: tuple[int, ...] = DEFAULT_FINGERPRINT_ROLLING_WINDOWS
    histogram_bins: int = DEFAULT_FINGERPRINT_HISTOGRAM_BINS
    max_rows: int = DEFAULT_FINGERPRINT_MAX_ROWS
    rounding_digits: int = DEFAULT_FINGERPRINT_ROUNDING_DIGITS
    calendar_profile: HistDataCalendarProfile = field(
        default_factory=default_calendar_profile,
        repr=False,
        compare=False,
    )
    distribution_attention: HistDataFingerprintDistributionAttentionProfile = (
        field(default_factory=HistDataFingerprintDistributionAttentionProfile)
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
            "distribution_attention": (
                self.distribution_attention.to_metadata()
            ),
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
    target_limit: int = DEFAULT_FINGERPRINT_TOPOLOGY_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded target summaries for fingerprint timestamp topology."""
    target_summaries = _series_fingerprint_topology_target_summaries(findings)

    if not target_summaries:
        return None

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

    included_source = (
        target_summaries
        if target_limit < 0
        else target_summaries[:target_limit]
    )
    included: list[JSONValue] = [dict(item) for item in included_source]
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
    target_limit: int = DEFAULT_FINGERPRINT_TOPOLOGY_ATTENTION_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded attention-first summaries for topology findings."""
    target_summaries = _series_fingerprint_topology_target_summaries(findings)
    if not target_summaries:
        return None
    return _topology_attention_summary_from_targets(
        target_summaries,
        target_limit=target_limit,
    )


def series_fingerprint_distribution_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int = DEFAULT_FINGERPRINT_DISTRIBUTION_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded target summaries for fingerprint distributions."""
    target_summaries = _series_fingerprint_distribution_target_summaries(
        findings
    )
    if not target_summaries:
        return None

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

    included_source = (
        target_summaries
        if target_limit < 0
        else target_summaries[:target_limit]
    )
    included: list[JSONValue] = [dict(item) for item in included_source]
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
        "distribution_target_count": sum(
            1
            for item in target_summaries
            if item.get("distribution_kind") != "missing"
        ),
        "m1_bar_distribution_target_count": distribution_kind_counts["m1_bar"],
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
    target_limit: int = DEFAULT_FINGERPRINT_DISTRIBUTION_ATTENTION_LIMIT,
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

    included_source = (
        attention_targets
        if target_limit < 0
        else attention_targets[:target_limit]
    )
    included: list[JSONValue] = [dict(item) for item in included_source]
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
        "attention_thresholds": attention_profile.to_metadata(),
        "attention_level_counts": _counter_payload(attention_level_counts),
        "attention_flag_counts": _counter_payload(attention_flag_counts),
        "target_summaries": included,
    }


def series_fingerprint_readiness_summary(
    findings: Iterable[QualityFinding],
    *,
    target_limit: int = DEFAULT_FINGERPRINT_READINESS_SUMMARY_LIMIT,
) -> dict[str, JSONValue] | None:
    """Return bounded target summaries for fingerprint audit/readiness."""
    target_summaries = _series_fingerprint_readiness_target_summaries(findings)
    if not target_summaries:
        return None

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

    included_source = (
        target_summaries
        if target_limit < 0
        else target_summaries[:target_limit]
    )
    included: list[JSONValue] = [dict(item) for item in included_source]
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
        "applicable_dynamics_status_counts": _counter_payload(
            applicable_status_counts
        ),
        "section_status_counts": section_status_counts,
        "dynamics_status_counts": dynamics_status_counts,
        "dynamics_reason_counts": dynamics_reason_counts,
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
    return_readiness = _fingerprint_readiness_for_section(
        "return_dynamics",
        finding,
        payload,
        audit,
    )
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
        return_readiness
        if applicable_section == "return_dynamics"
        else (
            microstructure_readiness
            if applicable_section == "microstructure_dynamics"
            else {
                "status": "unavailable",
                "reason": "unsupported_timeframe",
            }
        )
    )
    topology = _payload_mapping(payload.get("temporal_topology"))

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
        "return_dynamics": _fingerprint_readiness_return_summary(
            payload,
            return_readiness,
        ),
        "microstructure_dynamics": (
            _fingerprint_readiness_microstructure_summary(
                payload,
                microstructure_readiness,
            )
        ),
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
    if timeframe == M1:
        return "return_dynamics"
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


def _fingerprint_readiness_return_summary(
    payload: Mapping[str, JSONValue],
    readiness: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    summary = dict(readiness)
    dynamics = _payload_mapping(payload.get("return_dynamics"))
    if not dynamics:
        return summary
    summary.update(
        {
            "close_log_return": _compact_numeric_summary(
                dynamics.get("close_log_return")
            ),
            "absolute_return": _compact_numeric_summary(
                dynamics.get("absolute_return")
            ),
            "squared_return": _compact_numeric_summary(
                dynamics.get("squared_return")
            ),
            "open_jump": _compact_numeric_summary(dynamics.get("open_jump")),
            "flatline": _compact_flatline_summary(dynamics.get("flatline")),
        }
    )
    return summary


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


def _compact_flatline_summary(value: JSONValue) -> dict[str, JSONValue]:
    flatline = _payload_mapping(value)
    if not flatline:
        return {}
    return {
        "zero_return_count": _int_payload(flatline.get("zero_return_count")),
        "zero_return_rate": _optional_float_payload(
            flatline.get("zero_return_rate")
        ),
        "zero_return_run_count": _int_payload(
            flatline.get("zero_return_run_count")
        ),
        "ohlc_flatline_row_count": _int_payload(
            flatline.get("ohlc_flatline_row_count")
        ),
        "ohlc_flatline_rate": _optional_float_payload(
            flatline.get("ohlc_flatline_rate")
        ),
        "ohlc_flatline_run_count": _int_payload(
            flatline.get("ohlc_flatline_run_count")
        ),
        "ohlc_flatline_affected_row_count": _int_payload(
            flatline.get("ohlc_flatline_affected_row_count")
        ),
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
    return (
        _fingerprint_readiness_status_rank(
            _summary_key(target.get("applicable_dynamics_status"))
        ),
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
    m1_distribution = _payload_mapping(payload.get("m1_bar_distribution"))
    tick_distribution = _payload_mapping(payload.get("tick_distribution"))
    distribution_kind, distribution = _distribution_kind_and_payload(
        target_axis,
        source,
        m1_distribution=m1_distribution,
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
    m1_distribution: Mapping[str, JSONValue],
    tick_distribution: Mapping[str, JSONValue],
) -> tuple[str, Mapping[str, JSONValue]]:
    timeframe = _summary_key(target_axis.get("timeframe"))
    if timeframe == M1 and m1_distribution:
        return "m1_bar", m1_distribution
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
    if distribution_kind != "m1_bar":
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
        distribution_kind == "m1_bar"
        and _int_payload(target.get("precision_decimal_place_count")) == 0
    ):
        flags.append("missing_precision_counts")
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
        "temporal_topology": timestamp_topology_payload_for_target(target),
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
    return _finalize_fingerprint_payload(
        payload,
        target=target,
        profile=profile,
    )


def _finalize_fingerprint_payload(
    payload: dict[str, JSONValue],
    *,
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    payload["fingerprint_audit"] = _fingerprint_audit_payload(
        payload,
        target=target,
        profile=profile,
    )
    payload["fingerprint_id"] = _fingerprint_id(payload)
    return payload


def _fingerprint_audit_payload(
    payload: Mapping[str, JSONValue],
    *,
    target: QualityTarget,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    expected = _fingerprint_expected_sections(target)
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
        "return_dynamics": _fingerprint_dynamics_readiness(
            "return_dynamics",
            payload,
            target=target,
        ),
        "microstructure_dynamics": _fingerprint_dynamics_readiness(
            "microstructure_dynamics",
            payload,
            target=target,
        ),
    }
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
    }
    return audit_payload


def _fingerprint_expected_sections(
    target: QualityTarget,
) -> tuple[str, ...]:
    sections = ["coverage", "temporal_topology"]
    if target.timeframe in SUPPORTED_SERIES_FINGERPRINT_TIMEFRAMES:
        sections.append("calendar_regimes")
    if target.timeframe == M1:
        sections.extend(
            ("m1_bar_distribution", "return_dynamics", "dependence")
        )
    elif target.timeframe == TICK:
        sections.extend(
            (
                "tick_distribution",
                "conditional_distributions",
                "microstructure_dynamics",
                "dependence",
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
        "m1_bar_distribution",
        "tick_distribution",
        "return_dynamics",
        "microstructure_dynamics",
        "dependence",
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
            section in {"m1_bar_distribution", "return_dynamics"}
            and target.timeframe != M1
        )
        or (
            section
            in {
                "tick_distribution",
                "conditional_distributions",
                "microstructure_dynamics",
            }
            and target.timeframe != TICK
        )
        or (
            section == "dependence"
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
    if section in {"m1_bar_distribution", "tick_distribution"}:
        return _distribution_section_status(_payload_mapping(payload[section]))
    if section == "conditional_distributions":
        return _conditional_distribution_section_status(
            _payload_mapping(payload.get("conditional_distributions"))
        )
    if section in {"return_dynamics", "microstructure_dynamics"}:
        return _dynamics_section_status(_payload_mapping(payload[section]))
    if section == "dependence":
        return _dependence_section_status(_payload_mapping(payload[section]))
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


def _string_list(value: object) -> list[JSONValue]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item or "").strip()]


def _payload_mapping(value: object) -> Mapping[str, JSONValue]:
    if isinstance(value, Mapping):
        return cast(Mapping[str, JSONValue], value)
    return {}


def _summary_key(value: object) -> str:
    text = str(value or "").strip()
    return text or "unknown"


def _counter_payload(counter: Counter[str]) -> dict[str, JSONValue]:
    return {key: counter[key] for key in sorted(counter)}


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
        target_summaries.append(
            _topology_target_summary(finding, target_axis, topology)
        )
    return target_summaries


def _topology_target_summary(
    finding: QualityFinding,
    target_axis: Mapping[str, JSONValue],
    topology: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    flags = _topology_flags(topology)
    return {
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
    target_limit: int,
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

    included_source = (
        attention_targets
        if target_limit < 0
        else attention_targets[:target_limit]
    )
    included: list[JSONValue] = [dict(item) for item in included_source]
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
        "attention_level_counts": _counter_payload(priority_counts),
        "attention_flag_counts": _counter_payload(flag_counts),
        "target_summaries": included,
    }


def _topology_attention_target_summary(
    target: Mapping[str, JSONValue],
) -> dict[str, JSONValue] | None:
    flags = _topology_summary_flags(target.get("flags"))
    flag_set = set(flags)
    attention_flags = [
        flag for flag in ACTIONABLE_TOPOLOGY_FLAGS if flag in flag_set
    ]
    if not attention_flags:
        return None
    attention_level = _topology_attention_level(attention_flags)
    return {
        "target_axis": _topology_attention_axis(target),
        "attention_level": attention_level,
        "attention_flags": list(attention_flags),
        "remediation_hints": remediation_hint_payloads_for_flags(
            attention_flags
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


def _topology_attention_level(flags: list[str]) -> str:
    flag_set = set(flags)
    if "unavailable_topology" in flag_set:
        return "unavailable"
    if flag_set & {"invalid_timestamps", "non_monotonic_timestamps"}:
        return "structural"
    if flag_set & {"duplicate_timestamps", "suspicious_gaps"}:
        return "sequence"
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
class _M1DynamicsRow:
    timestamp_utc_ms: int
    open: float
    high: float
    low: float
    close: float


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
    if timeframe == M1:
        payload["m1_bar_distribution"] = distribution
    elif timeframe == TICK:
        payload["tick_distribution"] = distribution


def _add_dynamics_payload(
    payload: dict[str, JSONValue],
    *,
    target: QualityTarget,
    frame: Any | None,
    text: str | None,
    profile: HistDataFingerprintProfile,
) -> None:
    if target.timeframe == M1:
        return_dynamics, dependence = _m1_sequence_payloads(
            payload,
            frame=frame,
            text=text,
            profile=profile,
        )
        payload["return_dynamics"] = return_dynamics
        payload["dependence"] = dependence
    elif target.timeframe == TICK:
        microstructure_dynamics, dependence = _tick_sequence_payloads(
            payload,
            frame=frame,
            text=text,
            profile=profile,
        )
        payload["microstructure_dynamics"] = microstructure_dynamics
        payload["dependence"] = dependence


def _m1_sequence_payloads(
    payload: Mapping[str, JSONValue],
    *,
    frame: Any | None,
    text: str | None,
    profile: HistDataFingerprintProfile,
) -> tuple[dict[str, JSONValue], dict[str, JSONValue]]:
    if frame is not None:
        rows, row_count, usable_row_count, invalid_row_count = (
            _m1_dynamics_rows_from_frame(frame, profile)
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
        ) = _m1_dynamics_rows_from_text(text, profile)
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
    return (
        _m1_return_dynamics_payload(rows, base=base, profile=profile),
        _m1_dependence_payload(rows, base=base, profile=profile),
    )


def _tick_sequence_payloads(
    payload: Mapping[str, JSONValue],
    *,
    frame: Any | None,
    text: str | None,
    profile: HistDataFingerprintProfile,
) -> tuple[dict[str, JSONValue], dict[str, JSONValue]]:
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
    return (
        _tick_microstructure_dynamics_payload(
            rows,
            base=base,
            profile=profile,
        ),
        _tick_dependence_payload(rows, base=base, profile=profile),
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


def _m1_dynamics_rows_from_frame(
    frame: Any,
    profile: HistDataFingerprintProfile,
) -> tuple[list[_M1DynamicsRow], int, int, int]:
    row_count = int(getattr(frame, "height", 0) or 0)
    sample_limit = min(row_count, _profile_max_rows(profile))
    usable_row_count = _frame_numeric_usable_row_count(
        frame,
        ("datetime", "open", "high", "low", "close"),
    )
    rows: list[_M1DynamicsRow] = []
    for row in _iter_frame_head_rows(frame, sample_limit):
        timestamp = _finite_int(row.get("datetime"))
        open_value = _finite_float(row.get("open"))
        high_value = _finite_float(row.get("high"))
        low_value = _finite_float(row.get("low"))
        close_value = _finite_float(row.get("close"))
        if (
            timestamp is None
            or open_value is None
            or high_value is None
            or low_value is None
            or close_value is None
        ):
            continue
        rows.append(
            _M1DynamicsRow(
                timestamp_utc_ms=timestamp,
                open=open_value,
                high=high_value,
                low=low_value,
                close=close_value,
            )
        )
    return (
        rows,
        row_count,
        usable_row_count,
        max(0, row_count - usable_row_count),
    )


def _m1_dynamics_rows_from_text(
    text: str,
    profile: HistDataFingerprintProfile,
) -> tuple[list[_M1DynamicsRow], int, int, int, int]:
    row_count = 0
    usable_row_count = 0
    invalid_row_count = 0
    partial_row_count = 0
    rows: list[_M1DynamicsRow] = []
    sample_limit = _profile_max_rows(profile)
    expected_field_count = len(columns_for_timeframe(M1))
    reader = csv.reader(
        text.splitlines(),
        delimiter=delimiter_for_timeframe(M1),
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
            parsed = normalize_ascii_row(M1, row)
        except (TypeError, ValueError, OverflowError):
            invalid_row_count += 1
            continue
        timestamp = int(parsed[0])
        prices = tuple(float(parsed[index]) for index in range(1, 5))
        if not all(_is_finite(value) for value in prices):
            invalid_row_count += 1
            continue
        usable_row_count += 1
        if len(rows) >= sample_limit:
            continue
        rows.append(
            _M1DynamicsRow(
                timestamp_utc_ms=timestamp,
                open=prices[0],
                high=prices[1],
                low=prices[2],
                close=prices[3],
            )
        )
    return (
        rows,
        row_count,
        usable_row_count,
        invalid_row_count,
        partial_row_count,
    )


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


def _m1_return_dynamics_payload(
    rows: list[_M1DynamicsRow],
    *,
    base: dict[str, JSONValue],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    close_log_returns: list[float] = []
    absolute_returns: list[float] = []
    squared_returns: list[float] = []
    open_jumps: list[float] = []
    zero_return_count = 0
    zero_return_run_lengths: list[int] = []
    ohlc_flatline_row_count = 0
    ohlc_flatline_run_lengths: list[int] = []
    same_close_run_length = 1 if rows else 0
    ohlc_flatline_run_length = 0
    previous: _M1DynamicsRow | None = None

    for row in rows:
        if row.open == row.high == row.low == row.close:
            ohlc_flatline_row_count += 1
            ohlc_flatline_run_length += 1
        else:
            _append_minimum_run(
                ohlc_flatline_run_lengths,
                ohlc_flatline_run_length,
                minimum=2,
            )
            ohlc_flatline_run_length = 0

        if previous is None:
            previous = row
            continue

        if previous.close > 0.0 and row.close > 0.0:
            log_return = math.log(row.close / previous.close)
            close_log_returns.append(log_return)
            absolute_returns.append(abs(log_return))
            squared_returns.append(log_return * log_return)
            if row.close == previous.close:
                zero_return_count += 1
                same_close_run_length += 1
            else:
                _append_minimum_run(
                    zero_return_run_lengths,
                    same_close_run_length,
                    minimum=2,
                )
                same_close_run_length = 1
        else:
            _append_minimum_run(
                zero_return_run_lengths,
                same_close_run_length,
                minimum=2,
            )
            same_close_run_length = 1

        if previous.close > 0.0:
            open_jumps.append(abs(row.open - previous.close) / previous.close)
        previous = row

    _append_minimum_run(
        zero_return_run_lengths,
        same_close_run_length,
        minimum=2,
    )
    _append_minimum_run(
        ohlc_flatline_run_lengths,
        ohlc_flatline_run_length,
        minimum=2,
    )

    result = dict(base)
    result.update(
        {
            "close_log_return": _numeric_summary(close_log_returns, profile),
            "absolute_return": _numeric_summary(absolute_returns, profile),
            "squared_return": _numeric_summary(squared_returns, profile),
            "open_jump": _numeric_summary(open_jumps, profile),
            "flatline": {
                "zero_return_count": zero_return_count,
                "zero_return_rate": _rate(
                    zero_return_count,
                    len(close_log_returns),
                    profile,
                ),
                "zero_return_run_count": len(zero_return_run_lengths),
                "zero_return_run_length_counts": (
                    _run_length_counts_payload(
                        zero_return_run_lengths,
                        profile,
                    )
                ),
                "ohlc_flatline_row_count": ohlc_flatline_row_count,
                "ohlc_flatline_rate": _rate(
                    ohlc_flatline_row_count,
                    len(rows),
                    profile,
                ),
                "ohlc_flatline_run_count": len(ohlc_flatline_run_lengths),
                "ohlc_flatline_affected_row_count": sum(
                    ohlc_flatline_run_lengths
                ),
                "ohlc_flatline_run_length_counts": (
                    _run_length_counts_payload(
                        ohlc_flatline_run_lengths,
                        profile,
                    )
                ),
            },
        }
    )
    return result


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


def _m1_dependence_payload(
    rows: list[_M1DynamicsRow],
    *,
    base: dict[str, JSONValue],
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    close_log_returns: list[float] = []
    absolute_returns: list[float] = []
    squared_returns: list[float] = []
    range_ratios: list[float] = []
    previous: _M1DynamicsRow | None = None

    for row in rows:
        range_ratio = _m1_range_ratio(row)
        if range_ratio is not None:
            range_ratios.append(range_ratio)
        if previous is None:
            previous = row
            continue
        if previous.close > 0.0 and row.close > 0.0:
            log_return = math.log(row.close / previous.close)
            close_log_returns.append(log_return)
            absolute_returns.append(abs(log_return))
            squared_returns.append(log_return * log_return)
        previous = row

    return _dependence_payload(
        base,
        profile=profile,
        acf_series={
            "close_log_return_acf": close_log_returns,
            "absolute_return_acf": absolute_returns,
            "squared_return_acf": squared_returns,
            "range_ratio_acf": range_ratios,
        },
    )


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


def _m1_range_ratio(row: _M1DynamicsRow) -> float | None:
    decimal_values = tuple(
        Decimal(str(value))
        for value in (row.open, row.high, row.low, row.close)
    )
    _open_decimal, high_decimal, low_decimal, _close_decimal = decimal_values
    range_decimal = high_decimal - low_decimal
    midpoint_decimal = (high_decimal + low_decimal) / Decimal("2")
    if not midpoint_decimal:
        return None
    return float(range_decimal / midpoint_decimal)


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
    if timeframe == M1:
        return _m1_bar_distribution_from_frame(frame, profile)
    if timeframe == TICK:
        return _tick_distribution_from_frame(frame, profile)
    return {}


def _distribution_from_text(
    text: str,
    *,
    timeframe: str,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    if timeframe == M1:
        return _m1_bar_distribution_from_text(text, profile)
    if timeframe == TICK:
        return _tick_distribution_from_text(text, profile)
    return {}


def _m1_bar_distribution_from_frame(
    frame: Any,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    row_count = int(getattr(frame, "height", 0) or 0)
    sample_limit = min(row_count, _profile_max_rows(profile))
    usable_row_count = _frame_numeric_usable_row_count(
        frame,
        ("open", "high", "low", "close"),
    )
    columns = _frame_sampled_valid_column_values(
        frame,
        ("open", "high", "low", "close"),
        sample_limit,
    )
    return _m1_bar_distribution_from_column_values(
        columns,
        row_count=row_count,
        usable_row_count=usable_row_count,
        sample_limit=sample_limit,
        profile=profile,
        precision_source="cache_float",
    )


def _m1_bar_distribution_from_text(
    text: str,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    row_count = 0
    invalid_row_count = 0
    partial_row_count = 0
    usable_row_count = 0
    sample_limit = _profile_max_rows(profile)
    sampled_rows: list[tuple[float, float, float, float]] = []
    precision_counts: Counter[str] = Counter()
    column_precision_counts: dict[str, Counter[str]] = {
        "open": Counter(),
        "high": Counter(),
        "low": Counter(),
        "close": Counter(),
    }
    reader = csv.reader(
        text.splitlines(), delimiter=delimiter_for_timeframe(M1)
    )
    expected_field_count = len(columns_for_timeframe(M1))
    for row in reader:
        if not row or not any(cell.strip() for cell in row):
            continue
        row_count += 1
        if len(row) != expected_field_count:
            invalid_row_count += 1
            partial_row_count += 1
            continue
        try:
            parsed = normalize_ascii_row(M1, row)
        except (TypeError, ValueError, OverflowError):
            invalid_row_count += 1
            continue
        prices = tuple(float(parsed[index]) for index in range(1, 5))
        if not all(_is_finite(value) for value in prices):
            invalid_row_count += 1
            continue
        usable_row_count += 1
        if len(sampled_rows) >= sample_limit:
            continue
        sampled_rows.append(cast(tuple[float, float, float, float], prices))
        for column_name, raw_value in zip(
            ("open", "high", "low", "close"),
            row[1:5],
            strict=True,
        ):
            places = _decimal_places_from_text(raw_value)
            key = str(places)
            precision_counts[key] += 1
            column_precision_counts[column_name][key] += 1

    payload = _m1_bar_distribution_from_rows(
        sampled_rows,
        row_count=row_count,
        usable_row_count=usable_row_count,
        invalid_row_count=invalid_row_count,
        partial_row_count=partial_row_count,
        truncated=usable_row_count > len(sampled_rows),
        profile=profile,
    )
    precision = cast(dict[str, JSONValue], payload["precision"])
    precision["precision_source"] = "text"
    precision["decimal_place_counts"] = _counter_payload(precision_counts)
    precision["column_decimal_place_counts"] = {
        column: _counter_payload(counts)
        for column, counts in column_precision_counts.items()
    }
    return payload


def _m1_bar_distribution_from_column_values(
    columns: Mapping[str, list[Any]],
    *,
    row_count: int,
    usable_row_count: int,
    sample_limit: int,
    profile: HistDataFingerprintProfile,
    precision_source: str,
) -> dict[str, JSONValue]:
    sampled_rows: list[tuple[float, float, float, float]] = []
    sampled_invalid_row_count = 0
    sampled_count = min(
        sample_limit,
        *(
            len(columns.get(name, ()))
            for name in ("open", "high", "low", "close")
        ),
    )
    precision_counts: Counter[str] = Counter()
    column_precision_counts: dict[str, Counter[str]] = {
        "open": Counter(),
        "high": Counter(),
        "low": Counter(),
        "close": Counter(),
    }
    for index in range(sampled_count):
        prices: list[float] = []
        for column_name in ("open", "high", "low", "close"):
            value = _finite_float(columns[column_name][index])
            if value is None:
                prices = []
                break
            prices.append(value)
        if len(prices) != 4:
            sampled_invalid_row_count += 1
            continue
        sampled_rows.append((prices[0], prices[1], prices[2], prices[3]))
        for column_name, value in zip(
            ("open", "high", "low", "close"),
            prices,
            strict=True,
        ):
            key = str(_decimal_places_from_float(value, profile))
            precision_counts[key] += 1
            column_precision_counts[column_name][key] += 1

    payload = _m1_bar_distribution_from_rows(
        sampled_rows,
        row_count=row_count,
        usable_row_count=len(sampled_rows),
        invalid_row_count=sampled_invalid_row_count,
        truncated=usable_row_count > len(sampled_rows),
        profile=profile,
    )
    payload["usable_row_count"] = usable_row_count
    payload["invalid_row_count"] = max(0, row_count - usable_row_count)
    precision = cast(dict[str, JSONValue], payload["precision"])
    precision["precision_source"] = precision_source
    precision["decimal_place_counts"] = _counter_payload(precision_counts)
    precision["column_decimal_place_counts"] = {
        column: _counter_payload(counts)
        for column, counts in column_precision_counts.items()
    }
    return payload


def _m1_bar_distribution_from_rows(
    rows: list[tuple[float, float, float, float]],
    *,
    row_count: int,
    usable_row_count: int,
    invalid_row_count: int,
    partial_row_count: int = 0,
    truncated: bool,
    profile: HistDataFingerprintProfile,
) -> dict[str, JSONValue]:
    open_values = [row[0] for row in rows]
    high_values = [row[1] for row in rows]
    low_values = [row[2] for row in rows]
    close_values = [row[3] for row in rows]
    range_ratios: list[float] = []
    body_ratios: list[float] = []
    upper_wick_ratios: list[float] = []
    lower_wick_ratios: list[float] = []
    for open_value, high_value, low_value, close_value in rows:
        decimal_values = tuple(
            Decimal(str(value))
            for value in (open_value, high_value, low_value, close_value)
        )
        open_decimal, high_decimal, low_decimal, close_decimal = decimal_values
        range_decimal = high_decimal - low_decimal
        midpoint_decimal = (high_decimal + low_decimal) / Decimal("2")
        if midpoint_decimal:
            range_ratios.append(float(range_decimal / midpoint_decimal))
        if range_decimal <= 0:
            body_ratios.append(0.0)
            upper_wick_ratios.append(0.0)
            lower_wick_ratios.append(0.0)
            continue
        body_ratios.append(
            float(abs(close_decimal - open_decimal) / range_decimal)
        )
        upper_wick_ratios.append(
            float(
                (high_decimal - max(open_decimal, close_decimal))
                / range_decimal
            )
        )
        lower_wick_ratios.append(
            float(
                (min(open_decimal, close_decimal) - low_decimal) / range_decimal
            )
        )
    return {
        "row_count": row_count,
        "sampled_row_count": len(rows),
        "usable_row_count": usable_row_count,
        "invalid_row_count": invalid_row_count,
        "partial_row_count": partial_row_count,
        "truncated": truncated,
        "price": {
            "open": _numeric_summary(open_values, profile),
            "high": _numeric_summary(high_values, profile),
            "low": _numeric_summary(low_values, profile),
            "close": _numeric_summary(close_values, profile),
        },
        "range_ratio": _numeric_summary(range_ratios, profile),
        "ohlc_shape": {
            "body_ratio": _numeric_summary(body_ratios, profile),
            "upper_wick_ratio": _numeric_summary(
                upper_wick_ratios,
                profile,
            ),
            "lower_wick_ratio": _numeric_summary(
                lower_wick_ratios,
                profile,
            ),
        },
        "precision": {
            "precision_source": "unknown",
            "decimal_place_counts": {},
            "column_decimal_place_counts": {
                "open": {},
                "high": {},
                "low": {},
                "close": {},
            },
        },
    }


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
