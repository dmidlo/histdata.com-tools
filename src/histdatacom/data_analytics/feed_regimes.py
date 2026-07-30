"""Canonical feed-regime compatibility and feed-epoch analytics surface."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from statistics import median
from typing import Any

from histdatacom.data_analytics.feed_epochs import (
    FeedEpochDefinitionV1,
    FeedEpochEvidenceV1,
    FeedEpochFitConfigV1,
    fit_feed_epochs,
)
from histdatacom.data_quality.discovery import (
    discover_quality_targets,
    quality_target_from_path,
)
from histdatacom.data_quality.engine import run_quality_assessment
from histdatacom.data_quality.fingerprints import (
    TIME_SERIES_FINGERPRINT_METADATA_KEY,
    HistDataFingerprintProfile,
    fingerprint_quality_rules,
)
from histdatacom.histdata_ascii import TICK
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

ANALYTICS_REPORT_SCHEMA_VERSION = "histdatacom.feed-regime-report.v1"
FEED_REGIME_OPERATION = "feed-regime-detection"
DEFAULT_QUIET_GAP_MS = 60_000
_ASCII_FORMAT = "ascii"


@dataclass(frozen=True, slots=True)
class AnalyticsTarget:
    """Compatibility projection of one canonical quality target."""

    path: str
    kind: str
    data_format: str = ""
    timeframe: str = ""
    symbol: str = ""
    period: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def is_supported_tick_target(self) -> bool:
        """Return whether the target can feed technological epoch fitting."""
        return self.data_format == _ASCII_FORMAT and self.timeframe == TICK

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "path": self.path,
            "kind": self.kind,
            "data_format": self.data_format,
            "timeframe": self.timeframe,
            "symbol": self.symbol,
            "period": self.period,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class AnalyticsDiscoveryResult:
    """Canonical discovery result projected for analytics callers."""

    roots: tuple[str, ...] = ()
    targets: tuple[AnalyticsTarget, ...] = ()
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @property
    def target_count(self) -> int:
        """Return the number of discovered targets."""
        return len(self.targets)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "roots": list(self.roots),
            "target_count": self.target_count,
            "targets": [target.to_dict() for target in self.targets],
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class FeedPeriodProfile:
    """Canonical fingerprint summary for one symbol-period."""

    symbol: str
    period: str
    bucket: str
    row_count: int
    start_utc_ms: int
    end_utc_ms: int
    tick_rate_per_hour: float
    median_interarrival_ms: float
    p95_interarrival_ms: float
    max_interarrival_ms: int
    quiet_gap_count: int
    quote_update_count: int
    quote_update_ratio: float
    zero_change_run_count: int
    zero_change_tick_count: int
    spread_min: float
    spread_median: float
    spread_mean: float
    spread_max: float
    session_counts: dict[str, int] = field(default_factory=dict)
    target_paths: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "symbol": self.symbol,
            "period": self.period,
            "bucket": self.bucket,
            "row_count": self.row_count,
            "start_utc_ms": self.start_utc_ms,
            "end_utc_ms": self.end_utc_ms,
            "tick_rate_per_hour": _round_float(self.tick_rate_per_hour),
            "median_interarrival_ms": _round_float(self.median_interarrival_ms),
            "p95_interarrival_ms": _round_float(self.p95_interarrival_ms),
            "max_interarrival_ms": self.max_interarrival_ms,
            "quiet_gap_count": self.quiet_gap_count,
            "quote_update_count": self.quote_update_count,
            "quote_update_ratio": _round_float(self.quote_update_ratio),
            "zero_change_run_count": self.zero_change_run_count,
            "zero_change_tick_count": self.zero_change_tick_count,
            "spread_min": _round_float(self.spread_min),
            "spread_median": _round_float(self.spread_median),
            "spread_mean": _round_float(self.spread_mean),
            "spread_max": _round_float(self.spread_max),
            "session_counts": dict(sorted(self.session_counts.items())),
            "target_paths": list(self.target_paths),
        }


@dataclass(frozen=True, slots=True)
class FeedRegimeEra:
    """Compatibility projection of one versioned technological epoch."""

    symbol: str
    label: str
    bucket: str
    period_start: str
    period_end: str
    start_utc_ms: int
    end_utc_ms: int
    profile_count: int
    row_count: int
    mean_tick_rate_per_hour: float
    median_interarrival_ms: float
    quote_update_ratio: float
    quiet_gap_count: int
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible representation."""
        return {
            "symbol": self.symbol,
            "label": self.label,
            "bucket": self.bucket,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "start_utc_ms": self.start_utc_ms,
            "end_utc_ms": self.end_utc_ms,
            "profile_count": self.profile_count,
            "row_count": self.row_count,
            "mean_tick_rate_per_hour": _round_float(
                self.mean_tick_rate_per_hour
            ),
            "median_interarrival_ms": _round_float(self.median_interarrival_ms),
            "quote_update_ratio": _round_float(self.quote_update_ratio),
            "quiet_gap_count": self.quiet_gap_count,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class FeedRegimeReport:
    """Machine-readable compatibility report containing the epoch artifact."""

    discovery: AnalyticsDiscoveryResult
    period_profiles: tuple[FeedPeriodProfile, ...] = ()
    regimes: tuple[FeedRegimeEra, ...] = ()
    epoch_definition: FeedEpochDefinitionV1 | None = None
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def summary(self) -> dict[str, JSONValue]:
        """Return compact report-level summary statistics."""
        symbols: list[JSONValue] = []
        symbols.extend(
            sorted({profile.symbol for profile in self.period_profiles})
        )
        return {
            "operation": FEED_REGIME_OPERATION,
            "target_count": self.discovery.target_count,
            "supported_target_count": sum(
                target.is_supported_tick_target
                for target in self.discovery.targets
            ),
            "profile_count": len(self.period_profiles),
            "regime_count": len(self.regimes),
            "symbols": symbols,
            "epoch_definition_id": (
                self.epoch_definition.definition_id
                if self.epoch_definition is not None
                else None
            ),
            "stability_status": (
                self.epoch_definition.stability.status
                if self.epoch_definition is not None
                else "unavailable"
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible report payload."""
        return {
            "schema_version": ANALYTICS_REPORT_SCHEMA_VERSION,
            "operation": FEED_REGIME_OPERATION,
            "summary": self.summary(),
            "discovery": self.discovery.to_dict(),
            "period_profiles": [
                profile.to_dict() for profile in self.period_profiles
            ],
            "regimes": [regime.to_dict() for regime in self.regimes],
            "epoch_definition": (
                self.epoch_definition.to_dict()
                if self.epoch_definition is not None
                else None
            ),
            "metadata": dict(self.metadata),
        }


def discover_analytics_targets(
    paths: Iterable[str | Path],
) -> AnalyticsDiscoveryResult:
    """Project canonical quality discovery without an independent scanner."""
    discovery = discover_quality_targets(paths)
    return AnalyticsDiscoveryResult(
        roots=discovery.roots,
        targets=tuple(
            _analytics_target(target) for target in discovery.targets
        ),
        metadata=_discovery_metadata(),
    )


def analytics_target_from_path(path: str | Path) -> AnalyticsTarget | None:
    """Project one canonical quality target into the analytics type."""
    target = quality_target_from_path(path)
    return _analytics_target(target) if target is not None else None


def analyze_feed_regimes(
    paths: Iterable[str | Path],
    *,
    bucket: str = "month",
    quiet_gap_ms: int = DEFAULT_QUIET_GAP_MS,
    fit_config: FeedEpochFitConfigV1 | None = None,
    fingerprint_profile: HistDataFingerprintProfile | None = None,
) -> FeedRegimeReport:
    """Fit feed epochs through canonical discovery and fingerprint evidence."""
    normalized_bucket = _normalize_bucket(bucket)
    if quiet_gap_ms <= 0:
        raise ValueError("quiet_gap_ms must be positive")
    quality_discovery = discover_quality_targets(tuple(paths))
    discovery = AnalyticsDiscoveryResult(
        roots=quality_discovery.roots,
        targets=tuple(
            _analytics_target(target) for target in quality_discovery.targets
        ),
        metadata=_discovery_metadata(),
    )
    quality_report = run_quality_assessment(
        quality_discovery.targets,
        fingerprint_quality_rules(fingerprint_profile),
        metadata={
            "operation": FEED_REGIME_OPERATION,
            "consumer": "feed_epoch_fitting",
        },
    )
    raw_evidence = tuple(
        FeedEpochEvidenceV1.from_fingerprint(payload)
        for payload in (
            finding.metadata.get(TIME_SERIES_FINGERPRINT_METADATA_KEY)
            for finding in quality_report.findings
        )
        if isinstance(payload, Mapping) and _fingerprint_is_usable_tick(payload)
    )
    evidence, evidence_preparation = _prepare_canonical_evidence(
        raw_evidence,
        requested_bucket=normalized_bucket,
    )
    effective_bucket = str(evidence_preparation["effective_bucket"])
    effective_config = fit_config
    if effective_config is None and len({item.period for item in evidence}) < 6:
        effective_config = FeedEpochFitConfigV1(
            min_evidence_periods=2,
            min_segment_periods=1,
        )
    definition = fit_feed_epochs(evidence, config=effective_config)
    profiles = tuple(
        _period_profile_from_evidence(item, bucket=effective_bucket)
        for item in evidence
    )
    regimes = _regimes_from_definition(
        definition,
        bucket=effective_bucket,
        profiles=profiles,
    )
    return FeedRegimeReport(
        discovery=discovery,
        period_profiles=profiles,
        regimes=regimes,
        epoch_definition=definition,
        metadata={
            "quiet_gap_ms": quiet_gap_ms,
            "bucket": effective_bucket,
            "requested_bucket": normalized_bucket,
            "evidence_preparation": evidence_preparation,
            "fitting_basis": "canonical_time_series_fingerprint",
            "fingerprint_target_count": len(quality_discovery.targets),
            "raw_fingerprint_evidence_count": len(raw_evidence),
            "evidence_count": len(evidence),
            "config_id": definition.config.config_id,
            "short_history_compatibility_fit": (
                fit_config is None and definition.period_count < 6
            ),
            "quality_semantics": (
                "Feed-regime analytics are descriptive feature-engineering "
                "signals and do not imply data-quality pass/fail status."
            ),
        },
    )


def feed_regime_report_to_json(report: FeedRegimeReport) -> str:
    """Return deterministic formatted JSON for a feed-regime report."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def write_feed_regime_report(
    report: FeedRegimeReport,
    path: str | Path,
) -> ArtifactRef:
    """Write a report containing its versioned epoch artifact."""
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = f"{feed_regime_report_to_json(report)}\n".encode("utf-8")
    output.write_bytes(encoded)
    definition = report.epoch_definition
    return ArtifactRef(
        kind="feed-regime-report",
        path=str(output.resolve()),
        size_bytes=len(encoded),
        sha256=hashlib.sha256(encoded).hexdigest(),
        metadata={
            "schema_version": ANALYTICS_REPORT_SCHEMA_VERSION,
            "operation": FEED_REGIME_OPERATION,
            "target_count": report.discovery.target_count,
            "profile_count": len(report.period_profiles),
            "regime_count": len(report.regimes),
            "epoch_definition_id": (
                definition.definition_id if definition is not None else None
            ),
            "stability_status": (
                definition.stability.status
                if definition is not None
                else "unavailable"
            ),
        },
    )


def format_feed_regime_console_summary(
    report: FeedRegimeReport,
    *,
    artifact: ArtifactRef | None = None,
) -> str:
    """Return a compact human-readable technological epoch summary."""
    summary = report.summary()
    lines = [
        "Feed regime analytics",
        f"targets: {summary['target_count']}",
        f"supported tick targets: {summary['supported_target_count']}",
        f"profiles: {summary['profile_count']}",
        f"regimes: {summary['regime_count']}",
        f"stability: {summary['stability_status']}",
    ]
    if artifact is not None:
        lines.append(f"report: {artifact.path}")
    if not report.period_profiles:
        lines.append("No supported ASCII tick fingerprint evidence discovered.")
        return "\n".join(lines)
    lines.extend(("", "Technological epochs"))
    for regime in report.regimes:
        lines.append(
            "- "
            f"{regime.symbol} {regime.period_start}-{regime.period_end} "
            f"{regime.label} "
            f"rate={_round_float(regime.mean_tick_rate_per_hour)}/hour "
            f"median_gap={_round_float(regime.median_interarrival_ms)}ms"
        )
    definition = report.epoch_definition
    if definition is not None and definition.boundaries:
        lines.extend(("", "Uncertain transitions"))
        for boundary in definition.boundaries:
            lines.append(
                "- "
                f"{boundary.left_period}->{boundary.right_period} "
                f"support={boundary.support} confidence={boundary.confidence} "
                f"interval={boundary.uncertainty_start_utc_ms}:"
                f"{boundary.uncertainty_end_utc_ms}"
            )
    return "\n".join(lines)


def _discovery_metadata() -> dict[str, JSONValue]:
    return {
        "operation": FEED_REGIME_OPERATION,
        "supported_timeframe": TICK,
        "quality_semantics": "analytics-only; no pass/fail status",
        "discovery_basis": "canonical_quality_discovery",
    }


def _analytics_target(target: Any) -> AnalyticsTarget:
    metadata = dict(target.metadata)
    metadata["supported_for_feed_regimes"] = (
        target.data_format == _ASCII_FORMAT and target.timeframe == TICK
    )
    metadata["discovery_basis"] = "canonical_quality_discovery"
    return AnalyticsTarget(
        path=target.path,
        kind=target.kind.value,
        data_format=target.data_format,
        timeframe=target.timeframe,
        symbol=target.symbol,
        period=target.period,
        metadata=metadata,
    )


def _fingerprint_is_usable_tick(payload: Mapping[str, Any]) -> bool:
    axis = payload.get("target_axis")
    coverage = payload.get("coverage")
    source = payload.get("source")
    if not isinstance(axis, Mapping) or not isinstance(coverage, Mapping):
        return False
    if not isinstance(source, Mapping):
        return False
    return (
        str(axis.get("data_format", "")).lower() == _ASCII_FORMAT
        and str(axis.get("timeframe", "")).upper() == TICK
        and source.get("kind") != "unavailable"
        and int(coverage.get("parsed_row_count", 0) or 0) > 0
    )


def _prepare_canonical_evidence(
    evidence: Sequence[FeedEpochEvidenceV1],
    *,
    requested_bucket: str,
) -> tuple[tuple[FeedEpochEvidenceV1, ...], dict[str, JSONValue]]:
    """Select one canonical axis and coarsen safely to a common period grid."""
    by_axis: dict[tuple[str, str], list[FeedEpochEvidenceV1]] = defaultdict(
        list
    )
    for item in evidence:
        by_axis[(item.symbol, item.period)].append(item)
    selected = tuple(
        min(items, key=_canonical_evidence_rank)
        for _, items in sorted(by_axis.items())
    )
    duplicate_axis_count = len(evidence) - len(selected)
    has_annual_evidence = any(len(item.period) == 4 for item in selected)
    effective_bucket = (
        "year" if requested_bucket == "year" or has_annual_evidence else "month"
    )
    annual_overlap_skip_count = 0
    if effective_bucket == "year":
        by_year: dict[tuple[str, str], list[FeedEpochEvidenceV1]] = defaultdict(
            list
        )
        for item in selected:
            by_year[(item.symbol, item.period[:4])].append(item)
        prepared: list[FeedEpochEvidenceV1] = []
        for (_, year), items in sorted(by_year.items()):
            annual = [item for item in items if len(item.period) == 4]
            if annual:
                chosen = min(annual, key=_canonical_evidence_rank)
                prepared.append(chosen)
                annual_overlap_skip_count += len(items) - 1
            else:
                prepared.append(_aggregate_annual_evidence(items, year=year))
        result = tuple(prepared)
    else:
        result = selected
    reason = "requested_bucket"
    if requested_bucket == "month" and has_annual_evidence:
        reason = "annual_evidence_cannot_be_safely_disaggregated"
    return result, {
        "requested_bucket": requested_bucket,
        "effective_bucket": effective_bucket,
        "effective_bucket_reason": reason,
        "raw_evidence_count": len(evidence),
        "selected_axis_count": len(selected),
        "fitted_evidence_count": len(result),
        "duplicate_axis_skip_count": duplicate_axis_count,
        "annual_overlap_skip_count": annual_overlap_skip_count,
        "duplicate_axis_policy": "prefer_direct_cache_then_sibling_cache_then_text",
        "mixed_granularity_policy": "coarsen_to_year_never_disaggregate",
        "annual_overlap_policy": "prefer_canonical_annual_evidence",
    }


def _canonical_evidence_rank(item: FeedEpochEvidenceV1) -> tuple[int, str]:
    cache_source = str(item.quality.get("cache_source", ""))
    if item.source_kind == "cache" and cache_source == "direct":
        rank = 0
    elif item.source_kind == "cache":
        rank = 1
    elif item.source_kind == "csv_text":
        rank = 2
    elif item.source_kind == "zip_member":
        rank = 3
    else:
        rank = 4
    return rank, item.evidence_id


def _aggregate_annual_evidence(
    items: Sequence[FeedEpochEvidenceV1],
    *,
    year: str,
) -> FeedEpochEvidenceV1:
    ordered = tuple(
        sorted(items, key=lambda item: (item.period, item.evidence_id))
    )
    symbol = ordered[0].symbol
    component_sources: list[JSONValue] = [
        {
            "fingerprint_id": item.fingerprint_id,
            "source_artifact_sha256": item.source_artifact_sha256,
            "source_hash_basis": item.source_hash_basis,
            "symbol": item.symbol,
            "period": item.period,
            "source_kind": item.source_kind,
        }
        for item in ordered
    ]
    feature_names = tuple(
        sorted({name for item in ordered for name in item.feature_values})
    )
    feature_values = {
        name: float(
            median(
                item.feature_values[name]
                for item in ordered
                if name in item.feature_values
            )
        )
        for name in feature_names
    }
    feature_provenance = {
        name: tuple(
            sorted(
                {
                    path
                    for item in ordered
                    for path in item.feature_provenance.get(name, ())
                }
            )
        )
        for name in feature_names
    }
    fingerprint_id = _semantic_sha256(
        {
            "kind": "canonical_fingerprint_annual_aggregate",
            "symbol": symbol,
            "period": year,
            "fingerprint_ids": [item.fingerprint_id for item in ordered],
        }
    )
    source_hash = _semantic_sha256(
        {
            "kind": "canonical_fingerprint_source_aggregate",
            "source_hashes": [item.source_artifact_sha256 for item in ordered],
        }
    )
    return FeedEpochEvidenceV1(
        symbol=symbol,
        period=year,
        start_timestamp_utc_ms=min(
            item.start_timestamp_utc_ms for item in ordered
        ),
        end_timestamp_utc_ms=max(item.end_timestamp_utc_ms for item in ordered),
        fingerprint_id=fingerprint_id,
        source_artifact_sha256=source_hash,
        source_hash_basis="canonical_fingerprint_aggregate_id",
        source_kind="canonical_fingerprint_aggregate",
        feature_values=feature_values,
        feature_provenance=feature_provenance,
        conditioning=_aggregate_conditioning(ordered),
        quality={
            "aggregation": "median_monthly_canonical_fingerprints",
            "component_sources": component_sources,
            "component_count": len(ordered),
            "cache_source": "mixed",
            "sequence_status": "derived",
            "limitations": ["annual_values_are_monthly_fingerprint_aggregates"],
        },
        profile=_aggregate_profile(ordered),
    )


def _aggregate_conditioning(
    items: Sequence[FeedEpochEvidenceV1],
) -> dict[str, JSONValue]:
    count_fields = (
        "session_state_counts",
        "active_session_counts",
        "special_tag_counts",
        "holiday_tag_counts",
        "event_tag_counts",
    )
    result: dict[str, JSONValue] = {
        name: _sum_count_maps(item.conditioning.get(name) for item in items)
        for name in count_fields
    }
    statuses = sorted(
        {
            str(item.conditioning.get("calendar_status", "unavailable"))
            for item in items
        }
    )
    status_values: list[JSONValue] = []
    status_values.extend(statuses)
    result.update(
        {
            "calendar_status": statuses[0] if len(statuses) == 1 else "mixed",
            "calendar_statuses": status_values,
            "aggregation": "sum_counts_across_canonical_fingerprints",
            "conditioning_payload_sha256": _semantic_sha256(
                {
                    "conditioning_hashes": [
                        item.conditioning.get("conditioning_payload_sha256")
                        for item in items
                    ]
                }
            ),
        }
    )
    return result


def _aggregate_profile(
    items: Sequence[FeedEpochEvidenceV1],
) -> dict[str, JSONValue]:
    row_count = sum(_profile_int(item, "row_count") for item in items)
    quote_update_count = sum(
        _profile_int(item, "quote_update_count") for item in items
    )
    interval_count = sum(
        max(0, _profile_int(item, "row_count") - 1) for item in items
    )
    durations = sum(
        max(0, item.end_timestamp_utc_ms - item.start_timestamp_utc_ms)
        for item in items
    )
    weighted_spread_total = sum(
        _profile_float(item, "spread_mean") * _profile_int(item, "row_count")
        for item in items
    )
    return {
        "row_count": row_count,
        "tick_rate_per_hour": (
            row_count * 3_600_000.0 / durations if durations else 0.0
        ),
        "median_interarrival_ms": _median_profile(
            items, "median_interarrival_ms"
        ),
        "p95_interarrival_ms": _median_profile(items, "p95_interarrival_ms"),
        "max_interarrival_ms": max(
            (_profile_int(item, "max_interarrival_ms") for item in items),
            default=0,
        ),
        "quiet_gap_count": sum(
            _profile_int(item, "quiet_gap_count") for item in items
        ),
        "quote_update_count": quote_update_count,
        "quote_update_ratio": (
            quote_update_count / interval_count if interval_count else 0.0
        ),
        "zero_change_run_count": sum(
            _profile_int(item, "zero_change_run_count") for item in items
        ),
        "zero_change_tick_count": sum(
            _profile_int(item, "zero_change_tick_count") for item in items
        ),
        "spread_min": min(
            (_profile_float(item, "spread_min") for item in items),
            default=0.0,
        ),
        "spread_median": _median_profile(items, "spread_median"),
        "spread_mean": weighted_spread_total / row_count if row_count else 0.0,
        "spread_max": max(
            (_profile_float(item, "spread_max") for item in items),
            default=0.0,
        ),
        "session_counts": _sum_count_maps(
            item.profile.get("session_counts") for item in items
        ),
    }


def _sum_count_maps(values: Iterable[Any]) -> dict[str, JSONValue]:
    counts: Counter[str] = Counter()
    for value in values:
        if not isinstance(value, Mapping):
            continue
        for name, count in value.items():
            try:
                counts[str(name)] += max(0, int(count))
            except (TypeError, ValueError, OverflowError):
                continue
    result: dict[str, JSONValue] = {}
    for name, count in sorted(counts.items()):
        result[name] = int(count)
    return result


def _profile_int(item: FeedEpochEvidenceV1, name: str) -> int:
    return max(0, int(_json_float(item.profile.get(name), default=0.0)))


def _profile_float(item: FeedEpochEvidenceV1, name: str) -> float:
    return _json_float(item.profile.get(name), default=0.0)


def _json_float(value: JSONValue, *, default: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        return default
    try:
        return float(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _median_profile(items: Sequence[FeedEpochEvidenceV1], name: str) -> float:
    return float(median(_profile_float(item, name) for item in items))


def _semantic_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _period_profile_from_evidence(
    evidence: FeedEpochEvidenceV1,
    *,
    bucket: str,
) -> FeedPeriodProfile:
    profile = evidence.profile
    session_counts = profile.get("session_counts", {})
    if not isinstance(session_counts, Mapping):
        session_counts = {}
    return FeedPeriodProfile(
        symbol=evidence.symbol,
        period=evidence.period,
        bucket=bucket,
        row_count=_profile_int(evidence, "row_count"),
        start_utc_ms=evidence.start_timestamp_utc_ms,
        end_utc_ms=evidence.end_timestamp_utc_ms,
        tick_rate_per_hour=_profile_float(evidence, "tick_rate_per_hour"),
        median_interarrival_ms=_profile_float(
            evidence, "median_interarrival_ms"
        ),
        p95_interarrival_ms=_profile_float(evidence, "p95_interarrival_ms"),
        max_interarrival_ms=int(
            _profile_float(evidence, "max_interarrival_ms")
        ),
        quiet_gap_count=_profile_int(evidence, "quiet_gap_count"),
        quote_update_count=_profile_int(evidence, "quote_update_count"),
        quote_update_ratio=_profile_float(evidence, "quote_update_ratio"),
        zero_change_run_count=_profile_int(evidence, "zero_change_run_count"),
        zero_change_tick_count=_profile_int(evidence, "zero_change_tick_count"),
        spread_min=_profile_float(evidence, "spread_min"),
        spread_median=_profile_float(evidence, "spread_median"),
        spread_mean=_profile_float(evidence, "spread_mean"),
        spread_max=_profile_float(evidence, "spread_max"),
        session_counts={
            str(name): max(0, int(_json_float(value, default=0.0)))
            for name, value in session_counts.items()
        },
    )


def _regimes_from_definition(
    definition: FeedEpochDefinitionV1,
    *,
    bucket: str,
    profiles: Sequence[FeedPeriodProfile],
) -> tuple[FeedRegimeEra, ...]:
    result: list[FeedRegimeEra] = []
    for epoch in definition.epochs:
        members = tuple(
            profile
            for profile in profiles
            if epoch.period_start <= profile.period <= epoch.period_end
        )
        rates = [profile.tick_rate_per_hour for profile in members]
        gaps = [profile.median_interarrival_ms for profile in members]
        updates = [profile.quote_update_ratio for profile in members]
        result.append(
            FeedRegimeEra(
                symbol="+".join(definition.symbols),
                label=epoch.label,
                bucket=bucket,
                period_start=epoch.period_start,
                period_end=epoch.period_end,
                start_utc_ms=epoch.start_timestamp_utc_ms,
                end_utc_ms=epoch.end_timestamp_utc_ms,
                profile_count=len(members),
                row_count=sum(profile.row_count for profile in members),
                mean_tick_rate_per_hour=(
                    sum(rates) / len(rates) if rates else 0.0
                ),
                median_interarrival_ms=(float(median(gaps)) if gaps else 0.0),
                quote_update_ratio=(
                    sum(updates) / len(updates) if updates else 0.0
                ),
                quiet_gap_count=sum(
                    profile.quiet_gap_count for profile in members
                ),
                metadata={
                    "epoch_id": epoch.epoch_id,
                    "definition_id": definition.definition_id,
                    "stability_status": definition.stability.status,
                    "transition_intervals_are_separate": True,
                },
            )
        )
    return tuple(result)


def _normalize_bucket(bucket: str) -> str:
    normalized = str(bucket or "").strip().lower()
    if normalized not in {"month", "year"}:
        raise ValueError("feed-regime bucket must be 'month' or 'year'")
    return normalized


def _round_float(value: float, digits: int = 6) -> float:
    rounded = round(float(value), digits)
    return 0.0 if rounded == 0.0 else rounded
