"""Active-time multivariate technological feed-epoch fitting.

Version two deliberately lives beside :mod:`feed_epochs`.  Version-one
artifacts retain their original adjacent-period semantics; this module fits a
panel of real ASCII tick caches with explicit time denominators, a robust
multivariate PELT objective, and bounded sensitivity evidence.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from statistics import median
from typing import Any

from histdatacom.data_quality.calendar import (
    FX_CLOSE_OPEN_MINUTE,
    SESSION_WINDOWS,
    calendar_policy_metadata,
)
from histdatacom.data_quality.contracts import QualityTargetKind
from histdatacom.data_quality.discovery import discover_quality_targets
from histdatacom.histdata_ascii import EST_NO_DST_OFFSET_MS
from histdatacom.resource_usage import peak_rss_bytes
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

FEED_EPOCH_EVIDENCE_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-evidence.v2"
FEED_EPOCH_FIT_CONFIG_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-fit-config.v2"
FEED_EPOCH_BOUNDARY_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-boundary.v2"
FEED_EPOCH_INTERVAL_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-interval.v2"
FEED_EPOCH_STABILITY_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-stability.v2"
FEED_EPOCH_DEFINITION_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-definition.v2"
FEED_EPOCH_ASSIGNMENT_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-assignment.v2"
FEED_EPOCH_CAMPAIGN_V2_SCHEMA_VERSION = "histdatacom.feed-epoch-campaign.v2"

CALENDAR_POLICY_VERSION = "histdatacom.fx-active-time.v1"
HOUR_MS = 3_600_000
MINUTE_MS = 60_000
SOURCE_OFFSET_MS = -EST_NO_DST_OFFSET_MS
MAX_FEATURES = 64
MAX_EVIDENCE = 4096
MAX_ACTIVITY_BINS = 800
SESSION_ACTIVITY_KEYS = tuple(window.name for window in SESSION_WINDOWS) + (
    "off_session",
)
DAY_ACTIVITY_KEYS = tuple(str(value) for value in range(7))

DEFAULT_ACTIVE_TIME_FEATURES = (
    "log_calendar_tick_rate_per_hour",
    "log_market_open_tick_rate_per_hour",
    "log_active_window_tick_rate_per_hour",
    "bid_only_rate",
    "ask_only_rate",
    "joint_move_rate",
    "unchanged_rate",
    "subwindow_count_fano",
    "log_interarrival_median_ms",
    "interarrival_dispersion",
    "interarrival_lag1",
    "timestamp_exact_second_rate",
    "timestamp_last_digit_entropy",
    "price_precision_digits",
    "duplicate_timestamp_rate",
    "burst_interval_rate",
    "stale_quote_rate",
    "log_stale_run_p95",
    "log_stale_run_max",
    "log_spread_median",
    "spread_tail_ratio",
    "spread_jump_rate",
    "session_activity_dispersion",
    "day_activity_dispersion",
    *(f"session_activity_share_{name}" for name in SESSION_ACTIVITY_KEYS),
    *(f"day_activity_share_{name}" for name in DAY_ACTIVITY_KEYS),
    "cross_symbol_activity_correlation",
    "cross_symbol_active_bin_overlap",
)


@dataclass(frozen=True, slots=True)
class FeedEpochEvidenceV2:
    """One bounded monthly observation of a feed's delivery technology."""

    symbol: str
    period: str
    source_path: str
    source_artifact_sha256: str
    source_size_bytes: int
    start_timestamp_utc_ms: int
    end_timestamp_utc_ms: int
    row_count: int
    denominators_ms: Mapping[str, int]
    counts: Mapping[str, int]
    feature_values: Mapping[str, float]
    feature_provenance: Mapping[str, tuple[str, ...]]
    activity_bin_counts: Mapping[str, int]
    calendar_policy: Mapping[str, JSONValue]
    limitations: tuple[str, ...] = ()
    evidence_id: str = ""
    schema_version: str = FEED_EPOCH_EVIDENCE_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_EVIDENCE_V2_SCHEMA_VERSION:
            raise ValueError("unsupported v2 feed epoch evidence schema")
        symbol = _required_text(self.symbol, "symbol").upper()
        if not _valid_month(self.period):
            raise ValueError("v2 feed epoch evidence requires YYYYMM periods")
        source_hash = _sha256_id(
            self.source_artifact_sha256, "source_artifact_sha256"
        )
        row_count = _bounded_int(self.row_count, "row_count", 2, 2**63 - 1)
        start = _strict_int(self.start_timestamp_utc_ms, "start timestamp")
        end = _strict_int(self.end_timestamp_utc_ms, "end timestamp")
        if end < start:
            raise ValueError("evidence end precedes start")
        denominators = {
            _required_text(name, "denominator"): _bounded_int(
                value, f"denominator {name}", 1, 2**63 - 1
            )
            for name, value in sorted(self.denominators_ms.items())
        }
        required_denominators = {
            "calendar_duration_ms",
            "market_open_duration_ms",
            "active_window_duration_ms",
        }
        if not required_denominators.issubset(denominators):
            raise ValueError(
                "v2 evidence is missing an active-time denominator"
            )
        counts = {
            _required_text(name, "count"): _bounded_int(
                value, f"count {name}", 0, 2**63 - 1
            )
            for name, value in sorted(self.counts.items())
        }
        features = {
            _required_text(name, "feature"): _finite_float(value, name)
            for name, value in sorted(self.feature_values.items())
        }
        if not features or len(features) > MAX_FEATURES:
            raise ValueError(
                "v2 evidence features must be non-empty and bounded"
            )
        required_numerators = {
            "log_market_open_tick_rate_per_hour": "market_open_row_count",
            "log_active_window_tick_rate_per_hour": (
                "active_window_interval_count"
            ),
        }
        for feature, count in required_numerators.items():
            if feature in features and count not in counts:
                raise ValueError(
                    f"v2 evidence is missing rate numerator {count}"
                )
        if counts.get("market_open_row_count", 0) > row_count:
            raise ValueError("market-open row count exceeds evidence row count")
        if counts.get("active_window_interval_count", 0) > row_count - 1:
            raise ValueError(
                "active-window interval count exceeds evidence transitions"
            )
        provenance = {
            name: tuple(
                _required_text(path, "feature provenance")
                for path in self.feature_provenance.get(name, ())
            )
            for name in features
        }
        if any(not paths for paths in provenance.values()):
            raise ValueError("every v2 feature requires provenance")
        bins = {
            str(_integer_text(key, "activity bin")): _bounded_int(
                value, "activity bin count", 1, 2**63 - 1
            )
            for key, value in sorted(
                self.activity_bin_counts.items(), key=lambda item: int(item[0])
            )
        }
        if len(bins) > MAX_ACTIVITY_BINS:
            raise ValueError("activity-bin evidence exceeds monthly bound")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(
            self, "source_path", _required_text(self.source_path, "source_path")
        )
        object.__setattr__(self, "source_artifact_sha256", source_hash)
        object.__setattr__(
            self,
            "source_size_bytes",
            _bounded_int(
                self.source_size_bytes, "source_size_bytes", 1, 2**63 - 1
            ),
        )
        object.__setattr__(self, "row_count", row_count)
        object.__setattr__(self, "start_timestamp_utc_ms", start)
        object.__setattr__(self, "end_timestamp_utc_ms", end)
        object.__setattr__(self, "denominators_ms", denominators)
        object.__setattr__(self, "counts", counts)
        object.__setattr__(self, "feature_values", features)
        object.__setattr__(self, "feature_provenance", provenance)
        object.__setattr__(self, "activity_bin_counts", bins)
        object.__setattr__(self, "calendar_policy", dict(self.calendar_policy))
        object.__setattr__(
            self,
            "limitations",
            tuple(dict.fromkeys(str(value) for value in self.limitations)),
        )
        expected = _stable_id("feed-epoch-evidence-v2", self.identity_payload())
        if self.evidence_id and self.evidence_id != expected:
            raise ValueError(
                "evidence_id does not match deterministic identity"
            )
        object.__setattr__(self, "evidence_id", expected)

    @property
    def profile(self) -> Mapping[str, JSONValue]:
        """Expose a bounded compatibility profile for observation fitting."""
        median_interval = math.expm1(
            self.feature_values.get("log_interarrival_median_ms", 0.0)
        )
        return {
            "row_count": self.row_count,
            "tick_rate_per_hour": math.expm1(
                self.feature_values.get(
                    "log_active_window_tick_rate_per_hour", 0.0
                )
            ),
            "median_interarrival_ms": median_interval,
            "p95_interarrival_ms": median_interval
            * self.feature_values.get("interarrival_dispersion", 1.0),
            "calendar_policy_version": CALENDAR_POLICY_VERSION,
        }

    @property
    def source_hash_basis(self) -> str:
        """Return the provenance vocabulary consumed by observation fitting."""
        return "cache_content_sha256"

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return all semantic fields used by the evidence ID."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "period": self.period,
            "source_artifact_sha256": self.source_artifact_sha256,
            "source_size_bytes": self.source_size_bytes,
            "start_timestamp_utc_ms": self.start_timestamp_utc_ms,
            "end_timestamp_utc_ms": self.end_timestamp_utc_ms,
            "row_count": self.row_count,
            "denominators_ms": dict(self.denominators_ms),
            "counts": dict(self.counts),
            "feature_values": dict(self.feature_values),
            "feature_provenance": {
                name: list(paths)
                for name, paths in sorted(self.feature_provenance.items())
            },
            "activity_bin_counts": dict(self.activity_bin_counts),
            "calendar_policy": dict(self.calendar_policy),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible evidence."""
        return {
            **self.identity_payload(),
            "source_path": self.source_path,
            "evidence_id": self.evidence_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochEvidenceV2":
        """Read strict version-two evidence."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            source_path=str(data.get("source_path", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            source_size_bytes=_strict_int(
                data.get("source_size_bytes"), "source_size_bytes"
            ),
            start_timestamp_utc_ms=_strict_int(
                data.get("start_timestamp_utc_ms"), "start timestamp"
            ),
            end_timestamp_utc_ms=_strict_int(
                data.get("end_timestamp_utc_ms"), "end timestamp"
            ),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            denominators_ms=_int_mapping(data.get("denominators_ms")),
            counts=_int_mapping(data.get("counts")),
            feature_values=_float_mapping(data.get("feature_values")),
            feature_provenance={
                str(name): tuple(str(value) for value in _sequence(paths))
                for name, paths in _mapping(
                    data.get("feature_provenance")
                ).items()
            },
            activity_bin_counts=_int_mapping(data.get("activity_bin_counts")),
            calendar_policy=_mapping(data.get("calendar_policy")),
            limitations=tuple(
                str(value) for value in _sequence(data.get("limitations"))
            ),
            evidence_id=str(data.get("evidence_id", "")),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochFitConfigV2:
    """Explicit robust PELT and sensitivity policy."""

    feature_names: tuple[str, ...] = DEFAULT_ACTIVE_TIME_FEATURES
    min_evidence_periods: int = 24
    min_segment_periods: int = 6
    min_feature_coverage: float = 0.80
    min_symbol_count: int = 3
    penalty_multiplier: float = 24.0
    robust_clip: float = 6.0
    min_boundary_support: float = 0.60
    boundary_match_tolerance_periods: int = 2
    sensitivity_penalty_multipliers: tuple[float, ...] = (0.75, 1.25)
    active_gap_cap_ms: int = 60_000
    burst_interval_ms: int = 100
    activity_bin_ms: int = HOUR_MS
    max_evidence: int = MAX_EVIDENCE
    max_sensitivity_runs: int = 128
    rounding_digits: int = 10
    config_id: str = ""
    schema_version: str = FEED_EPOCH_FIT_CONFIG_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_FIT_CONFIG_V2_SCHEMA_VERSION:
            raise ValueError("unsupported v2 feed epoch fit config schema")
        names = tuple(dict.fromkeys(str(name) for name in self.feature_names))
        unknown = sorted(set(names).difference(DEFAULT_ACTIVE_TIME_FEATURES))
        if not names or len(names) > MAX_FEATURES or unknown:
            raise ValueError(
                "unsupported v2 feature selection"
                + (": " + ", ".join(unknown) if unknown else "")
            )
        min_periods = _bounded_int(
            self.min_evidence_periods, "min_evidence_periods", 4, MAX_EVIDENCE
        )
        min_segment = _bounded_int(
            self.min_segment_periods, "min_segment_periods", 2, MAX_EVIDENCE
        )
        if min_segment * 2 > min_periods:
            raise ValueError("minimum segment exceeds half minimum evidence")
        object.__setattr__(self, "feature_names", names)
        object.__setattr__(self, "min_evidence_periods", min_periods)
        object.__setattr__(self, "min_segment_periods", min_segment)
        object.__setattr__(
            self,
            "min_feature_coverage",
            _bounded_float(self.min_feature_coverage, "coverage", 0.0, 1.0),
        )
        object.__setattr__(
            self,
            "min_boundary_support",
            _bounded_float(self.min_boundary_support, "support", 0.0, 1.0),
        )
        _bounded_float(self.penalty_multiplier, "penalty", 0.0001, 1_000.0)
        _bounded_float(self.robust_clip, "robust_clip", 0.1, 100.0)
        penalties = tuple(
            _bounded_float(value, "sensitivity penalty", 0.01, 100.0)
            for value in self.sensitivity_penalty_multipliers
        )
        object.__setattr__(self, "sensitivity_penalty_multipliers", penalties)
        _bounded_int(self.min_symbol_count, "min_symbol_count", 2, 32)
        _bounded_int(
            self.boundary_match_tolerance_periods,
            "boundary_match_tolerance_periods",
            0,
            24,
        )
        _bounded_int(self.active_gap_cap_ms, "active_gap_cap_ms", 1, 86_400_000)
        _bounded_int(self.burst_interval_ms, "burst_interval_ms", 1, 60_000)
        _bounded_int(
            self.activity_bin_ms, "activity_bin_ms", MINUTE_MS, 86_400_000
        )
        max_evidence = _bounded_int(
            self.max_evidence, "max_evidence", min_periods, MAX_EVIDENCE
        )
        if max_evidence < min_periods * self.min_symbol_count:
            raise ValueError("max_evidence cannot hold minimum panel support")
        _bounded_int(self.max_sensitivity_runs, "max_sensitivity_runs", 1, 4096)
        _bounded_int(self.rounding_digits, "rounding_digits", 1, 15)
        payload = self.identity_payload(include_id=False)
        expected = _stable_id("feed-epoch-fit-config-v2", payload)
        if self.config_id and self.config_id != expected:
            raise ValueError("config_id does not match deterministic identity")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(
        self, *, include_id: bool = True
    ) -> dict[str, JSONValue]:
        """Return deterministic config metadata."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "feature_names": list(self.feature_names),
            "min_evidence_periods": self.min_evidence_periods,
            "min_segment_periods": self.min_segment_periods,
            "min_feature_coverage": self.min_feature_coverage,
            "min_symbol_count": self.min_symbol_count,
            "penalty_multiplier": self.penalty_multiplier,
            "robust_clip": self.robust_clip,
            "min_boundary_support": self.min_boundary_support,
            "boundary_match_tolerance_periods": (
                self.boundary_match_tolerance_periods
            ),
            "sensitivity_penalty_multipliers": list(
                self.sensitivity_penalty_multipliers
            ),
            "active_gap_cap_ms": self.active_gap_cap_ms,
            "burst_interval_ms": self.burst_interval_ms,
            "activity_bin_ms": self.activity_bin_ms,
            "max_evidence": self.max_evidence,
            "max_sensitivity_runs": self.max_sensitivity_runs,
            "rounding_digits": self.rounding_digits,
        }
        if include_id:
            payload["config_id"] = self.config_id
        return payload

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic config metadata."""
        return self.identity_payload()

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochFitConfigV2":
        """Read a strict version-two fit config."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            feature_names=tuple(
                str(value) for value in _sequence(data.get("feature_names"))
            ),
            min_evidence_periods=_strict_int(
                data.get("min_evidence_periods"), "min_evidence_periods"
            ),
            min_segment_periods=_strict_int(
                data.get("min_segment_periods"), "min_segment_periods"
            ),
            min_feature_coverage=_finite_float(
                data.get("min_feature_coverage"), "min_feature_coverage"
            ),
            min_symbol_count=_strict_int(
                data.get("min_symbol_count"), "min_symbol_count"
            ),
            penalty_multiplier=_finite_float(
                data.get("penalty_multiplier"), "penalty_multiplier"
            ),
            robust_clip=_finite_float(data.get("robust_clip"), "robust_clip"),
            min_boundary_support=_finite_float(
                data.get("min_boundary_support"), "min_boundary_support"
            ),
            boundary_match_tolerance_periods=_strict_int(
                data.get("boundary_match_tolerance_periods"),
                "boundary_match_tolerance_periods",
            ),
            sensitivity_penalty_multipliers=tuple(
                _finite_float(value, "sensitivity penalty")
                for value in _sequence(
                    data.get("sensitivity_penalty_multipliers")
                )
            ),
            active_gap_cap_ms=_strict_int(
                data.get("active_gap_cap_ms"), "active_gap_cap_ms"
            ),
            burst_interval_ms=_strict_int(
                data.get("burst_interval_ms"), "burst_interval_ms"
            ),
            activity_bin_ms=_strict_int(
                data.get("activity_bin_ms"), "activity_bin_ms"
            ),
            max_evidence=_strict_int(data.get("max_evidence"), "max_evidence"),
            max_sensitivity_runs=_strict_int(
                data.get("max_sensitivity_runs"), "max_sensitivity_runs"
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            config_id=str(data.get("config_id", "")),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochBoundaryV2:
    """A shared panel boundary with sensitivity-derived uncertainty."""

    left_period: str
    right_period: str
    central_timestamp_utc_ms: int
    support: float
    uncertainty_start_period: str
    uncertainty_end_period: str
    objective_gain: float
    supporting_features: tuple[str, ...]
    boundary_id: str = ""
    schema_version: str = FEED_EPOCH_BOUNDARY_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_BOUNDARY_V2_SCHEMA_VERSION:
            raise ValueError("unsupported v2 feed epoch boundary schema")
        for value in (
            self.left_period,
            self.right_period,
            self.uncertainty_start_period,
            self.uncertainty_end_period,
        ):
            if not _valid_month(value):
                raise ValueError("v2 boundary periods must use YYYYMM")
        _bounded_float(self.support, "boundary support", 0.0, 1.0)
        _bounded_float(self.objective_gain, "objective gain", 0.0, math.inf)
        expected = _stable_id("feed-epoch-boundary-v2", self.identity_payload())
        if self.boundary_id and self.boundary_id != expected:
            raise ValueError(
                "boundary_id does not match deterministic identity"
            )
        object.__setattr__(self, "boundary_id", expected)

    @property
    def transition_label(self) -> str:
        """Return the stable transition label used by observation models."""
        return f"transition:{self.left_period}-{self.right_period}"

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic boundary metadata."""
        return {
            **self.identity_payload(),
            "transition_label": self.transition_label,
            "boundary_id": self.boundary_id,
        }

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic boundary fields without the derived ID."""
        return {
            "schema_version": self.schema_version,
            "left_period": self.left_period,
            "right_period": self.right_period,
            "central_timestamp_utc_ms": self.central_timestamp_utc_ms,
            "support": self.support,
            "uncertainty_start_period": self.uncertainty_start_period,
            "uncertainty_end_period": self.uncertainty_end_period,
            "objective_gain": self.objective_gain,
            "supporting_features": list(self.supporting_features),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochBoundaryV2":
        """Read a strict version-two boundary."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            left_period=str(data.get("left_period", "")),
            right_period=str(data.get("right_period", "")),
            central_timestamp_utc_ms=_strict_int(
                data.get("central_timestamp_utc_ms"), "boundary timestamp"
            ),
            support=_finite_float(data.get("support"), "boundary support"),
            uncertainty_start_period=str(
                data.get("uncertainty_start_period", "")
            ),
            uncertainty_end_period=str(data.get("uncertainty_end_period", "")),
            objective_gain=_finite_float(
                data.get("objective_gain"), "objective gain"
            ),
            supporting_features=tuple(
                str(value)
                for value in _sequence(data.get("supporting_features"))
            ),
            boundary_id=str(data.get("boundary_id", "")),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochIntervalV2:
    """One stable shared technology epoch."""

    label: str
    period_start: str
    period_end: str
    start_timestamp_utc_ms: int
    end_timestamp_utc_ms: int
    evidence_count: int
    feature_medians: Mapping[str, float]
    epoch_id: str = ""
    schema_version: str = FEED_EPOCH_INTERVAL_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_INTERVAL_V2_SCHEMA_VERSION:
            raise ValueError("unsupported v2 feed epoch interval schema")
        if not _valid_month(self.period_start) or not _valid_month(
            self.period_end
        ):
            raise ValueError("v2 interval periods must use YYYYMM")
        expected = _stable_id("feed-epoch-interval-v2", self.identity_payload())
        if self.epoch_id and self.epoch_id != expected:
            raise ValueError("epoch_id does not match deterministic identity")
        object.__setattr__(self, "epoch_id", expected)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic interval metadata."""
        return {**self.identity_payload(), "epoch_id": self.epoch_id}

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic interval fields without the derived ID."""
        return {
            "schema_version": self.schema_version,
            "label": self.label,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "start_timestamp_utc_ms": self.start_timestamp_utc_ms,
            "end_timestamp_utc_ms": self.end_timestamp_utc_ms,
            "evidence_count": self.evidence_count,
            "feature_medians": dict(self.feature_medians),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochIntervalV2":
        """Read a strict version-two interval."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            label=str(data.get("label", "")),
            period_start=str(data.get("period_start", "")),
            period_end=str(data.get("period_end", "")),
            start_timestamp_utc_ms=_strict_int(
                data.get("start_timestamp_utc_ms"), "interval start"
            ),
            end_timestamp_utc_ms=_strict_int(
                data.get("end_timestamp_utc_ms"), "interval end"
            ),
            evidence_count=_strict_int(
                data.get("evidence_count"), "interval evidence_count"
            ),
            feature_medians=_float_mapping(data.get("feature_medians")),
            epoch_id=str(data.get("epoch_id", "")),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochSymbolDeviationV2:
    """A symbol-specific boundary not supported by the shared panel."""

    symbol: str
    left_period: str
    right_period: str
    nearest_global_distance_periods: int | None

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic deviation metadata."""
        return {
            "symbol": self.symbol,
            "left_period": self.left_period,
            "right_period": self.right_period,
            "nearest_global_distance_periods": (
                self.nearest_global_distance_periods
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochSymbolDeviationV2":
        """Read one symbol-specific deviation."""
        distance = data.get("nearest_global_distance_periods")
        return cls(
            symbol=str(data.get("symbol", "")),
            left_period=str(data.get("left_period", "")),
            right_period=str(data.get("right_period", "")),
            nearest_global_distance_periods=(
                _strict_int(distance, "nearest boundary distance")
                if distance is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochStabilityV2:
    """Bounded sensitivity and support result."""

    status: str
    reasons: tuple[str, ...]
    run_count: int
    run_counts: Mapping[str, int]
    boundary_support: Mapping[str, float]
    boundary_support_by_family: Mapping[str, Mapping[str, float]]
    rejected_candidates: Mapping[str, Mapping[str, JSONValue]]
    feature_coverage: Mapping[str, float]
    common_period_count: int
    symbol_count: int
    schema_version: str = FEED_EPOCH_STABILITY_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_STABILITY_V2_SCHEMA_VERSION:
            raise ValueError("unsupported v2 feed epoch stability schema")
        if self.status not in {"pass", "fail"}:
            raise ValueError("unsupported v2 feed epoch stability status")
        if sum(self.run_counts.values()) != self.run_count:
            raise ValueError("v2 stability run counts do not reconcile")

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic stability metadata."""
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "reasons": list(self.reasons),
            "run_count": self.run_count,
            "run_counts": dict(self.run_counts),
            "boundary_support": dict(self.boundary_support),
            "boundary_support_by_family": {
                period: dict(values)
                for period, values in sorted(
                    self.boundary_support_by_family.items()
                )
            },
            "rejected_candidates": {
                period: dict(values)
                for period, values in sorted(self.rejected_candidates.items())
            },
            "feature_coverage": dict(self.feature_coverage),
            "common_period_count": self.common_period_count,
            "symbol_count": self.symbol_count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochStabilityV2":
        """Read strict version-two stability evidence."""
        family = _mapping(data.get("boundary_support_by_family"))
        return cls(
            schema_version=str(data.get("schema_version", "")),
            status=str(data.get("status", "")),
            reasons=tuple(
                str(value) for value in _sequence(data.get("reasons"))
            ),
            run_count=_strict_int(data.get("run_count"), "run_count"),
            run_counts=_int_mapping(data.get("run_counts")),
            boundary_support=_float_mapping(data.get("boundary_support")),
            boundary_support_by_family={
                str(period): _float_mapping(values)
                for period, values in family.items()
            },
            rejected_candidates={
                str(period): _mapping(values)
                for period, values in _mapping(
                    data.get("rejected_candidates")
                ).items()
            },
            feature_coverage=_float_mapping(data.get("feature_coverage")),
            common_period_count=_strict_int(
                data.get("common_period_count"), "common_period_count"
            ),
            symbol_count=_strict_int(data.get("symbol_count"), "symbol_count"),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochAssignmentV2:
    """Assignment compatible with the downstream observation hierarchy."""

    definition_id: str
    symbol: str
    timestamp_utc_ms: int
    assignment_kind: str
    label: str
    epoch_id: str | None = None
    boundary_id: str | None = None
    schema_version: str = FEED_EPOCH_ASSIGNMENT_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_ASSIGNMENT_V2_SCHEMA_VERSION:
            raise ValueError("unsupported v2 feed epoch assignment schema")
        if self.assignment_kind not in {"epoch", "transition", "out_of_scope"}:
            raise ValueError("unsupported v2 feed epoch assignment kind")


@dataclass(frozen=True, slots=True)
class FeedEpochDefinitionV2:
    """Versioned shared-epoch definition fitted from a tick panel."""

    config: FeedEpochFitConfigV2
    symbols: tuple[str, ...]
    coverage_start_utc_ms: int
    coverage_end_utc_ms: int
    evidence_count: int
    period_count: int
    feature_names: tuple[str, ...]
    boundaries: tuple[FeedEpochBoundaryV2, ...]
    epochs: tuple[FeedEpochIntervalV2, ...]
    symbol_deviations: tuple[FeedEpochSymbolDeviationV2, ...]
    stability: FeedEpochStabilityV2
    lineage: Mapping[str, JSONValue]
    definition_id: str = ""
    schema_version: str = FEED_EPOCH_DEFINITION_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_DEFINITION_V2_SCHEMA_VERSION:
            raise ValueError("unsupported v2 feed epoch definition schema")
        if len(self.epochs) != len(self.boundaries) + 1:
            raise ValueError(
                "v2 definitions require one more epoch than boundary"
            )
        expected = _stable_id(
            "feed-epoch-definition-v2", self.identity_payload()
        )
        if self.definition_id and self.definition_id != expected:
            raise ValueError(
                "definition_id does not match deterministic identity"
            )
        object.__setattr__(self, "definition_id", expected)

    @property
    def valid_for_observation_models(self) -> bool:
        """Return whether observation operators may consume this definition."""
        return self.stability.status == "pass"

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return all semantic definition fields."""
        return {
            "schema_version": self.schema_version,
            "config": self.config.to_dict(),
            "symbols": list(self.symbols),
            "coverage_start_utc_ms": self.coverage_start_utc_ms,
            "coverage_end_utc_ms": self.coverage_end_utc_ms,
            "evidence_count": self.evidence_count,
            "period_count": self.period_count,
            "feature_names": list(self.feature_names),
            "boundaries": [item.to_dict() for item in self.boundaries],
            "epochs": [item.to_dict() for item in self.epochs],
            "symbol_deviations": [
                item.to_dict() for item in self.symbol_deviations
            ],
            "stability": self.stability.to_dict(),
            "lineage": dict(self.lineage),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible definition metadata."""
        return {
            **self.identity_payload(),
            "definition_id": self.definition_id,
            "valid_for_observation_models": self.valid_for_observation_models,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochDefinitionV2":
        """Read a strict version-two definition while leaving v1 untouched."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            config=FeedEpochFitConfigV2.from_dict(_mapping(data.get("config"))),
            symbols=tuple(
                str(value) for value in _sequence(data.get("symbols"))
            ),
            coverage_start_utc_ms=_strict_int(
                data.get("coverage_start_utc_ms"), "coverage start"
            ),
            coverage_end_utc_ms=_strict_int(
                data.get("coverage_end_utc_ms"), "coverage end"
            ),
            evidence_count=_strict_int(
                data.get("evidence_count"), "evidence_count"
            ),
            period_count=_strict_int(data.get("period_count"), "period_count"),
            feature_names=tuple(
                str(value) for value in _sequence(data.get("feature_names"))
            ),
            boundaries=tuple(
                FeedEpochBoundaryV2.from_dict(_mapping(value))
                for value in _sequence(data.get("boundaries"))
            ),
            epochs=tuple(
                FeedEpochIntervalV2.from_dict(_mapping(value))
                for value in _sequence(data.get("epochs"))
            ),
            symbol_deviations=tuple(
                FeedEpochSymbolDeviationV2.from_dict(_mapping(value))
                for value in _sequence(data.get("symbol_deviations"))
            ),
            stability=FeedEpochStabilityV2.from_dict(
                _mapping(data.get("stability"))
            ),
            lineage=_mapping(data.get("lineage")),
            definition_id=str(data.get("definition_id", "")),
        )

    def assign(
        self, *, symbol: str, timestamp_utc_ms: int
    ) -> FeedEpochAssignmentV2:
        """Assign an event without collapsing uncertainty windows."""
        normalized = symbol.upper()
        timestamp = int(timestamp_utc_ms)
        if normalized not in self.symbols or not (
            self.coverage_start_utc_ms <= timestamp <= self.coverage_end_utc_ms
        ):
            return FeedEpochAssignmentV2(
                definition_id=self.definition_id,
                symbol=normalized,
                timestamp_utc_ms=timestamp,
                assignment_kind="out_of_scope",
                label="out_of_scope",
            )
        for boundary in self.boundaries:
            start = _period_start_ms(boundary.uncertainty_start_period)
            end = _period_end_ms(boundary.uncertainty_end_period)
            if start <= timestamp <= end:
                return FeedEpochAssignmentV2(
                    definition_id=self.definition_id,
                    symbol=normalized,
                    timestamp_utc_ms=timestamp,
                    assignment_kind="transition",
                    label=boundary.transition_label,
                    boundary_id=boundary.boundary_id,
                )
        for epoch in self.epochs:
            if (
                epoch.start_timestamp_utc_ms
                <= timestamp
                <= epoch.end_timestamp_utc_ms
            ):
                return FeedEpochAssignmentV2(
                    definition_id=self.definition_id,
                    symbol=normalized,
                    timestamp_utc_ms=timestamp,
                    assignment_kind="epoch",
                    label=epoch.label,
                    epoch_id=epoch.epoch_id,
                )
        return FeedEpochAssignmentV2(
            definition_id=self.definition_id,
            symbol=normalized,
            timestamp_utc_ms=timestamp,
            assignment_kind="out_of_scope",
            label="out_of_scope",
        )


@dataclass(frozen=True, slots=True)
class FeedEpochCampaignV2:
    """Complete bounded output from discovery, scan, fit, and sensitivity."""

    definition: FeedEpochDefinitionV2
    evidence: tuple[FeedEpochEvidenceV2, ...]
    source_count: int
    source_bytes: int
    runtime_seconds: float
    peak_memory_bytes: int
    skipped_sources: tuple[Mapping[str, JSONValue], ...] = ()
    schema_version: str = FEED_EPOCH_CAMPAIGN_V2_SCHEMA_VERSION

    def to_dict(self, *, include_evidence: bool = True) -> dict[str, JSONValue]:
        """Return campaign evidence with an optional compact source table."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "definition": self.definition.to_dict(),
            "source_count": self.source_count,
            "source_bytes": self.source_bytes,
            "runtime_seconds": self.runtime_seconds,
            "peak_memory_bytes": self.peak_memory_bytes,
            "skipped_sources": [dict(value) for value in self.skipped_sources],
            "limitations": [
                "epochs describe delivery technology, not latent market state",
                (
                    "hourly synchronization is an activity-count proxy, "
                    "not tick identity"
                ),
                (
                    "the static HistData source calendar is advisory for "
                    "holidays/events"
                ),
            ],
        }
        if include_evidence:
            payload["evidence"] = [item.to_dict() for item in self.evidence]
        return payload


def scan_active_time_evidence(
    path: str | Path,
    *,
    symbol: str,
    period: str,
    config: FeedEpochFitConfigV2 | None = None,
) -> FeedEpochEvidenceV2:
    """Scan one Arrow cache into bounded, denominator-explicit evidence."""
    import polars as pl  # pylint: disable=import-outside-toplevel

    selected = config or FeedEpochFitConfigV2()
    source = Path(path).resolve()
    lazy = pl.scan_ipc(source)
    schema = set(lazy.collect_schema().names())
    if not {"datetime", "bid", "ask"}.issubset(schema):
        raise ValueError("tick cache requires datetime, bid, and ask columns")
    timestamp = pl.col("datetime").cast(pl.Int64)
    bid = pl.col("bid").cast(pl.Float64)
    ask = pl.col("ask").cast(pl.Float64)
    interval = timestamp.diff()
    bid_change = bid.diff()
    ask_change = ask.diff()
    spread = ask - bid
    transition_count = pl.len() - 1
    source_minutes = (timestamp + SOURCE_OFFSET_MS) // MINUTE_MS
    source_weekday = (source_minutes // (24 * 60) + 3) % 7
    source_minute = source_minutes % (24 * 60)
    market_open = ~(
        (source_weekday == 5)
        | ((source_weekday == 4) & (source_minute >= FX_CLOSE_OPEN_MINUTE))
        | ((source_weekday == 6) & (source_minute < FX_CLOSE_OPEN_MINUTE))
    )
    active_interval = (
        (interval > 0) & (interval <= selected.active_gap_cap_ms) & market_open
    )
    expressions: list[Any] = [
        pl.len().alias("row_count"),
        timestamp.min().alias("start"),
        timestamp.max().alias("end"),
        interval.filter(interval > 0).median().alias("interval_median"),
        interval.filter(interval > 0).quantile(0.9).alias("interval_p90"),
        pl.corr(
            interval.cast(pl.Float64), interval.shift(1).cast(pl.Float64)
        ).alias("interval_lag1"),
        ((bid_change != 0) & (ask_change == 0)).sum().alias("bid_only"),
        ((bid_change == 0) & (ask_change != 0)).sum().alias("ask_only"),
        ((bid_change != 0) & (ask_change != 0)).sum().alias("joint"),
        ((bid_change == 0) & (ask_change == 0)).sum().alias("unchanged"),
        (interval == 0).sum().alias("duplicate_timestamp"),
        (interval < 0).sum().alias("non_monotonic_timestamp"),
        interval.filter(
            (interval > 0) & (interval <= selected.burst_interval_ms)
        )
        .count()
        .alias("burst_interval"),
        interval.filter(active_interval).sum().alias("active_window_duration"),
        active_interval.sum().alias("active_window_interval_count"),
        spread.median().alias("spread_median"),
        spread.quantile(0.99).alias("spread_p99"),
        (spread.diff().abs() > spread.median() * 5).sum().alias("spread_jump"),
        ((timestamp % 1000) == 0).mean().alias("exact_second_rate"),
        bid_change.abs()
        .filter(bid_change.abs() > 0)
        .quantile(0.01)
        .alias("bid_step"),
        ask_change.abs()
        .filter(ask_change.abs() > 0)
        .quantile(0.01)
        .alias("ask_step"),
        market_open.sum().alias("market_open_row_count"),
        transition_count.alias("transition_count"),
    ]
    expressions.extend(
        ((timestamp % 10) == digit).sum().alias(f"digit_{digit}")
        for digit in range(10)
    )
    summary = (
        lazy.select(expressions).collect(engine="streaming").row(named=True)
    )
    row_count = int(summary["row_count"])
    if row_count < 2:
        raise ValueError("tick cache needs at least two rows")
    activity = (
        lazy.group_by((timestamp // selected.activity_bin_ms).alias("bucket"))
        .len()
        .sort("bucket")
        .collect(engine="streaming")
    )
    activity_bins = {
        str(int(bucket)): int(count) for bucket, count in activity.iter_rows()
    }
    if len(activity_bins) > MAX_ACTIVITY_BINS:
        raise ValueError(
            "configured activity bins exceed monthly evidence bound"
        )
    session_counts, day_counts = _activity_conditioning_counts(
        lazy, selected.activity_bin_ms
    )
    stale_runs = _stale_run_summary(source)
    calendar_duration, market_open_duration = _period_denominators(period)
    active_duration = max(1, int(summary["active_window_duration"] or 0))
    active_interval_count = int(summary["active_window_interval_count"] or 0)
    market_open_row_count = int(summary["market_open_row_count"] or 0)
    transitions = max(1, int(summary["transition_count"] or row_count - 1))
    activity_values = [float(value) for value in activity_bins.values()]
    spread_median = _optional_float(summary.get("spread_median")) or 0.0
    spread_p99 = _optional_float(summary.get("spread_p99")) or spread_median
    median_interval = _optional_float(summary.get("interval_median")) or 0.0
    p90_interval = (
        _optional_float(summary.get("interval_p90")) or median_interval
    )
    price_steps = [
        value
        for value in (
            _positive_float(summary.get("bid_step")),
            _positive_float(summary.get("ask_step")),
        )
        if value is not None
    ]
    price_step = min(price_steps) if price_steps else 1e-8
    precision = max(0.0, min(12.0, round(-math.log10(price_step))))
    digit_counts = [int(summary[f"digit_{value}"] or 0) for value in range(10)]
    features = {
        "log_calendar_tick_rate_per_hour": math.log1p(
            row_count * HOUR_MS / calendar_duration
        ),
        "log_market_open_tick_rate_per_hour": math.log1p(
            market_open_row_count * HOUR_MS / market_open_duration
        ),
        "log_active_window_tick_rate_per_hour": math.log1p(
            active_interval_count * HOUR_MS / active_duration
        ),
        "bid_only_rate": int(summary["bid_only"] or 0) / transitions,
        "ask_only_rate": int(summary["ask_only"] or 0) / transitions,
        "joint_move_rate": int(summary["joint"] or 0) / transitions,
        "unchanged_rate": int(summary["unchanged"] or 0) / transitions,
        "subwindow_count_fano": _fano(activity_values),
        "log_interarrival_median_ms": math.log1p(median_interval),
        "interarrival_dispersion": p90_interval / max(1.0, median_interval),
        "interarrival_lag1": _optional_float(summary.get("interval_lag1"))
        or 0.0,
        "timestamp_exact_second_rate": _optional_float(
            summary.get("exact_second_rate")
        )
        or 0.0,
        "timestamp_last_digit_entropy": _normalized_entropy(digit_counts),
        "price_precision_digits": precision,
        "duplicate_timestamp_rate": int(summary["duplicate_timestamp"] or 0)
        / transitions,
        "burst_interval_rate": int(summary["burst_interval"] or 0)
        / transitions,
        "stale_quote_rate": int(summary["unchanged"] or 0) / transitions,
        "log_stale_run_p95": math.log1p(stale_runs["p95"]),
        "log_stale_run_max": math.log1p(stale_runs["max"]),
        "log_spread_median": math.log1p(max(0.0, spread_median) * 1e6),
        "spread_tail_ratio": spread_p99 / max(1e-12, spread_median),
        "spread_jump_rate": int(summary["spread_jump"] or 0) / transitions,
        "session_activity_dispersion": _count_dispersion(session_counts),
        "day_activity_dispersion": _count_dispersion(day_counts),
    }
    features.update(
        {
            f"session_activity_share_{name}": value
            for name, value in _normalized_activity(
                session_counts, SESSION_ACTIVITY_KEYS
            ).items()
        }
    )
    features.update(
        {
            f"day_activity_share_{name}": value
            for name, value in _normalized_activity(
                day_counts, DAY_ACTIVITY_KEYS
            ).items()
        }
    )
    rounded = {
        name: round(value, selected.rounding_digits)
        for name, value in features.items()
        if math.isfinite(value)
    }
    provenance = {name: _feature_provenance(name) for name in rounded}
    counts = {
        "transition_count": transitions,
        "market_open_row_count": market_open_row_count,
        "active_window_interval_count": active_interval_count,
        "bid_only_count": int(summary["bid_only"] or 0),
        "ask_only_count": int(summary["ask_only"] or 0),
        "joint_move_count": int(summary["joint"] or 0),
        "unchanged_count": int(summary["unchanged"] or 0),
        "duplicate_timestamp_count": int(summary["duplicate_timestamp"] or 0),
        "non_monotonic_timestamp_count": int(
            summary["non_monotonic_timestamp"] or 0
        ),
        "burst_interval_count": int(summary["burst_interval"] or 0),
        "stale_run_count": stale_runs["count"],
        "stale_run_p95": stale_runs["p95"],
        "stale_run_max": stale_runs["max"],
        "spread_jump_count": int(summary["spread_jump"] or 0),
    }
    counts.update(
        {
            f"timestamp_last_digit_{digit}_count": count
            for digit, count in enumerate(digit_counts)
        }
    )
    policy = calendar_policy_metadata()
    policy.update(
        {
            "active_time_policy_version": CALENDAR_POLICY_VERSION,
            "market_open_denominator_states": [
                "market_open",
                "friday_close",
                "sunday_open",
            ],
            "active_window_basis": "sum_positive_interarrival_at_or_below_cap",
            "active_gap_cap_ms": selected.active_gap_cap_ms,
            "activity_bin_ms": selected.activity_bin_ms,
            "conditioning_activity_basis": (
                "mean_tick_count_per_observed_activity_bin"
            ),
        }
    )
    return FeedEpochEvidenceV2(
        symbol=symbol,
        period=period,
        source_path=str(source),
        source_artifact_sha256="sha256:" + _file_sha256(source),
        source_size_bytes=source.stat().st_size,
        start_timestamp_utc_ms=int(summary["start"]),
        end_timestamp_utc_ms=int(summary["end"]),
        row_count=row_count,
        denominators_ms={
            "calendar_duration_ms": calendar_duration,
            "market_open_duration_ms": market_open_duration,
            "active_window_duration_ms": active_duration,
        },
        counts=counts,
        feature_values=rounded,
        feature_provenance=provenance,
        activity_bin_counts=activity_bins,
        calendar_policy=policy,
        limitations=(
            "hourly_activity_bins_are_synchronization_proxies",
            "static_holiday_and_event_calendar_is_advisory",
        ),
    )


def fit_active_time_feed_epochs(
    evidence: Sequence[FeedEpochEvidenceV2],
    *,
    config: FeedEpochFitConfigV2 | None = None,
) -> FeedEpochDefinitionV2:
    """Fit shared robust PELT epochs and symbol-specific deviations."""
    selected = config or FeedEpochFitConfigV2()
    prepared = _prepare_evidence(evidence, selected)
    prepared = _augment_cross_symbol_evidence(prepared, selected)
    symbols = tuple(sorted({item.symbol for item in prepared}))
    common_periods = _common_periods(prepared, symbols)
    if not common_periods:
        raise ValueError("v2 panel has no common symbol periods")
    feature_coverage = _feature_coverage(prepared, common_periods, symbols)
    features = tuple(
        name
        for name in selected.feature_names
        if feature_coverage.get(name, 0.0) >= selected.min_feature_coverage
    )
    if not features:
        raise ValueError(
            "no v2 features pass the configured coverage threshold"
        )
    global_rows = _normalized_panel_rows(
        prepared,
        periods=common_periods,
        symbols=symbols,
        features=features,
        robust_clip=selected.robust_clip,
    )
    candidate_indexes, _ = _pelt_boundaries(
        global_rows,
        min_segment=selected.min_segment_periods,
        penalty=_pelt_penalty(len(common_periods), len(features), selected),
    )
    sensitivity = _sensitivity_runs(
        prepared,
        common_periods,
        symbols,
        features,
        selected,
    )
    candidate_supports, candidate_family_supports, uncertainty = (
        _boundary_support(
            candidate_indexes,
            sensitivity,
            tolerance=selected.boundary_match_tolerance_periods,
        )
    )
    boundary_indexes = tuple(
        index
        for index in candidate_indexes
        if candidate_supports.get(index, 0.0) >= selected.min_boundary_support
        and all(
            support >= selected.min_boundary_support
            for support in candidate_family_supports.get(index, {}).values()
        )
    )
    rejected_indexes = tuple(
        index for index in candidate_indexes if index not in boundary_indexes
    )
    boundary_supports = {
        index: candidate_supports[index] for index in boundary_indexes
    }
    family_supports = {
        index: candidate_family_supports[index] for index in boundary_indexes
    }
    boundaries: list[FeedEpochBoundaryV2] = []
    pelt_penalty = _pelt_penalty(len(common_periods), len(features), selected)
    for position, index in enumerate(boundary_indexes):
        segment_start = boundary_indexes[position - 1] if position else 0
        segment_end = (
            boundary_indexes[position + 1]
            if position + 1 < len(boundary_indexes)
            else len(common_periods)
        )
        left_period = common_periods[index - 1]
        right_period = common_periods[index]
        matched = uncertainty.get(index, (index,))
        item = FeedEpochBoundaryV2(
            left_period=left_period,
            right_period=right_period,
            central_timestamp_utc_ms=_period_start_ms(right_period),
            support=round(boundary_supports.get(index, 0.0), 8),
            uncertainty_start_period=common_periods[max(0, min(matched))],
            uncertainty_end_period=common_periods[
                min(len(common_periods) - 1, max(matched))
            ],
            objective_gain=round(
                _local_boundary_gain(
                    global_rows,
                    segment_start,
                    index,
                    segment_end,
                    pelt_penalty,
                ),
                8,
            ),
            supporting_features=_boundary_supporting_features(
                global_rows,
                segment_start,
                index,
                segment_end,
                features,
            ),
        )
        boundaries.append(item)
    epochs = _epoch_intervals(
        prepared, common_periods, boundary_indexes, features
    )
    deviations = _symbol_deviations(
        prepared,
        periods=common_periods,
        symbols=symbols,
        features=features,
        global_boundaries=boundary_indexes,
        global_candidates=candidate_indexes,
        config=selected,
    )
    run_counts: dict[str, int] = defaultdict(int)
    for label, _ in sensitivity:
        run_counts[label.split(":", 1)[0]] += 1
    reasons: list[str] = []
    if len(symbols) < selected.min_symbol_count:
        reasons.append("insufficient_symbol_support")
    if len(common_periods) < selected.min_evidence_periods:
        reasons.append("insufficient_common_period_support")
    if not boundary_indexes:
        reasons.append("no_shared_technology_boundary_detected")
    stability = FeedEpochStabilityV2(
        status="pass" if not reasons else "fail",
        reasons=tuple(reasons),
        run_count=len(sensitivity),
        run_counts=dict(sorted(run_counts.items())),
        boundary_support={
            common_periods[index]: round(value, 8)
            for index, value in sorted(boundary_supports.items())
        },
        boundary_support_by_family={
            common_periods[index]: {
                family: round(value, 8)
                for family, value in sorted(values.items())
            }
            for index, values in sorted(family_supports.items())
        },
        rejected_candidates={
            common_periods[index]: {
                "overall_support": round(candidate_supports[index], 8),
                "support_by_family": {
                    family: round(value, 8)
                    for family, value in sorted(
                        candidate_family_supports[index].items()
                    )
                },
                "reason": "sensitivity_support_below_threshold",
            }
            for index in rejected_indexes
        },
        feature_coverage={
            name: round(feature_coverage[name], 8) for name in features
        },
        common_period_count=len(common_periods),
        symbol_count=len(symbols),
    )
    lineage = _lineage(prepared, common_periods, sensitivity)
    return FeedEpochDefinitionV2(
        config=selected,
        symbols=symbols,
        coverage_start_utc_ms=min(
            item.start_timestamp_utc_ms for item in prepared
        ),
        coverage_end_utc_ms=max(item.end_timestamp_utc_ms for item in prepared),
        evidence_count=len(prepared),
        period_count=len(common_periods),
        feature_names=features,
        boundaries=tuple(boundaries),
        epochs=epochs,
        symbol_deviations=deviations,
        stability=stability,
        lineage=lineage,
    )


def analyze_active_time_feed_epochs(
    paths: Iterable[str | Path],
    *,
    config: FeedEpochFitConfigV2 | None = None,
) -> FeedEpochCampaignV2:
    """Discover and fit every supported monthly ASCII tick cache."""
    selected = config or FeedEpochFitConfigV2()
    started = time.perf_counter()
    discovered = discover_quality_targets(paths)
    candidates = [
        target
        for target in discovered.targets
        if target.kind is QualityTargetKind.CACHE
        and target.data_format.lower() == "ascii"
        and target.timeframe.upper() == "T"
        and _valid_month(target.period)
    ]
    if not candidates:
        raise ValueError("no monthly ASCII tick caches were discovered")
    if len(candidates) > selected.max_evidence:
        raise ValueError("discovered evidence exceeds configured maximum")
    evidence: list[FeedEpochEvidenceV2] = []
    skipped: list[Mapping[str, JSONValue]] = []
    for target in candidates:
        try:
            evidence.append(
                scan_active_time_evidence(
                    target.path,
                    symbol=target.symbol,
                    period=target.period,
                    config=selected,
                )
            )
        except (OSError, TypeError, ValueError) as exc:
            skipped.append(
                {
                    "path": target.path,
                    "symbol": target.symbol,
                    "period": target.period,
                    "reason": str(exc)[:512],
                }
            )
    fitted_evidence = _augment_cross_symbol_evidence(evidence, selected)
    definition = fit_active_time_feed_epochs(fitted_evidence, config=selected)
    return FeedEpochCampaignV2(
        definition=definition,
        evidence=fitted_evidence,
        source_count=len(evidence),
        source_bytes=sum(item.source_size_bytes for item in evidence),
        runtime_seconds=round(time.perf_counter() - started, 6),
        peak_memory_bytes=peak_rss_bytes(),
        skipped_sources=tuple(skipped),
    )


def write_active_time_feed_epoch_campaign(
    campaign: FeedEpochCampaignV2,
    directory: str | Path,
) -> Mapping[str, ArtifactRef]:
    """Write separate compact definition, evidence, and campaign artifacts."""
    root = Path(directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    payloads: dict[str, Mapping[str, JSONValue]] = {
        "definition": campaign.definition.to_dict(),
        "evidence": {
            "schema_version": FEED_EPOCH_EVIDENCE_V2_SCHEMA_VERSION,
            "evidence_count": len(campaign.evidence),
            "evidence": [item.to_dict() for item in campaign.evidence],
        },
        "campaign": campaign.to_dict(include_evidence=False),
    }
    artifacts: dict[str, ArtifactRef] = {}
    for name, payload in payloads.items():
        encoded = _canonical_json(payload).encode("utf-8") + b"\n"
        path = root / f"feed-epochs-v2-{name}.json"
        path.write_bytes(encoded)
        artifacts[name] = ArtifactRef(
            path=str(path),
            sha256=hashlib.sha256(encoded).hexdigest(),
            size_bytes=len(encoded),
            kind=f"feed_epochs_v2_{name}",
        )
    return artifacts


def read_active_time_feed_epoch_definition(
    path: str | Path,
) -> FeedEpochDefinitionV2:
    """Read and validate a persisted version-two definition artifact."""
    source = Path(path).expanduser().resolve()
    if source.stat().st_size > 64 * 1024 * 1024:
        raise ValueError("v2 definition artifact exceeds its size bound")
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("v2 definition artifact must be a JSON object")
    return FeedEpochDefinitionV2.from_dict(payload)


def _activity_conditioning_counts(
    lazy: Any,
    activity_bin_ms: int,
) -> tuple[dict[str, float], dict[str, float]]:
    import polars as pl  # pylint: disable=import-outside-toplevel

    timestamp = pl.col("datetime").cast(pl.Int64)
    utc_minutes = timestamp // MINUTE_MS
    utc_minute = utc_minutes % (24 * 60)
    source_minutes = (timestamp + SOURCE_OFFSET_MS) // MINUTE_MS
    source_weekday = (source_minutes // (24 * 60) + 3) % 7
    bucket = (timestamp // activity_bin_ms).alias("bucket")
    conditions: dict[str, Any] = {}
    any_session = pl.lit(False)
    for window in SESSION_WINDOWS:
        condition = utc_minute.is_between(
            window.start_minute_utc, window.end_minute_utc - 1
        )
        conditions[window.name] = condition
        any_session = any_session | condition
    conditions["off_session"] = ~any_session
    session_frame = (
        lazy.select(
            bucket,
            *(
                condition.cast(pl.UInt64).alias(name)
                for name, condition in conditions.items()
            ),
        )
        .group_by("bucket")
        .agg(*(pl.col(name).sum() for name in conditions))
        .select(*(pl.col(name).mean().alias(name) for name in conditions))
        .collect(engine="streaming")
    )
    day_frame = (
        lazy.select(bucket, source_weekday.alias("weekday"))
        .group_by("bucket", "weekday")
        .len()
        .group_by("weekday")
        .agg(pl.col("len").mean().alias("mean_count"))
        .collect(engine="streaming")
    )
    sessions = {
        name: float(session_frame[name][0] or 0.0) for name in conditions
    }
    days = {str(int(key)): float(value) for key, value in day_frame.iter_rows()}
    return sessions, days


def _stale_run_summary(path: Path) -> dict[str, int]:
    """Return exact bounded summaries of unchanged bid/ask transition runs."""
    import numpy as np  # pylint: disable=import-outside-toplevel
    import pyarrow as pa  # pylint: disable=import-outside-toplevel
    import pyarrow.ipc as ipc  # pylint: disable=import-outside-toplevel

    histogram: Counter[int] = Counter()
    current_run = 0
    previous_bid: float | None = None
    previous_ask: float | None = None
    with pa.memory_map(str(path), "r") as source:
        reader = ipc.open_file(source)
        bid_index = reader.schema.get_field_index("bid")
        ask_index = reader.schema.get_field_index("ask")
        if bid_index < 0 or ask_index < 0:
            raise ValueError("tick cache requires bid and ask columns")
        for index in range(reader.num_record_batches):
            batch = reader.get_batch(index)
            bids = batch.column(bid_index).to_numpy(zero_copy_only=False)
            asks = batch.column(ask_index).to_numpy(zero_copy_only=False)
            if len(bids) == 0:
                continue
            unchanged = np.empty(len(bids), dtype=np.bool_)
            unchanged[0] = (
                previous_bid is not None
                and previous_ask is not None
                and bids[0] == previous_bid
                and asks[0] == previous_ask
            )
            unchanged[1:] = (bids[1:] == bids[:-1]) & (asks[1:] == asks[:-1])
            breaks = np.flatnonzero(~unchanged)
            if len(breaks) == 0:
                current_run += len(unchanged)
            else:
                current_run += int(breaks[0])
                if current_run:
                    histogram[current_run] += 1
                between = np.diff(breaks) - 1
                positive = between[between > 0]
                if len(positive):
                    lengths, frequencies = np.unique(
                        positive, return_counts=True
                    )
                    histogram.update(
                        {
                            int(length): int(frequency)
                            for length, frequency in zip(
                                lengths, frequencies, strict=True
                            )
                        }
                    )
                current_run = len(unchanged) - int(breaks[-1]) - 1
            previous_bid = float(bids[-1])
            previous_ask = float(asks[-1])
    if current_run:
        histogram[current_run] += 1
    run_count = sum(histogram.values())
    if not run_count:
        return {"count": 0, "p95": 0, "max": 0}
    target = math.ceil(run_count * 0.95)
    cumulative = 0
    p95 = 0
    for length, frequency in sorted(histogram.items()):
        cumulative += frequency
        if cumulative >= target:
            p95 = length
            break
    return {"count": run_count, "p95": p95, "max": max(histogram)}


@lru_cache(maxsize=512)
def _period_denominators(period: str) -> tuple[int, int]:
    start = datetime(int(period[:4]), int(period[4:]), 1, tzinfo=timezone.utc)
    if start.month == 12:
        end = datetime(start.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(start.year, start.month + 1, 1, tzinfo=timezone.utc)
    calendar_duration = int((end - start).total_seconds() * 1000)
    open_minutes = 0
    current = start
    while current < end:
        source = current + timedelta(hours=-5)
        weekday = source.weekday()
        minute = source.hour * 60 + source.minute
        closed = (
            weekday == 5
            or (weekday == 4 and minute >= FX_CLOSE_OPEN_MINUTE)
            or (weekday == 6 and minute < FX_CLOSE_OPEN_MINUTE)
        )
        open_minutes += int(not closed)
        current += timedelta(minutes=1)
    return calendar_duration, open_minutes * MINUTE_MS


def _augment_cross_symbol_evidence(
    evidence: Sequence[FeedEpochEvidenceV2],
    config: FeedEpochFitConfigV2,
) -> tuple[FeedEpochEvidenceV2, ...]:
    by_period: dict[str, list[FeedEpochEvidenceV2]] = defaultdict(list)
    for item in evidence:
        by_period[item.period].append(item)
    result: list[FeedEpochEvidenceV2] = []
    for item in evidence:
        peers = [value for value in by_period[item.period] if value != item]
        correlations: list[float] = []
        overlaps: list[float] = []
        for peer in peers:
            keys = sorted(
                set(item.activity_bin_counts) | set(peer.activity_bin_counts),
                key=int,
            )
            left = [float(item.activity_bin_counts.get(key, 0)) for key in keys]
            right = [
                float(peer.activity_bin_counts.get(key, 0)) for key in keys
            ]
            correlation = _correlation(left, right)
            if correlation is not None:
                correlations.append(correlation)
            active_union = sum(
                1 for lvalue, rvalue in zip(left, right) if lvalue or rvalue
            )
            active_intersection = sum(
                1 for lvalue, rvalue in zip(left, right) if lvalue and rvalue
            )
            if active_union:
                overlaps.append(active_intersection / active_union)
        features = dict(item.feature_values)
        provenance = dict(item.feature_provenance)
        if correlations:
            features["cross_symbol_activity_correlation"] = round(
                median(correlations), config.rounding_digits
            )
            provenance["cross_symbol_activity_correlation"] = (
                "activity_bin_counts",
                "peer.activity_bin_counts",
            )
        if overlaps:
            features["cross_symbol_active_bin_overlap"] = round(
                median(overlaps), config.rounding_digits
            )
            provenance["cross_symbol_active_bin_overlap"] = (
                "activity_bin_counts",
                "peer.activity_bin_counts",
            )
        result.append(
            replace(
                item,
                feature_values=features,
                feature_provenance=provenance,
                evidence_id="",
            )
        )
    return tuple(result)


def _prepare_evidence(
    evidence: Sequence[FeedEpochEvidenceV2], config: FeedEpochFitConfigV2
) -> tuple[FeedEpochEvidenceV2, ...]:
    ordered = tuple(
        sorted(evidence, key=lambda item: (item.period, item.symbol))
    )
    if not ordered or len(ordered) > config.max_evidence:
        raise ValueError("v2 evidence is empty or exceeds configured maximum")
    axes = {(item.symbol, item.period) for item in ordered}
    if len(axes) != len(ordered):
        raise ValueError("duplicate symbol-period v2 evidence")
    return ordered


def _common_periods(
    evidence: Sequence[FeedEpochEvidenceV2], symbols: Sequence[str]
) -> tuple[str, ...]:
    by_symbol = {
        symbol: {item.period for item in evidence if item.symbol == symbol}
        for symbol in symbols
    }
    return tuple(
        sorted(set.intersection(*(by_symbol[symbol] for symbol in symbols)))
    )


def _feature_coverage(
    evidence: Sequence[FeedEpochEvidenceV2],
    periods: Sequence[str],
    symbols: Sequence[str],
) -> dict[str, float]:
    selected = [
        item
        for item in evidence
        if item.period in periods and item.symbol in symbols
    ]
    denominator = max(1, len(selected))
    return {
        name: sum(name in item.feature_values for item in selected)
        / denominator
        for name in DEFAULT_ACTIVE_TIME_FEATURES
    }


def _normalized_panel_rows(
    evidence: Sequence[FeedEpochEvidenceV2],
    *,
    periods: Sequence[str],
    symbols: Sequence[str],
    features: Sequence[str],
    robust_clip: float,
) -> tuple[tuple[float | None, ...], ...]:
    lookup = {(item.symbol, item.period): item for item in evidence}
    normalized: dict[tuple[str, str, str], float] = {}
    for symbol in symbols:
        for feature in features:
            values: list[float | None] = [
                lookup[(symbol, period)].feature_values.get(feature)
                for period in periods
            ]
            finite = [value for value in values if value is not None]
            if not finite:
                continue
            center = median(finite)
            absolute = [abs(value - center) for value in finite]
            scale = max(1e-12, median(absolute) * 1.4826)
            for period, value in zip(periods, values):
                if value is not None:
                    normalized[(symbol, period, feature)] = max(
                        -robust_clip, min(robust_clip, (value - center) / scale)
                    )
    rows: list[tuple[float | None, ...]] = []
    for period in periods:
        row: list[float | None] = []
        for feature in features:
            period_values: list[float] = [
                normalized[(symbol, period, feature)]
                for symbol in symbols
                if (symbol, period, feature) in normalized
            ]
            row.append(median(period_values) if period_values else None)
        rows.append(tuple(row))
    return tuple(rows)


def _pelt_penalty(
    period_count: int, feature_count: int, config: FeedEpochFitConfigV2
) -> float:
    return (
        config.penalty_multiplier
        * math.log(max(2, period_count))
        * math.sqrt(max(1, feature_count))
    )


def _pelt_boundaries(
    rows: Sequence[Sequence[float | None]],
    *,
    min_segment: int,
    penalty: float,
) -> tuple[tuple[int, ...], float]:
    """Return exact PELT boundaries using a missing-aware squared cost."""
    count = len(rows)
    if count < min_segment * 2:
        return (), 0.0
    prefix = _cost_prefix(rows)
    costs = [math.inf] * (count + 1)
    changes: list[tuple[int, ...]] = [()] * (count + 1)
    costs[0] = -penalty
    candidates: list[int] = []
    for end in range(min_segment, count + 1):
        new_candidate = end - min_segment
        if new_candidate == 0 or costs[new_candidate] < math.inf:
            candidates.append(new_candidate)
        evaluated: list[tuple[float, int]] = []
        for start in candidates:
            if end - start < min_segment or costs[start] == math.inf:
                continue
            value = costs[start] + _segment_cost(prefix, start, end) + penalty
            evaluated.append((value, start))
        if not evaluated:
            continue
        best, best_start = min(
            evaluated, key=lambda value: (value[0], value[1])
        )
        costs[end] = best
        changes[end] = changes[best_start] + (
            (best_start,) if best_start else ()
        )
        candidates = [
            start
            for value, start in evaluated
            if value <= best + penalty + 1e-12
        ]
    if costs[count] == math.inf:
        return (), 0.0
    no_change = _segment_cost(prefix, 0, count)
    objective_gain = max(0.0, no_change - costs[count])
    return tuple(index for index in changes[count] if index), objective_gain


def _cost_prefix(
    rows: Sequence[Sequence[float | None]],
) -> tuple[tuple[list[int], list[float], list[float]], ...]:
    width = len(rows[0]) if rows else 0
    result: list[tuple[list[int], list[float], list[float]]] = []
    for column in range(width):
        counts = [0]
        sums = [0.0]
        squares = [0.0]
        for row in rows:
            value = row[column]
            counts.append(counts[-1] + int(value is not None))
            sums.append(sums[-1] + (value or 0.0))
            squares.append(squares[-1] + ((value or 0.0) ** 2))
        result.append((counts, sums, squares))
    return tuple(result)


def _segment_cost(
    prefix: Sequence[tuple[list[int], list[float], list[float]]],
    start: int,
    end: int,
) -> float:
    result = 0.0
    for counts, sums, squares in prefix:
        count = counts[end] - counts[start]
        if count < 2:
            continue
        total = sums[end] - sums[start]
        square_total = squares[end] - squares[start]
        result += max(0.0, square_total - total * total / count)
    return result


def _sensitivity_runs(
    evidence: Sequence[FeedEpochEvidenceV2],
    periods: tuple[str, ...],
    symbols: tuple[str, ...],
    features: tuple[str, ...],
    config: FeedEpochFitConfigV2,
) -> tuple[tuple[str, tuple[int, ...]], ...]:
    specs: list[tuple[str, tuple[str, ...], tuple[str, ...], float]] = []
    for multiplier in config.sensitivity_penalty_multipliers:
        specs.append((f"penalty:{multiplier:g}", symbols, features, multiplier))
    for symbol in symbols:
        retained = tuple(value for value in symbols if value != symbol)
        specs.append((f"symbol_holdout:{symbol}", retained, features, 1.0))
    for feature in features:
        retained = tuple(value for value in features if value != feature)
        if retained:
            specs.append((f"feature_holdout:{feature}", symbols, retained, 1.0))
    for label, excluded in (
        ("calendar_policy", "log_calendar_tick_rate_per_hour"),
        ("market_open_policy", "log_market_open_tick_rate_per_hour"),
        ("duplicate_policy", "duplicate_timestamp_rate"),
    ):
        retained = tuple(value for value in features if value != excluded)
        if retained:
            specs.append((f"{label}:exclude", symbols, retained, 1.0))
    specs = specs[: config.max_sensitivity_runs]
    runs: list[tuple[str, tuple[int, ...]]] = []
    for label, selected_symbols, selected_features, multiplier in specs:
        if not selected_symbols or not selected_features:
            continue
        rows = _normalized_panel_rows(
            evidence,
            periods=periods,
            symbols=selected_symbols,
            features=selected_features,
            robust_clip=config.robust_clip,
        )
        indexes, _ = _pelt_boundaries(
            rows,
            min_segment=config.min_segment_periods,
            penalty=_pelt_penalty(len(periods), len(selected_features), config)
            * multiplier,
        )
        runs.append((label, indexes))
    return tuple(runs)


def _boundary_support(
    boundaries: Sequence[int],
    runs: Sequence[tuple[str, tuple[int, ...]]],
    *,
    tolerance: int,
) -> tuple[
    dict[int, float],
    dict[int, dict[str, float]],
    dict[int, tuple[int, ...]],
]:
    supports: dict[int, float] = {}
    family_supports: dict[int, dict[str, float]] = {}
    uncertainty: dict[int, tuple[int, ...]] = {}
    denominator = max(1, len(runs))
    for boundary in boundaries:
        matches = [
            candidate
            for _, indexes in runs
            for candidate in indexes
            if abs(candidate - boundary) <= tolerance
        ]
        supported_runs = sum(
            any(abs(candidate - boundary) <= tolerance for candidate in indexes)
            for _, indexes in runs
        )
        supports[boundary] = supported_runs / denominator
        families = sorted({label.split(":", 1)[0] for label, _ in runs})
        family_supports[boundary] = {}
        for family in families:
            family_runs = [
                indexes
                for label, indexes in runs
                if label.split(":", 1)[0] == family
            ]
            family_supports[boundary][family] = sum(
                any(
                    abs(candidate - boundary) <= tolerance
                    for candidate in indexes
                )
                for indexes in family_runs
            ) / max(1, len(family_runs))
        uncertainty[boundary] = tuple(matches or (boundary,))
    return supports, family_supports, uncertainty


def _boundary_supporting_features(
    rows: Sequence[Sequence[float | None]],
    segment_start: int,
    index: int,
    segment_end: int,
    features: Sequence[str],
) -> tuple[str, ...]:
    result: list[tuple[float, str]] = []
    for column, feature in enumerate(features):
        left: list[float] = []
        right: list[float] = []
        for row in rows[segment_start:index]:
            value = row[column]
            if value is not None:
                left.append(value)
        for row in rows[index:segment_end]:
            value = row[column]
            if value is not None:
                right.append(value)
        if left and right:
            result.append((abs(median(right) - median(left)), feature))
    return tuple(value[1] for value in sorted(result, reverse=True)[:8])


def _local_boundary_gain(
    rows: Sequence[Sequence[float | None]],
    segment_start: int,
    index: int,
    segment_end: int,
    penalty: float,
) -> float:
    prefix = _cost_prefix(rows)
    unsplit = _segment_cost(prefix, segment_start, segment_end)
    split = _segment_cost(prefix, segment_start, index) + _segment_cost(
        prefix, index, segment_end
    )
    return max(0.0, unsplit - split - penalty)


def _epoch_intervals(
    evidence: Sequence[FeedEpochEvidenceV2],
    periods: tuple[str, ...],
    boundaries: tuple[int, ...],
    features: tuple[str, ...],
) -> tuple[FeedEpochIntervalV2, ...]:
    result: list[FeedEpochIntervalV2] = []
    indexes = (0, *boundaries, len(periods))
    for number, (start, end) in enumerate(zip(indexes, indexes[1:]), 1):
        selected_periods = set(periods[start:end])
        selected = [
            item for item in evidence if item.period in selected_periods
        ]
        medians = {
            feature: round(
                median(
                    item.feature_values[feature]
                    for item in selected
                    if feature in item.feature_values
                ),
                10,
            )
            for feature in features
            if any(feature in item.feature_values for item in selected)
        }
        item = FeedEpochIntervalV2(
            label=f"technology_epoch_{number:02d}",
            period_start=periods[start],
            period_end=periods[end - 1],
            start_timestamp_utc_ms=_period_start_ms(periods[start]),
            end_timestamp_utc_ms=_period_end_ms(periods[end - 1]),
            evidence_count=len(selected),
            feature_medians=medians,
        )
        result.append(item)
    return tuple(result)


def _symbol_deviations(
    evidence: Sequence[FeedEpochEvidenceV2],
    *,
    periods: tuple[str, ...],
    symbols: tuple[str, ...],
    features: tuple[str, ...],
    global_boundaries: tuple[int, ...],
    global_candidates: tuple[int, ...],
    config: FeedEpochFitConfigV2,
) -> tuple[FeedEpochSymbolDeviationV2, ...]:
    result: list[FeedEpochSymbolDeviationV2] = []
    for symbol in symbols:
        rows = _normalized_panel_rows(
            evidence,
            periods=periods,
            symbols=(symbol,),
            features=features,
            robust_clip=config.robust_clip,
        )
        indexes, _ = _pelt_boundaries(
            rows,
            min_segment=config.min_segment_periods,
            penalty=_pelt_penalty(len(periods), len(features), config),
        )
        for index in indexes:
            if any(
                abs(index - candidate)
                <= config.boundary_match_tolerance_periods
                for candidate in global_candidates
            ):
                continue
            distance = (
                min(abs(index - value) for value in global_boundaries)
                if global_boundaries
                else None
            )
            if (
                distance is None
                or distance > config.boundary_match_tolerance_periods
            ):
                result.append(
                    FeedEpochSymbolDeviationV2(
                        symbol=symbol,
                        left_period=periods[index - 1],
                        right_period=periods[index],
                        nearest_global_distance_periods=distance,
                    )
                )
    return tuple(result)


def _lineage(
    evidence: Sequence[FeedEpochEvidenceV2],
    periods: Sequence[str],
    sensitivity: Sequence[tuple[str, tuple[int, ...]]],
) -> Mapping[str, JSONValue]:
    sources: list[JSONValue] = [
        {
            "symbol": item.symbol,
            "period": item.period,
            "source_artifact_sha256": item.source_artifact_sha256,
            "evidence_id": item.evidence_id,
        }
        for item in evidence
    ]
    payload: dict[str, JSONValue] = {
        "fitting_basis": "real_ascii_tick_arrow_caches",
        "algorithm": "robust_multivariate_pelt",
        "cost": "within_segment_winsorized_squared_error",
        "global_aggregation": "within_symbol_robust_scale_then_symbol_median",
        "calendar_policy_version": CALENDAR_POLICY_VERSION,
        "common_period_start": periods[0],
        "common_period_end": periods[-1],
        "sources": sources,
        "sensitivity_runs": [
            {"label": label, "boundary_indexes": list(indexes)}
            for label, indexes in sensitivity
        ],
    }
    payload["lineage_sha256"] = _payload_sha256(payload)
    return payload


def _feature_provenance(name: str) -> tuple[str, ...]:
    if name.startswith("log_calendar"):
        return ("row_count", "denominators_ms.calendar_duration_ms")
    if name.startswith("log_market_open"):
        return (
            "counts.market_open_row_count",
            "denominators_ms.market_open_duration_ms",
        )
    if name.startswith("log_active_window"):
        return (
            "counts.active_window_interval_count",
            "denominators_ms.active_window_duration_ms",
        )
    if name.startswith("cross_symbol"):
        return ("activity_bin_counts", "peer.activity_bin_counts")
    if name in {
        "bid_only_rate",
        "ask_only_rate",
        "joint_move_rate",
        "unchanged_rate",
        "stale_quote_rate",
    }:
        return (
            "cache.bid.observed_sequence_diff",
            "cache.ask.observed_sequence_diff",
            "counts.transition_count",
        )
    if name == "subwindow_count_fano":
        return ("activity_bin_counts", "variance_over_mean")
    if name in {
        "log_interarrival_median_ms",
        "interarrival_dispersion",
        "interarrival_lag1",
        "burst_interval_rate",
    }:
        return ("cache.datetime.observed_sequence_diff",)
    if name in {
        "timestamp_exact_second_rate",
        "timestamp_last_digit_entropy",
        "duplicate_timestamp_rate",
    }:
        return ("cache.datetime", "cache.datetime.observed_sequence_diff")
    if name == "price_precision_digits":
        return (
            "cache.bid.nonzero_diff_p01",
            "cache.ask.nonzero_diff_p01",
            "negative_log10_rounded",
        )
    if name in {"log_spread_median", "spread_tail_ratio"}:
        return ("cache.ask_minus_bid",)
    if name == "spread_jump_rate":
        return (
            "cache.ask_minus_bid.observed_sequence_diff",
            "median_spread_x5_threshold",
        )
    if name.startswith("log_stale_run"):
        return (
            "cache.bid.observed_sequence_diff",
            "cache.ask.observed_sequence_diff",
            "exact_run_length_histogram",
        )
    if name.startswith("session_activity_share_"):
        return (
            "cache.datetime",
            "calendar_policy.session_windows",
            "mean_tick_count_per_observed_activity_bin",
        )
    if name.startswith("day_activity_share_"):
        return (
            "cache.datetime",
            "calendar_policy.source_timezone",
            "mean_tick_count_per_observed_activity_bin",
        )
    if name in {"session_activity_dispersion", "day_activity_dispersion"}:
        return (
            "cache.datetime",
            "mean_tick_count_per_observed_activity_bin",
            "coefficient_of_variation",
        )
    return ("cache.datetime", "cache.bid", "cache.ask")


def _fano(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean <= 0:
        return 0.0
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return variance / mean


def _normalized_entropy(counts: Sequence[int]) -> float:
    total = sum(counts)
    if total <= 0:
        return 0.0
    entropy = -sum(
        (value / total) * math.log(value / total)
        for value in counts
        if value > 0
    )
    return entropy / math.log(max(2, len(counts)))


def _count_dispersion(counts: Mapping[str, int | float]) -> float:
    values = [float(value) for value in counts.values()]
    if not values:
        return 0.0
    center = sum(values) / len(values)
    if center <= 0:
        return 0.0
    return (
        math.sqrt(sum((value - center) ** 2 for value in values) / len(values))
        / center
    )


def _normalized_activity(
    counts: Mapping[str, int | float], keys: Sequence[str]
) -> dict[str, float]:
    values = {key: float(counts.get(key, 0.0)) for key in keys}
    total = sum(values.values())
    if total <= 0:
        return {key: 0.0 for key in keys}
    return {key: value / total for key, value in values.items()}


def _correlation(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = sum(left) / len(left)
    right_mean = sum(right) / len(right)
    numerator = sum(
        (lvalue - left_mean) * (rvalue - right_mean)
        for lvalue, rvalue in zip(left, right)
    )
    left_square = sum((value - left_mean) ** 2 for value in left)
    right_square = sum((value - right_mean) ** 2 for value in right)
    denominator = math.sqrt(left_square * right_square)
    return numerator / denominator if denominator else None


def _period_start_ms(period: str) -> int:
    value = datetime(int(period[:4]), int(period[4:]), 1, tzinfo=timezone.utc)
    return int(value.timestamp() * 1000)


def _period_end_ms(period: str) -> int:
    start = datetime(int(period[:4]), int(period[4:]), 1, tzinfo=timezone.utc)
    if start.month == 12:
        end = datetime(start.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(start.year, start.month + 1, 1, tzinfo=timezone.utc)
    return int(end.timestamp() * 1000) - 1


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _payload_sha256(payload: Mapping[str, JSONValue]) -> str:
    return (
        "sha256:"
        + hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()
    )


def _sha256_id(value: Any, name: str) -> str:
    text = str(value or "").strip().lower()
    digest = text.removeprefix("sha256:")
    if len(digest) != 64 or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise ValueError(f"{name} must be a sha256 identifier")
    return "sha256:" + digest


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = _canonical_json(payload).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _canonical_json(payload: Mapping[str, JSONValue]) -> str:
    return json.dumps(
        payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def _valid_month(value: str) -> bool:
    return len(value) == 6 and value.isdigit() and 1 <= int(value[4:]) <= 12


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} is required")
    return text


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _integer_text(value: Any, name: str) -> int:
    try:
        result = int(str(value))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    return result


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} is outside its supported range")
    return result


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be finite")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _bounded_float(
    value: Any, name: str, minimum: float, maximum: float
) -> float:
    result = _finite_float(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} is outside its supported range")
    return result


def _optional_float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _positive_float(value: Any) -> float | None:
    result = _optional_float(value)
    return result if result is not None and result > 0 else None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return value
    return ()


def _int_mapping(value: Any) -> dict[str, int]:
    return {
        str(name): _strict_int(item, str(name))
        for name, item in _mapping(value).items()
    }


def _float_mapping(value: Any) -> dict[str, float]:
    return {
        str(name): _finite_float(item, str(name))
        for name, item in _mapping(value).items()
    }
