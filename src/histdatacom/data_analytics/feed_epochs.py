"""Deterministic technological feed-epoch contracts and fitting.

This module consumes bounded canonical time-series fingerprints.  It never
discovers or scans raw tick files.  The resulting artifact describes changes
in the technical observation process, including uncertainty and deterministic
stability evidence, without claiming that calendar years are regimes.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from statistics import median
from typing import Any

from histdatacom.data_quality.fingerprints import (
    TIME_SERIES_FINGERPRINT_SCHEMA_VERSION,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json

FEED_EPOCH_EVIDENCE_SCHEMA_VERSION = "histdatacom.feed-epoch-evidence.v1"
FEED_EPOCH_FIT_CONFIG_SCHEMA_VERSION = "histdatacom.feed-epoch-fit-config.v1"
FEED_EPOCH_BOUNDARY_SCHEMA_VERSION = "histdatacom.feed-epoch-boundary.v1"
FEED_EPOCH_INTERVAL_SCHEMA_VERSION = "histdatacom.feed-epoch-interval.v1"
FEED_EPOCH_STABILITY_SCHEMA_VERSION = "histdatacom.feed-epoch-stability.v1"
FEED_EPOCH_DEFINITION_SCHEMA_VERSION = "histdatacom.feed-epoch-definition.v1"
FEED_EPOCH_ASSIGNMENT_SCHEMA_VERSION = "histdatacom.feed-epoch-assignment.v1"

MAX_FEED_EPOCH_EVIDENCE = 4096
MAX_FEED_EPOCH_FEATURES = 64
MAX_FEED_EPOCH_SENSITIVITY_RUNS = 256
_PERIOD_RE = re.compile(r"^\d{4}(?:\d{2})?$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_STABILITY_ANALYSES = ("sampling", "missing_periods", "feature_removal")
_SOURCE_HASH_BASES = {
    "canonical_fingerprint_id",
    "canonical_fingerprint_aggregate_id",
    "persisted_fingerprint_artifact_sha256",
}

DEFAULT_FEED_EPOCH_FEATURES = (
    "log_tick_rate_per_hour",
    "log_median_interarrival_ms",
    "log_p95_interarrival_ms",
    "minimum_observed_interval_ms",
    "price_precision_digits",
    "spread_median",
    "conditioned_spread_median",
    "absolute_spread_change_median",
    "stale_repeat_rate",
    "burst_rate",
    "duplicate_timestamp_rate",
    "suspicious_gap_rate",
    "source_quality_penalty",
)

_FEATURE_PROVENANCE = {
    "log_tick_rate_per_hour": ("coverage.row_count", "coverage.duration_ms"),
    "log_median_interarrival_ms": (
        "microstructure_dynamics.interarrival_ms.median",
    ),
    "log_p95_interarrival_ms": (
        "microstructure_dynamics.interarrival_ms.quantiles.0.95",
    ),
    "minimum_observed_interval_ms": ("temporal_topology.min_interval_ms",),
    "price_precision_digits": (
        "tick_distribution.precision.column_decimal_place_counts",
    ),
    "spread_median": ("tick_distribution.spread.median",),
    "conditioned_spread_median": (
        "conditional_distributions.by_active_session.*.spread.median",
    ),
    "absolute_spread_change_median": (
        "microstructure_dynamics.absolute_spread_change.median",
    ),
    "stale_repeat_rate": ("microstructure_dynamics.stale_quote.repeat_rate",),
    "burst_rate": ("microstructure_dynamics.burst.burst_rate",),
    "duplicate_timestamp_rate": (
        "temporal_topology.duplicate_timestamp_count",
        "temporal_topology.parsed_row_count",
    ),
    "suspicious_gap_rate": (
        "temporal_topology.suspicious_gap_count",
        "temporal_topology.interval_count",
    ),
    "source_quality_penalty": (
        "fingerprint_audit.section_statuses",
        "fingerprint_audit.source_status",
    ),
}


@dataclass(frozen=True, slots=True)
class FeedEpochEvidenceV1:
    """One bounded canonical fingerprint observation for epoch fitting."""

    symbol: str
    period: str
    start_timestamp_utc_ms: int
    end_timestamp_utc_ms: int
    fingerprint_id: str
    source_artifact_sha256: str
    source_hash_basis: str
    source_kind: str
    feature_values: Mapping[str, float]
    feature_provenance: Mapping[str, tuple[str, ...]]
    conditioning: Mapping[str, JSONValue]
    quality: Mapping[str, JSONValue]
    profile: Mapping[str, JSONValue]
    evidence_id: str = ""
    schema_version: str = FEED_EPOCH_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_EVIDENCE_SCHEMA_VERSION:
            raise ValueError("unsupported feed epoch evidence schema")
        symbol = _required_text(self.symbol, "symbol").upper()
        period = _required_text(self.period, "period")
        if not _valid_period(period):
            raise ValueError("period must use YYYY or YYYYMM")
        start = _strict_int(
            self.start_timestamp_utc_ms, "start_timestamp_utc_ms"
        )
        end = _strict_int(self.end_timestamp_utc_ms, "end_timestamp_utc_ms")
        if end < start:
            raise ValueError("evidence end timestamp precedes start timestamp")
        fingerprint_id = _required_sha256_id(
            self.fingerprint_id, "fingerprint_id"
        )
        source_hash = _required_sha256_id(
            self.source_artifact_sha256,
            "source_artifact_sha256",
        )
        source_kind = _required_text(self.source_kind, "source_kind")
        source_hash_basis = _required_text(
            self.source_hash_basis,
            "source_hash_basis",
        )
        if source_hash_basis not in _SOURCE_HASH_BASES:
            raise ValueError("unsupported source hash basis")
        feature_values = {
            _required_text(name, "feature name"): _finite_float(value, name)
            for name, value in sorted(self.feature_values.items())
        }
        if not feature_values:
            raise ValueError("feed epoch evidence requires feature values")
        if len(feature_values) > MAX_FEED_EPOCH_FEATURES:
            raise ValueError("feed epoch evidence exceeds feature limit")
        feature_provenance = {
            name: tuple(
                _required_text(path, "feature provenance")
                for path in self.feature_provenance.get(name, ())
            )
            for name in feature_values
        }
        if any(not paths for paths in feature_provenance.values()):
            raise ValueError("every feed epoch feature requires provenance")
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "start_timestamp_utc_ms", start)
        object.__setattr__(self, "end_timestamp_utc_ms", end)
        object.__setattr__(self, "fingerprint_id", fingerprint_id)
        object.__setattr__(self, "source_artifact_sha256", source_hash)
        object.__setattr__(self, "source_hash_basis", source_hash_basis)
        object.__setattr__(self, "source_kind", source_kind)
        object.__setattr__(self, "feature_values", feature_values)
        object.__setattr__(self, "feature_provenance", feature_provenance)
        object.__setattr__(self, "conditioning", dict(self.conditioning))
        object.__setattr__(self, "quality", dict(self.quality))
        object.__setattr__(self, "profile", dict(self.profile))
        expected = _stable_id("feed-epoch-evidence", self.identity_payload())
        supplied = str(self.evidence_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "evidence_id does not match deterministic identity"
            )
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return the complete deterministic evidence identity payload."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "period": self.period,
            "start_timestamp_utc_ms": self.start_timestamp_utc_ms,
            "end_timestamp_utc_ms": self.end_timestamp_utc_ms,
            "fingerprint_id": self.fingerprint_id,
            "source_artifact_sha256": self.source_artifact_sha256,
            "source_hash_basis": self.source_hash_basis,
            "source_kind": self.source_kind,
            "feature_values": dict(self.feature_values),
            "feature_provenance": {
                name: list(paths)
                for name, paths in sorted(self.feature_provenance.items())
            },
            "conditioning": dict(self.conditioning),
            "quality": dict(self.quality),
            "profile": dict(self.profile),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible evidence."""
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochEvidenceV1":
        """Read strict version-one evidence."""
        provenance = _mapping(data.get("feature_provenance"))
        return cls(
            schema_version=str(data.get("schema_version", "")),
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            start_timestamp_utc_ms=_strict_int(
                data.get("start_timestamp_utc_ms"),
                "start_timestamp_utc_ms",
            ),
            end_timestamp_utc_ms=_strict_int(
                data.get("end_timestamp_utc_ms"),
                "end_timestamp_utc_ms",
            ),
            fingerprint_id=str(data.get("fingerprint_id", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            source_hash_basis=str(data.get("source_hash_basis", "")),
            source_kind=str(data.get("source_kind", "")),
            feature_values={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(data.get("feature_values")).items()
            },
            feature_provenance={
                str(name): tuple(str(value) for value in _sequence(paths))
                for name, paths in provenance.items()
            },
            conditioning=_mapping(data.get("conditioning")),
            quality=_mapping(data.get("quality")),
            profile=_mapping(data.get("profile")),
            evidence_id=str(data.get("evidence_id", "")),
        )

    @classmethod
    def from_fingerprint(
        cls,
        payload: Mapping[str, Any],
        *,
        source_artifact_sha256: str | None = None,
    ) -> "FeedEpochEvidenceV1":
        """Build bounded epoch evidence from one canonical fingerprint."""
        if (
            payload.get("schema_version")
            != TIME_SERIES_FINGERPRINT_SCHEMA_VERSION
        ):
            raise ValueError(
                "feed epoch evidence requires a canonical v1 fingerprint"
            )
        axis = _mapping(payload.get("target_axis"))
        if str(axis.get("data_format", "")).lower() != "ascii":
            raise ValueError("feed epoch evidence requires ASCII fingerprints")
        if str(axis.get("timeframe", "")).upper() != "T":
            raise ValueError("feed epoch evidence requires tick fingerprints")
        coverage = _mapping(payload.get("coverage"))
        start = _strict_int(
            coverage.get("start_timestamp_utc_ms"),
            "coverage.start_timestamp_utc_ms",
        )
        end = _strict_int(
            coverage.get("end_timestamp_utc_ms"),
            "coverage.end_timestamp_utc_ms",
        )
        fingerprint_id = _required_sha256_id(
            payload.get("fingerprint_id"),
            "fingerprint_id",
        )
        feature_values = _fingerprint_feature_values(payload)
        source = _mapping(payload.get("source"))
        return cls(
            symbol=str(axis.get("symbol", "")),
            period=str(axis.get("period", "")),
            start_timestamp_utc_ms=start,
            end_timestamp_utc_ms=end,
            fingerprint_id=fingerprint_id,
            source_artifact_sha256=(source_artifact_sha256 or fingerprint_id),
            source_hash_basis=(
                "persisted_fingerprint_artifact_sha256"
                if source_artifact_sha256 is not None
                else "canonical_fingerprint_id"
            ),
            source_kind=str(source.get("kind", "unknown")),
            feature_values=feature_values,
            feature_provenance={
                name: _FEATURE_PROVENANCE[name] for name in feature_values
            },
            conditioning=_fingerprint_conditioning(payload),
            quality=_fingerprint_quality(payload),
            profile=_fingerprint_profile(payload, feature_values),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochFitConfigV1:
    """Deterministic and bounded feed-epoch fitting policy."""

    feature_names: tuple[str, ...] = DEFAULT_FEED_EPOCH_FEATURES
    min_evidence_periods: int = 6
    min_segment_periods: int = 2
    min_feature_coverage: float = 0.75
    min_change_score: float = 0.8
    min_boundary_support: float = 0.6
    boundary_match_tolerance_periods: int = 1
    max_evidence: int = MAX_FEED_EPOCH_EVIDENCE
    max_sensitivity_runs: int = 128
    rounding_digits: int = 8
    config_id: str = ""
    schema_version: str = FEED_EPOCH_FIT_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_FIT_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported feed epoch fit config schema")
        names = tuple(
            dict.fromkeys(
                _required_text(name, "feature") for name in self.feature_names
            )
        )
        if not names or len(names) > MAX_FEED_EPOCH_FEATURES:
            raise ValueError("feature_names must be non-empty and bounded")
        unknown = sorted(set(names).difference(DEFAULT_FEED_EPOCH_FEATURES))
        if unknown:
            raise ValueError(
                "unsupported feed epoch feature(s): " + ", ".join(unknown)
            )
        min_periods = _bounded_int(
            self.min_evidence_periods,
            "min_evidence_periods",
            minimum=2,
            maximum=MAX_FEED_EPOCH_EVIDENCE,
        )
        min_segment = _bounded_int(
            self.min_segment_periods,
            "min_segment_periods",
            minimum=1,
            maximum=MAX_FEED_EPOCH_EVIDENCE // 2,
        )
        if min_segment * 2 > min_periods:
            raise ValueError(
                "min_segment_periods cannot exceed half min_evidence_periods"
            )
        object.__setattr__(self, "feature_names", names)
        object.__setattr__(self, "min_evidence_periods", min_periods)
        object.__setattr__(self, "min_segment_periods", min_segment)
        object.__setattr__(
            self,
            "min_feature_coverage",
            _bounded_float(
                self.min_feature_coverage, "min_feature_coverage", 0.0, 1.0
            ),
        )
        object.__setattr__(
            self,
            "min_change_score",
            _bounded_float(
                self.min_change_score, "min_change_score", 0.0, 1_000_000.0
            ),
        )
        object.__setattr__(
            self,
            "min_boundary_support",
            _bounded_float(
                self.min_boundary_support, "min_boundary_support", 0.0, 1.0
            ),
        )
        object.__setattr__(
            self,
            "boundary_match_tolerance_periods",
            _bounded_int(
                self.boundary_match_tolerance_periods,
                "boundary_match_tolerance_periods",
                minimum=0,
                maximum=24,
            ),
        )
        object.__setattr__(
            self,
            "max_evidence",
            _bounded_int(
                self.max_evidence,
                "max_evidence",
                minimum=min_periods,
                maximum=MAX_FEED_EPOCH_EVIDENCE,
            ),
        )
        object.__setattr__(
            self,
            "max_sensitivity_runs",
            _bounded_int(
                self.max_sensitivity_runs,
                "max_sensitivity_runs",
                minimum=3,
                maximum=MAX_FEED_EPOCH_SENSITIVITY_RUNS,
            ),
        )
        object.__setattr__(
            self,
            "rounding_digits",
            _bounded_int(
                self.rounding_digits, "rounding_digits", minimum=0, maximum=12
            ),
        )
        expected = _stable_id("feed-epoch-config", self.identity_payload())
        supplied = str(self.config_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("config_id does not match deterministic identity")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return config fields that affect fitting semantics."""
        return {
            "schema_version": self.schema_version,
            "feature_names": list(self.feature_names),
            "min_evidence_periods": self.min_evidence_periods,
            "min_segment_periods": self.min_segment_periods,
            "min_feature_coverage": self.min_feature_coverage,
            "min_change_score": self.min_change_score,
            "min_boundary_support": self.min_boundary_support,
            "boundary_match_tolerance_periods": self.boundary_match_tolerance_periods,
            "max_evidence": self.max_evidence,
            "max_sensitivity_runs": self.max_sensitivity_runs,
            "rounding_digits": self.rounding_digits,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible configuration."""
        return {**self.identity_payload(), "config_id": self.config_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochFitConfigV1":
        """Read a strict fit configuration."""
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
            min_change_score=_finite_float(
                data.get("min_change_score"), "min_change_score"
            ),
            min_boundary_support=_finite_float(
                data.get("min_boundary_support"), "min_boundary_support"
            ),
            boundary_match_tolerance_periods=_strict_int(
                data.get("boundary_match_tolerance_periods"),
                "boundary_match_tolerance_periods",
            ),
            max_evidence=_strict_int(data.get("max_evidence"), "max_evidence"),
            max_sensitivity_runs=_strict_int(
                data.get("max_sensitivity_runs"),
                "max_sensitivity_runs",
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            config_id=str(data.get("config_id", "")),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochBoundaryV1:
    """One uncertain and stability-scored technological boundary."""

    boundary_id: str
    left_period: str
    right_period: str
    central_timestamp_utc_ms: int
    uncertainty_start_utc_ms: int
    uncertainty_end_utc_ms: int
    change_score: float
    confidence: float
    support: float
    support_by_analysis: Mapping[str, float]
    contributing_features: tuple[str, ...]
    transition_label: str
    schema_version: str = FEED_EPOCH_BOUNDARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_BOUNDARY_SCHEMA_VERSION:
            raise ValueError("unsupported feed epoch boundary schema")
        start = _strict_int(
            self.uncertainty_start_utc_ms, "uncertainty_start_utc_ms"
        )
        central = _strict_int(
            self.central_timestamp_utc_ms, "central_timestamp_utc_ms"
        )
        end = _strict_int(self.uncertainty_end_utc_ms, "uncertainty_end_utc_ms")
        if not start <= central <= end:
            raise ValueError(
                "boundary central timestamp must lie inside uncertainty interval"
            )
        object.__setattr__(self, "uncertainty_start_utc_ms", start)
        object.__setattr__(self, "central_timestamp_utc_ms", central)
        object.__setattr__(self, "uncertainty_end_utc_ms", end)
        for name in ("left_period", "right_period"):
            if not _valid_period(str(getattr(self, name))):
                raise ValueError(f"{name} must use YYYY or YYYYMM")
        object.__setattr__(
            self, "boundary_id", _required_text(self.boundary_id, "boundary_id")
        )
        object.__setattr__(
            self,
            "change_score",
            _finite_float(self.change_score, "change_score"),
        )
        object.__setattr__(
            self,
            "confidence",
            _bounded_float(self.confidence, "confidence", 0.0, 1.0),
        )
        object.__setattr__(
            self, "support", _bounded_float(self.support, "support", 0.0, 1.0)
        )
        object.__setattr__(
            self,
            "support_by_analysis",
            {
                str(name): _bounded_float(value, str(name), 0.0, 1.0)
                for name, value in sorted(self.support_by_analysis.items())
            },
        )
        object.__setattr__(
            self, "contributing_features", tuple(self.contributing_features)
        )
        object.__setattr__(
            self,
            "transition_label",
            _required_text(self.transition_label, "transition_label"),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic boundary metadata."""
        return {
            "schema_version": self.schema_version,
            "boundary_id": self.boundary_id,
            "left_period": self.left_period,
            "right_period": self.right_period,
            "central_timestamp_utc_ms": self.central_timestamp_utc_ms,
            "uncertainty_start_utc_ms": self.uncertainty_start_utc_ms,
            "uncertainty_end_utc_ms": self.uncertainty_end_utc_ms,
            "change_score": self.change_score,
            "confidence": self.confidence,
            "support": self.support,
            "support_by_analysis": dict(self.support_by_analysis),
            "contributing_features": list(self.contributing_features),
            "transition_label": self.transition_label,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochBoundaryV1":
        """Read a strict boundary contract."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            boundary_id=str(data.get("boundary_id", "")),
            left_period=str(data.get("left_period", "")),
            right_period=str(data.get("right_period", "")),
            central_timestamp_utc_ms=_strict_int(
                data.get("central_timestamp_utc_ms"), "central_timestamp_utc_ms"
            ),
            uncertainty_start_utc_ms=_strict_int(
                data.get("uncertainty_start_utc_ms"), "uncertainty_start_utc_ms"
            ),
            uncertainty_end_utc_ms=_strict_int(
                data.get("uncertainty_end_utc_ms"), "uncertainty_end_utc_ms"
            ),
            change_score=_finite_float(
                data.get("change_score"), "change_score"
            ),
            confidence=_finite_float(data.get("confidence"), "confidence"),
            support=_finite_float(data.get("support"), "support"),
            support_by_analysis={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(
                    data.get("support_by_analysis")
                ).items()
            },
            contributing_features=tuple(
                str(value)
                for value in _sequence(data.get("contributing_features"))
            ),
            transition_label=str(data.get("transition_label", "")),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochIntervalV1:
    """One central technological epoch between uncertain transitions."""

    epoch_id: str
    label: str
    period_start: str
    period_end: str
    start_timestamp_utc_ms: int
    end_timestamp_utc_ms: int
    evidence_count: int
    schema_version: str = FEED_EPOCH_INTERVAL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_INTERVAL_SCHEMA_VERSION:
            raise ValueError("unsupported feed epoch interval schema")
        start = _strict_int(
            self.start_timestamp_utc_ms, "start_timestamp_utc_ms"
        )
        end = _strict_int(self.end_timestamp_utc_ms, "end_timestamp_utc_ms")
        if end < start:
            raise ValueError("epoch end timestamp precedes start timestamp")
        for name in ("period_start", "period_end"):
            if not _valid_period(str(getattr(self, name))):
                raise ValueError(f"{name} must use YYYY or YYYYMM")
        if self.period_end < self.period_start:
            raise ValueError("epoch period_end precedes period_start")
        object.__setattr__(self, "start_timestamp_utc_ms", start)
        object.__setattr__(self, "end_timestamp_utc_ms", end)
        object.__setattr__(
            self, "epoch_id", _required_text(self.epoch_id, "epoch_id")
        )
        object.__setattr__(self, "label", _required_text(self.label, "label"))
        object.__setattr__(
            self,
            "evidence_count",
            _bounded_int(
                self.evidence_count,
                "evidence_count",
                1,
                MAX_FEED_EPOCH_EVIDENCE,
            ),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic interval metadata."""
        return {
            "schema_version": self.schema_version,
            "epoch_id": self.epoch_id,
            "label": self.label,
            "period_start": self.period_start,
            "period_end": self.period_end,
            "start_timestamp_utc_ms": self.start_timestamp_utc_ms,
            "end_timestamp_utc_ms": self.end_timestamp_utc_ms,
            "evidence_count": self.evidence_count,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochIntervalV1":
        """Read a strict interval contract."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            epoch_id=str(data.get("epoch_id", "")),
            label=str(data.get("label", "")),
            period_start=str(data.get("period_start", "")),
            period_end=str(data.get("period_end", "")),
            start_timestamp_utc_ms=_strict_int(
                data.get("start_timestamp_utc_ms"), "start_timestamp_utc_ms"
            ),
            end_timestamp_utc_ms=_strict_int(
                data.get("end_timestamp_utc_ms"), "end_timestamp_utc_ms"
            ),
            evidence_count=_strict_int(
                data.get("evidence_count"), "evidence_count"
            ),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochStabilityV1:
    """Deterministic sampling, missingness, and feature sensitivity evidence."""

    status: str
    run_counts: Mapping[str, int]
    usable_run_counts: Mapping[str, int]
    unstable_boundary_ids: tuple[str, ...] = ()
    limitations: tuple[str, ...] = ()
    schema_version: str = FEED_EPOCH_STABILITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_STABILITY_SCHEMA_VERSION:
            raise ValueError("unsupported feed epoch stability schema")
        if self.status not in {"pass", "limited", "fail"}:
            raise ValueError("unsupported feed epoch stability status")
        run_counts = _count_mapping(self.run_counts)
        usable_run_counts = _count_mapping(self.usable_run_counts)
        required = set(_STABILITY_ANALYSES)
        if set(run_counts) != required or set(usable_run_counts) != required:
            raise ValueError(
                "stability counts must cover every required analysis"
            )
        if any(usable_run_counts[name] > run_counts[name] for name in required):
            raise ValueError("usable stability runs cannot exceed planned runs")
        unstable = tuple(
            _required_text(value, "unstable_boundary_id")
            for value in self.unstable_boundary_ids
        )
        if len(set(unstable)) != len(unstable):
            raise ValueError("unstable boundary IDs must be unique")
        missing_analysis = any(
            usable_run_counts[name] == 0 for name in required
        )
        expected_status = (
            "fail" if unstable else ("limited" if missing_analysis else "pass")
        )
        if self.status != expected_status:
            raise ValueError(
                "stability status does not reconcile with its evidence"
            )
        object.__setattr__(self, "run_counts", run_counts)
        object.__setattr__(self, "usable_run_counts", usable_run_counts)
        object.__setattr__(self, "unstable_boundary_ids", unstable)
        object.__setattr__(self, "limitations", tuple(self.limitations))

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic stability metadata."""
        return {
            "schema_version": self.schema_version,
            "status": self.status,
            "run_counts": dict(self.run_counts),
            "usable_run_counts": dict(self.usable_run_counts),
            "unstable_boundary_ids": list(self.unstable_boundary_ids),
            "limitations": list(self.limitations),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochStabilityV1":
        """Read a strict stability contract."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            status=str(data.get("status", "")),
            run_counts={
                str(name): _strict_int(value, str(name))
                for name, value in _mapping(data.get("run_counts")).items()
            },
            usable_run_counts={
                str(name): _strict_int(value, str(name))
                for name, value in _mapping(
                    data.get("usable_run_counts")
                ).items()
            },
            unstable_boundary_ids=tuple(
                str(value)
                for value in _sequence(data.get("unstable_boundary_ids"))
            ),
            limitations=tuple(
                str(value) for value in _sequence(data.get("limitations"))
            ),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochAssignmentV1:
    """Stable epoch or transition assignment for one event timestamp."""

    definition_id: str
    symbol: str
    timestamp_utc_ms: int
    assignment_kind: str
    label: str
    epoch_id: str | None = None
    boundary_id: str | None = None
    schema_version: str = FEED_EPOCH_ASSIGNMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_ASSIGNMENT_SCHEMA_VERSION:
            raise ValueError("unsupported feed epoch assignment schema")
        kind = str(self.assignment_kind or "").strip()
        if kind not in {"epoch", "transition", "out_of_scope"}:
            raise ValueError("unsupported feed epoch assignment kind")
        object.__setattr__(
            self,
            "definition_id",
            _required_text(self.definition_id, "definition_id"),
        )
        object.__setattr__(
            self, "symbol", _required_text(self.symbol, "symbol").upper()
        )
        object.__setattr__(
            self,
            "timestamp_utc_ms",
            _strict_int(self.timestamp_utc_ms, "timestamp_utc_ms"),
        )
        object.__setattr__(self, "assignment_kind", kind)
        object.__setattr__(self, "label", _required_text(self.label, "label"))
        if kind == "epoch" and (not self.epoch_id or self.boundary_id):
            raise ValueError("epoch assignments require only epoch_id")
        if kind == "transition" and (not self.boundary_id or self.epoch_id):
            raise ValueError("transition assignments require only boundary_id")
        if kind == "out_of_scope" and (self.epoch_id or self.boundary_id):
            raise ValueError(
                "out-of-scope assignments cannot reference an epoch or boundary"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic assignment metadata."""
        return {
            "schema_version": self.schema_version,
            "definition_id": self.definition_id,
            "symbol": self.symbol,
            "timestamp_utc_ms": self.timestamp_utc_ms,
            "assignment_kind": self.assignment_kind,
            "label": self.label,
            "epoch_id": self.epoch_id,
            "boundary_id": self.boundary_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochAssignmentV1":
        """Read a strict assignment contract."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            definition_id=str(data.get("definition_id", "")),
            symbol=str(data.get("symbol", "")),
            timestamp_utc_ms=_strict_int(
                data.get("timestamp_utc_ms"),
                "timestamp_utc_ms",
            ),
            assignment_kind=str(data.get("assignment_kind", "")),
            label=str(data.get("label", "")),
            epoch_id=(
                str(data["epoch_id"])
                if data.get("epoch_id") is not None
                else None
            ),
            boundary_id=(
                str(data["boundary_id"])
                if data.get("boundary_id") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class FeedEpochDefinitionV1:
    """Versioned, lineage-complete technological feed-epoch artifact."""

    config: FeedEpochFitConfigV1
    symbols: tuple[str, ...]
    coverage_start_utc_ms: int
    coverage_end_utc_ms: int
    evidence_count: int
    period_count: int
    feature_names: tuple[str, ...]
    boundaries: tuple[FeedEpochBoundaryV1, ...]
    epochs: tuple[FeedEpochIntervalV1, ...]
    stability: FeedEpochStabilityV1
    lineage: Mapping[str, JSONValue]
    definition_id: str = ""
    schema_version: str = FEED_EPOCH_DEFINITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != FEED_EPOCH_DEFINITION_SCHEMA_VERSION:
            raise ValueError("unsupported feed epoch definition schema")
        symbols = tuple(
            sorted(
                {
                    _required_text(symbol, "symbol").upper()
                    for symbol in self.symbols
                }
            )
        )
        if not symbols:
            raise ValueError("feed epoch definition requires symbols")
        coverage_start = _strict_int(
            self.coverage_start_utc_ms,
            "coverage_start_utc_ms",
        )
        coverage_end = _strict_int(
            self.coverage_end_utc_ms,
            "coverage_end_utc_ms",
        )
        if coverage_end < coverage_start:
            raise ValueError("definition coverage end precedes start")
        if not self.epochs:
            raise ValueError(
                "feed epoch definition requires at least one epoch"
            )
        feature_names = tuple(self.feature_names)
        if not feature_names or not set(feature_names).issubset(
            self.config.feature_names
        ):
            raise ValueError(
                "definition feature_names must come from its fit config"
            )
        if len(self.epochs) != len(self.boundaries) + 1:
            raise ValueError(
                "feed epoch definitions require one more epoch than boundary"
            )
        if len({item.boundary_id for item in self.boundaries}) != len(
            self.boundaries
        ):
            raise ValueError("feed epoch boundary IDs must be unique")
        if len({item.epoch_id for item in self.epochs}) != len(self.epochs):
            raise ValueError("feed epoch IDs must be unique")
        if (
            tuple(
                sorted(
                    self.boundaries,
                    key=lambda item: item.central_timestamp_utc_ms,
                )
            )
            != self.boundaries
        ):
            raise ValueError("feed epoch boundaries must be time ordered")
        if (
            tuple(
                sorted(
                    self.epochs, key=lambda item: item.start_timestamp_utc_ms
                )
            )
            != self.epochs
        ):
            raise ValueError("feed epochs must be time ordered")
        for index, boundary in enumerate(self.boundaries):
            left_epoch = self.epochs[index]
            right_epoch = self.epochs[index + 1]
            if (
                boundary.left_period != left_epoch.period_end
                or boundary.right_period != right_epoch.period_start
            ):
                raise ValueError(
                    "feed epoch boundaries must align with adjacent epochs"
                )
            if (
                left_epoch.end_timestamp_utc_ms
                >= right_epoch.start_timestamp_utc_ms
            ):
                raise ValueError("adjacent feed epochs must not overlap")
        if self.epochs[0].start_timestamp_utc_ms != coverage_start:
            raise ValueError("first epoch must start at definition coverage")
        if self.epochs[-1].end_timestamp_utc_ms != coverage_end:
            raise ValueError("last epoch must end at definition coverage")
        lineage = dict(self.lineage)
        if lineage.get("config_id") != self.config.config_id:
            raise ValueError("definition lineage config_id mismatch")
        if (
            _strict_int(lineage.get("source_count"), "lineage.source_count")
            != self.evidence_count
        ):
            raise ValueError("definition lineage source_count mismatch")
        sources = _sequence(lineage.get("sources"))
        if len(sources) != self.evidence_count:
            raise ValueError("definition lineage must include every source")
        source_rows = tuple(_mapping(source) for source in sources)
        if any(not source for source in source_rows):
            raise ValueError(
                "definition lineage source entries must be objects"
            )
        evidence_ids = [
            _required_text(source.get("evidence_id"), "lineage evidence_id")
            for source in source_rows
        ]
        if len(set(evidence_ids)) != len(evidence_ids):
            raise ValueError("definition lineage evidence IDs must be unique")
        for source in source_rows:
            _required_sha256_id(
                source.get("fingerprint_id"),
                "lineage fingerprint_id",
            )
            _required_sha256_id(
                source.get("source_artifact_sha256"),
                "lineage source_artifact_sha256",
            )
            _required_text(source.get("symbol"), "lineage symbol")
            if not _valid_period(str(source.get("period", ""))):
                raise ValueError("lineage period must use YYYY or YYYYMM")
            _required_text(source.get("source_kind"), "lineage source_kind")
            source_hash_basis = _required_text(
                source.get("source_hash_basis"),
                "lineage source_hash_basis",
            )
            if source_hash_basis not in _SOURCE_HASH_BASES:
                raise ValueError("unsupported lineage source hash basis")
        source_kind_counts = Counter(
            str(source["source_kind"]) for source in source_rows
        )
        if _count_mapping_unbounded(lineage.get("source_kind_counts")) != dict(
            sorted(source_kind_counts.items())
        ):
            raise ValueError("definition lineage source-kind counts mismatch")
        canonical_sources = tuple(
            _mapping(source)
            for source in _sequence(lineage.get("canonical_sources"))
        )
        if any(not source for source in canonical_sources):
            raise ValueError("canonical lineage source entries must be objects")
        canonical_source_count = _strict_int(
            lineage.get("canonical_source_count"),
            "lineage.canonical_source_count",
        )
        if canonical_source_count != len(canonical_sources):
            raise ValueError("canonical lineage source count mismatch")
        canonical_axes: list[tuple[str, str, str]] = []
        for source in canonical_sources:
            fingerprint_id = _required_sha256_id(
                source.get("fingerprint_id"),
                "canonical lineage fingerprint_id",
            )
            _required_sha256_id(
                source.get("source_artifact_sha256"),
                "canonical lineage source_artifact_sha256",
            )
            symbol = _required_text(
                source.get("symbol"), "canonical lineage symbol"
            )
            period = str(source.get("period", ""))
            if not _valid_period(period):
                raise ValueError(
                    "canonical lineage period must use YYYY or YYYYMM"
                )
            source_hash_basis = _required_text(
                source.get("source_hash_basis"),
                "canonical lineage source_hash_basis",
            )
            if source_hash_basis not in _SOURCE_HASH_BASES:
                raise ValueError(
                    "unsupported canonical lineage source hash basis"
                )
            _required_text(
                source.get("source_kind"), "canonical lineage source_kind"
            )
            canonical_axes.append((symbol, period, fingerprint_id))
        if len(set(canonical_axes)) != len(canonical_axes):
            raise ValueError("canonical lineage sources must be unique")
        unstable_ids = set(self.stability.unstable_boundary_ids)
        boundary_ids = {item.boundary_id for item in self.boundaries}
        if not unstable_ids.issubset(boundary_ids):
            raise ValueError("stability references an unknown boundary")
        if (
            sum(self.stability.run_counts.values())
            > self.config.max_sensitivity_runs
        ):
            raise ValueError(
                "definition stability exceeds configured run limit"
            )
        supplied_lineage_hash = _required_sha256_id(
            lineage.get("lineage_sha256"),
            "lineage_sha256",
        )
        expected_lineage_hash = _payload_sha256(_lineage_hash_material(lineage))
        if supplied_lineage_hash != expected_lineage_hash:
            raise ValueError("definition lineage hash mismatch")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "coverage_start_utc_ms", coverage_start)
        object.__setattr__(self, "coverage_end_utc_ms", coverage_end)
        object.__setattr__(
            self,
            "evidence_count",
            _bounded_int(
                self.evidence_count,
                "evidence_count",
                1,
                self.config.max_evidence,
            ),
        )
        object.__setattr__(
            self,
            "period_count",
            _bounded_int(
                self.period_count, "period_count", 1, self.evidence_count
            ),
        )
        object.__setattr__(self, "feature_names", feature_names)
        object.__setattr__(self, "lineage", lineage)
        expected = _stable_id("feed-epoch-definition", self.identity_payload())
        supplied = str(self.definition_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "definition_id does not match deterministic identity"
            )
        object.__setattr__(self, "definition_id", expected)

    @property
    def valid_for_observation_models(self) -> bool:
        """Return whether downstream observation models may consume this artifact."""
        return self.stability.status == "pass"

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return all definition fields that determine semantic identity."""
        return {
            "schema_version": self.schema_version,
            "config": self.config.to_dict(),
            "symbols": list(self.symbols),
            "coverage_start_utc_ms": self.coverage_start_utc_ms,
            "coverage_end_utc_ms": self.coverage_end_utc_ms,
            "evidence_count": self.evidence_count,
            "period_count": self.period_count,
            "feature_names": list(self.feature_names),
            "boundaries": [boundary.to_dict() for boundary in self.boundaries],
            "epochs": [epoch.to_dict() for epoch in self.epochs],
            "stability": self.stability.to_dict(),
            "lineage": dict(self.lineage),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible artifact metadata."""
        return {
            **self.identity_payload(),
            "definition_id": self.definition_id,
            "valid_for_observation_models": self.valid_for_observation_models,
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "FeedEpochDefinitionV1":
        """Read a strict definition and verify its identity."""
        return cls(
            schema_version=str(data.get("schema_version", "")),
            config=FeedEpochFitConfigV1.from_dict(_mapping(data.get("config"))),
            symbols=tuple(
                str(value) for value in _sequence(data.get("symbols"))
            ),
            coverage_start_utc_ms=_strict_int(
                data.get("coverage_start_utc_ms"), "coverage_start_utc_ms"
            ),
            coverage_end_utc_ms=_strict_int(
                data.get("coverage_end_utc_ms"), "coverage_end_utc_ms"
            ),
            evidence_count=_strict_int(
                data.get("evidence_count"), "evidence_count"
            ),
            period_count=_strict_int(data.get("period_count"), "period_count"),
            feature_names=tuple(
                str(value) for value in _sequence(data.get("feature_names"))
            ),
            boundaries=tuple(
                FeedEpochBoundaryV1.from_dict(_mapping(value))
                for value in _sequence(data.get("boundaries"))
            ),
            epochs=tuple(
                FeedEpochIntervalV1.from_dict(_mapping(value))
                for value in _sequence(data.get("epochs"))
            ),
            stability=FeedEpochStabilityV1.from_dict(
                _mapping(data.get("stability"))
            ),
            lineage=_mapping(data.get("lineage")),
            definition_id=str(data.get("definition_id", "")),
        )

    @classmethod
    def from_json(cls, value: str) -> "FeedEpochDefinitionV1":
        """Read a strict definition from JSON."""
        data = json.loads(value)
        if not isinstance(data, Mapping):
            raise ValueError("feed epoch definition JSON must be an object")
        return cls.from_dict(data)

    def assign(
        self,
        *,
        symbol: str,
        timestamp_utc_ms: int,
        require_stable: bool = True,
    ) -> FeedEpochAssignmentV1:
        """Assign an event to an epoch or explicit transition interval."""
        normalized_symbol = _required_text(symbol, "symbol").upper()
        timestamp = _strict_int(timestamp_utc_ms, "timestamp_utc_ms")
        if require_stable and not self.valid_for_observation_models:
            raise ValueError(
                "feed epoch definition has not passed stability checks"
            )
        if normalized_symbol not in self.symbols:
            return FeedEpochAssignmentV1(
                definition_id=self.definition_id,
                symbol=normalized_symbol,
                timestamp_utc_ms=timestamp,
                assignment_kind="out_of_scope",
                label="symbol_out_of_scope",
            )
        if (
            not self.coverage_start_utc_ms
            <= timestamp
            <= self.coverage_end_utc_ms
        ):
            return FeedEpochAssignmentV1(
                definition_id=self.definition_id,
                symbol=normalized_symbol,
                timestamp_utc_ms=timestamp,
                assignment_kind="out_of_scope",
                label="timestamp_out_of_scope",
            )
        for boundary in self.boundaries:
            if (
                boundary.uncertainty_start_utc_ms
                <= timestamp
                <= boundary.uncertainty_end_utc_ms
            ):
                return FeedEpochAssignmentV1(
                    definition_id=self.definition_id,
                    symbol=normalized_symbol,
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
                return FeedEpochAssignmentV1(
                    definition_id=self.definition_id,
                    symbol=normalized_symbol,
                    timestamp_utc_ms=timestamp,
                    assignment_kind="epoch",
                    label=epoch.label,
                    epoch_id=epoch.epoch_id,
                )
        return FeedEpochAssignmentV1(
            definition_id=self.definition_id,
            symbol=normalized_symbol,
            timestamp_utc_ms=timestamp,
            assignment_kind="out_of_scope",
            label="unassigned_gap",
        )


@dataclass(frozen=True, slots=True)
class _PeriodPoint:
    """One deterministic panel aggregate used internally by the fitter."""

    period: str
    start_utc_ms: int
    end_utc_ms: int
    evidence_count: int
    values: Mapping[str, float]


@dataclass(frozen=True, slots=True)
class _Candidate:
    """One boundary candidate before stability enrichment."""

    left_index: int
    right_index: int
    left_period: str
    right_period: str
    left_end_utc_ms: int
    right_start_utc_ms: int
    score: float
    feature_differences: Mapping[str, float]


@dataclass(frozen=True, slots=True)
class _SensitivityVariant:
    """One deterministic perturbation of period points or features."""

    analysis: str
    points: tuple[_PeriodPoint, ...]
    features: tuple[str, ...]


def fit_feed_epochs(
    evidence: Iterable[FeedEpochEvidenceV1],
    *,
    config: FeedEpochFitConfigV1 | None = None,
) -> FeedEpochDefinitionV1:
    """Fit uncertainty-aware technological epochs from canonical evidence."""
    policy = config or FeedEpochFitConfigV1()
    ordered = tuple(
        sorted(
            evidence,
            key=lambda item: (
                item.start_timestamp_utc_ms,
                item.period,
                item.symbol,
                item.evidence_id,
            ),
        )
    )
    if not ordered:
        raise ValueError("feed epoch fitting requires canonical evidence")
    if len(ordered) > policy.max_evidence:
        raise ValueError("feed epoch evidence exceeds configured maximum")
    evidence_ids = [item.evidence_id for item in ordered]
    if len(set(evidence_ids)) != len(evidence_ids):
        raise ValueError("duplicate feed epoch evidence_id")
    source_axes = [(item.symbol, item.period) for item in ordered]
    if len(set(source_axes)) != len(source_axes):
        raise ValueError("feed epoch evidence duplicates a symbol-period axis")

    points = _aggregate_period_points(ordered, policy)
    if len(points) < policy.min_evidence_periods:
        raise ValueError(
            "feed epoch fitting requires at least "
            f"{policy.min_evidence_periods} distinct periods"
        )
    feature_names = _eligible_features(points, policy)
    if len(feature_names) < 2:
        raise ValueError(
            "feed epoch fitting requires at least two covered features"
        )
    candidates = _detect_boundaries(points, feature_names, policy)
    variants = _sensitivity_variants(points, feature_names, policy)
    boundaries, stability = _stabilize_boundaries(
        points,
        feature_names,
        candidates,
        variants,
        policy,
    )
    epochs = _epoch_intervals(points, boundaries)
    lineage = _definition_lineage(ordered, feature_names, policy)
    return FeedEpochDefinitionV1(
        config=policy,
        symbols=tuple(sorted({item.symbol for item in ordered})),
        coverage_start_utc_ms=min(
            item.start_timestamp_utc_ms for item in ordered
        ),
        coverage_end_utc_ms=max(item.end_timestamp_utc_ms for item in ordered),
        evidence_count=len(ordered),
        period_count=len(points),
        feature_names=feature_names,
        boundaries=boundaries,
        epochs=epochs,
        stability=stability,
        lineage=lineage,
    )


def feed_epoch_definition_to_json(definition: FeedEpochDefinitionV1) -> str:
    """Return deterministic formatted artifact JSON."""
    return json.dumps(definition.to_dict(), indent=2, sort_keys=True)


def write_feed_epoch_definition(
    definition: FeedEpochDefinitionV1,
    path: str | Path,
) -> ArtifactRef:
    """Write a definition artifact and return a compact reference."""
    output = Path(path).expanduser()
    output.parent.mkdir(parents=True, exist_ok=True)
    encoded = f"{feed_epoch_definition_to_json(definition)}\n".encode("utf-8")
    output.write_bytes(encoded)
    return ArtifactRef(
        kind="feed-epoch-definition",
        path=str(output.resolve()),
        size_bytes=len(encoded),
        sha256=hashlib.sha256(encoded).hexdigest(),
        metadata={
            "schema_version": definition.schema_version,
            "definition_id": definition.definition_id,
            "stability_status": definition.stability.status,
            "valid_for_observation_models": (
                definition.valid_for_observation_models
            ),
            "symbol_count": len(definition.symbols),
            "period_count": definition.period_count,
            "boundary_count": len(definition.boundaries),
        },
    )


def _fingerprint_feature_values(payload: Mapping[str, Any]) -> dict[str, float]:
    coverage = _mapping(payload.get("coverage"))
    topology = _mapping(payload.get("temporal_topology"))
    distribution = _mapping(payload.get("tick_distribution"))
    dynamics = _mapping(payload.get("microstructure_dynamics"))
    audit = _mapping(payload.get("fingerprint_audit"))
    row_count = _nonnegative_int(coverage.get("row_count"))
    duration_ms = _optional_finite_float(coverage.get("duration_ms"))
    values: dict[str, float] = {}
    if duration_ms is not None and duration_ms > 0.0:
        values["log_tick_rate_per_hour"] = math.log1p(
            row_count * 3_600_000.0 / duration_ms
        )
    interarrival = _mapping(dynamics.get("interarrival_ms"))
    median_interval = _optional_finite_float(interarrival.get("median"))
    if median_interval is not None and median_interval >= 0.0:
        values["log_median_interarrival_ms"] = math.log1p(median_interval)
    p95_interval = _quantile_value(interarrival, 0.95)
    if p95_interval is not None and p95_interval >= 0.0:
        values["log_p95_interarrival_ms"] = math.log1p(p95_interval)
    min_interval = _optional_finite_float(topology.get("min_interval_ms"))
    if min_interval is not None and min_interval >= 0.0:
        values["minimum_observed_interval_ms"] = min_interval
    precision = _price_precision_digits(distribution)
    if precision is not None:
        values["price_precision_digits"] = precision
    spread = _mapping(distribution.get("spread"))
    spread_median = _optional_finite_float(spread.get("median"))
    if spread_median is not None:
        values["spread_median"] = spread_median
    conditioned_spread = _conditioned_spread_median(payload)
    if conditioned_spread is not None:
        values["conditioned_spread_median"] = conditioned_spread
    absolute_spread_change = _mapping(dynamics.get("absolute_spread_change"))
    spread_change_median = _optional_finite_float(
        absolute_spread_change.get("median")
    )
    if spread_change_median is not None:
        values["absolute_spread_change_median"] = spread_change_median
    stale = _mapping(dynamics.get("stale_quote"))
    stale_rate = _optional_finite_float(stale.get("repeat_rate"))
    if stale_rate is not None:
        values["stale_repeat_rate"] = stale_rate
    burst = _mapping(dynamics.get("burst"))
    burst_rate = _optional_finite_float(burst.get("burst_rate"))
    if burst_rate is not None:
        values["burst_rate"] = burst_rate
    duplicate_count = _nonnegative_int(
        topology.get("duplicate_timestamp_count")
    )
    parsed_count = _nonnegative_int(topology.get("parsed_row_count"))
    if parsed_count:
        values["duplicate_timestamp_rate"] = duplicate_count / parsed_count
    suspicious_count = _nonnegative_int(topology.get("suspicious_gap_count"))
    interval_count = _nonnegative_int(topology.get("interval_count"))
    if interval_count:
        values["suspicious_gap_rate"] = suspicious_count / interval_count
    statuses = _mapping(audit.get("section_statuses"))
    if statuses:
        limited = sum(
            1
            for status in statuses.values()
            if str(status) in {"limited", "skipped", "unavailable"}
        )
        values["source_quality_penalty"] = limited / len(statuses)
    return values


def _fingerprint_conditioning(
    payload: Mapping[str, Any],
) -> dict[str, JSONValue]:
    calendar = _mapping(payload.get("calendar_regimes"))
    conditional = _mapping(payload.get("conditional_distributions"))
    return {
        "calendar_schema_version": calendar.get("schema_version"),
        "calendar_status": calendar.get("status", "unavailable"),
        "calendar_profile_name": calendar.get("calendar_profile_name"),
        "calendar_profile_version": calendar.get("calendar_profile_version"),
        "calendar_profile_complete": calendar.get("calendar_profile_complete"),
        "session_state_counts": dict(
            _mapping(calendar.get("session_state_counts"))
        ),
        "active_session_counts": dict(
            _mapping(calendar.get("active_session_counts"))
        ),
        "special_tag_counts": dict(
            _mapping(calendar.get("special_tag_counts"))
        ),
        "holiday_tag_counts": dict(
            _mapping(calendar.get("holiday_tag_counts"))
        ),
        "event_tag_counts": dict(_mapping(calendar.get("event_tag_counts"))),
        "conditional_schema_version": conditional.get("schema_version"),
        "conditional_status": conditional.get("status", "unavailable"),
        "conditioning_payload_sha256": _payload_sha256(
            {"calendar": dict(calendar), "conditional": dict(conditional)}
        ),
    }


def _fingerprint_quality(payload: Mapping[str, Any]) -> dict[str, JSONValue]:
    audit = _mapping(payload.get("fingerprint_audit"))
    topology = _mapping(payload.get("temporal_topology"))
    dynamics = _mapping(payload.get("microstructure_dynamics"))
    source = _mapping(payload.get("source"))
    return {
        "audit_schema_version": audit.get("schema_version"),
        "section_statuses": dict(_mapping(audit.get("section_statuses"))),
        "sections_skipped": dict(_mapping(audit.get("sections_skipped"))),
        "source_status": dict(_mapping(audit.get("source_status"))),
        "source_kind": source.get("kind", "unknown"),
        "calculation_basis": dynamics.get(
            "basis", topology.get("sampling_basis")
        ),
        "computed_from": dynamics.get(
            "computed_from", topology.get("computed_from")
        ),
        "cache_source": dynamics.get(
            "cache_source", topology.get("cache_source")
        ),
        "sequence_status": dynamics.get("sequence_status", "unavailable"),
        "limitations": list(_sequence(dynamics.get("limitations"))),
    }


def _fingerprint_profile(
    payload: Mapping[str, Any],
    feature_values: Mapping[str, float],
) -> dict[str, JSONValue]:
    coverage = _mapping(payload.get("coverage"))
    topology = _mapping(payload.get("temporal_topology"))
    distribution = _mapping(payload.get("tick_distribution"))
    dynamics = _mapping(payload.get("microstructure_dynamics"))
    calendar = _mapping(payload.get("calendar_regimes"))
    interarrival = _mapping(dynamics.get("interarrival_ms"))
    stale = _mapping(dynamics.get("stale_quote"))
    spread = _mapping(distribution.get("spread"))
    row_count = _nonnegative_int(coverage.get("row_count"))
    repeat_count = _nonnegative_int(stale.get("repeat_count"))
    interval_count = max(0, row_count - 1)
    tick_rate_log = feature_values.get("log_tick_rate_per_hour")
    return {
        "row_count": row_count,
        "tick_rate_per_hour": (
            math.expm1(tick_rate_log) if tick_rate_log is not None else 0.0
        ),
        "median_interarrival_ms": _optional_finite_float(
            interarrival.get("median")
        )
        or 0.0,
        "p95_interarrival_ms": _quantile_value(interarrival, 0.95) or 0.0,
        "max_interarrival_ms": _optional_finite_float(interarrival.get("max"))
        or 0.0,
        "quiet_gap_count": _nonnegative_int(
            topology.get("suspicious_gap_count")
        ),
        "quote_update_count": max(0, interval_count - repeat_count),
        "quote_update_ratio": (
            max(0, interval_count - repeat_count) / interval_count
            if interval_count
            else 0.0
        ),
        "zero_change_run_count": _nonnegative_int(stale.get("run_count")),
        "zero_change_tick_count": _nonnegative_int(
            stale.get("affected_row_count")
        ),
        "spread_min": _optional_finite_float(spread.get("min")) or 0.0,
        "spread_median": _optional_finite_float(spread.get("median")) or 0.0,
        "spread_mean": _optional_finite_float(spread.get("mean")) or 0.0,
        "spread_max": _optional_finite_float(spread.get("max")) or 0.0,
        "session_counts": dict(_mapping(calendar.get("active_session_counts"))),
    }


def _aggregate_period_points(
    evidence: Sequence[FeedEpochEvidenceV1],
    config: FeedEpochFitConfigV1,
) -> tuple[_PeriodPoint, ...]:
    normalized_by_evidence = _symbol_normalized_evidence_values(
        evidence,
        config.feature_names,
    )
    grouped: dict[str, list[FeedEpochEvidenceV1]] = defaultdict(list)
    for item in evidence:
        grouped[item.period].append(item)
    points: list[_PeriodPoint] = []
    for period, items in grouped.items():
        values: dict[str, float] = {}
        for feature in config.feature_names:
            observed = [
                normalized_by_evidence[item.evidence_id][feature]
                for item in items
                if feature in normalized_by_evidence[item.evidence_id]
            ]
            if observed:
                values[feature] = float(median(observed))
        points.append(
            _PeriodPoint(
                period=period,
                start_utc_ms=min(item.start_timestamp_utc_ms for item in items),
                end_utc_ms=max(item.end_timestamp_utc_ms for item in items),
                evidence_count=len(items),
                values=values,
            )
        )
    return tuple(
        sorted(points, key=lambda item: (item.start_utc_ms, item.period))
    )


def _symbol_normalized_evidence_values(
    evidence: Sequence[FeedEpochEvidenceV1],
    features: Sequence[str],
) -> dict[str, dict[str, float]]:
    """Normalize within symbols so panel membership cannot create epochs."""
    grouped: dict[str, list[FeedEpochEvidenceV1]] = defaultdict(list)
    for item in evidence:
        grouped[item.symbol].append(item)
    normalized: dict[str, dict[str, float]] = {
        item.evidence_id: {} for item in evidence
    }
    for items in grouped.values():
        for feature in features:
            observed = [
                float(item.feature_values[feature])
                for item in items
                if feature in item.feature_values
            ]
            if not observed:
                continue
            center = float(median(observed))
            deviations = [abs(value - center) for value in observed]
            scale = float(median(deviations)) * 1.4826
            if scale <= 0.0:
                scale = max(observed) - min(observed)
            for item in items:
                if feature not in item.feature_values:
                    continue
                value = float(item.feature_values[feature])
                normalized[item.evidence_id][feature] = (
                    (value - center) / scale if scale > 0.0 else 0.0
                )
    return normalized


def _eligible_features(
    points: Sequence[_PeriodPoint],
    config: FeedEpochFitConfigV1,
) -> tuple[str, ...]:
    minimum = max(2, math.ceil(len(points) * config.min_feature_coverage))
    return tuple(
        feature
        for feature in config.feature_names
        if sum(feature in point.values for point in points) >= minimum
    )


def _detect_boundaries(
    points: Sequence[_PeriodPoint],
    features: Sequence[str],
    config: FeedEpochFitConfigV1,
) -> tuple[_Candidate, ...]:
    if len(points) < max(2, config.min_segment_periods * 2):
        return ()
    normalized = _normalized_feature_values(points, features)
    candidates: list[_Candidate] = []
    for right_index in range(1, len(points)):
        if right_index < config.min_segment_periods:
            continue
        if len(points) - right_index < config.min_segment_periods:
            continue
        differences = {
            feature: abs(
                normalized[feature][right_index]
                - normalized[feature][right_index - 1]
            )
            for feature in normalized
            if right_index < len(normalized[feature])
        }
        if not differences:
            continue
        score = sum(differences.values()) / len(differences)
        if score < config.min_change_score:
            continue
        left = points[right_index - 1]
        right = points[right_index]
        candidates.append(
            _Candidate(
                left_index=right_index - 1,
                right_index=right_index,
                left_period=left.period,
                right_period=right.period,
                left_end_utc_ms=left.end_utc_ms,
                right_start_utc_ms=right.start_utc_ms,
                score=score,
                feature_differences=differences,
            )
        )
    return _select_separated_candidates(candidates, len(points), config)


def _normalized_feature_values(
    points: Sequence[_PeriodPoint],
    features: Sequence[str],
) -> dict[str, list[float]]:
    result: dict[str, list[float]] = {}
    for feature in features:
        observed = [point.values.get(feature) for point in points]
        finite = [float(value) for value in observed if value is not None]
        if len(finite) < 2:
            continue
        center = float(median(finite))
        deviations = [abs(value - center) for value in finite]
        scale = float(median(deviations)) * 1.4826
        if scale <= 0.0:
            scale = max(finite) - min(finite)
        if scale <= 0.0:
            continue
        result[feature] = [
            ((float(value) - center) / scale if value is not None else 0.0)
            for value in observed
        ]
    return result


def _select_separated_candidates(
    candidates: Sequence[_Candidate],
    point_count: int,
    config: FeedEpochFitConfigV1,
) -> tuple[_Candidate, ...]:
    selected: list[_Candidate] = []
    for candidate in sorted(
        candidates, key=lambda item: (-item.score, item.right_index)
    ):
        if candidate.right_index < config.min_segment_periods:
            continue
        if point_count - candidate.right_index < config.min_segment_periods:
            continue
        if any(
            abs(candidate.right_index - existing.right_index)
            < config.min_segment_periods
            for existing in selected
        ):
            continue
        selected.append(candidate)
    return tuple(sorted(selected, key=lambda item: item.right_index))


def _sensitivity_variants(
    points: tuple[_PeriodPoint, ...],
    features: tuple[str, ...],
    config: FeedEpochFitConfigV1,
) -> tuple[_SensitivityVariant, ...]:
    variants: list[_SensitivityVariant] = []
    for offset in (0, 1):
        sampled = points[offset::2]
        if len(sampled) >= config.min_evidence_periods:
            variants.append(_SensitivityVariant("sampling", sampled, features))
    for index in range(1, len(points) - 1):
        reduced = points[:index] + points[index + 1 :]
        if len(reduced) >= config.min_evidence_periods:
            variants.append(
                _SensitivityVariant("missing_periods", reduced, features)
            )
    for feature in features:
        reduced_features = tuple(name for name in features if name != feature)
        if len(reduced_features) >= 2:
            variants.append(
                _SensitivityVariant("feature_removal", points, reduced_features)
            )
    by_analysis: dict[str, list[_SensitivityVariant]] = defaultdict(list)
    for variant in variants:
        by_analysis[variant.analysis].append(variant)
    for analysis in by_analysis:
        by_analysis[analysis].sort(
            key=lambda item: (
                tuple(point.period for point in item.points),
                item.features,
            )
        )
    selected: list[_SensitivityVariant] = []
    for analysis in _STABILITY_ANALYSES:
        if by_analysis[analysis]:
            selected.append(by_analysis[analysis].pop(0))
    remaining = config.max_sensitivity_runs - len(selected)
    for analysis in ("sampling", "feature_removal"):
        if remaining <= 0:
            break
        additions = by_analysis[analysis][:remaining]
        selected.extend(additions)
        remaining -= len(additions)
    if remaining > 0:
        selected.extend(
            _evenly_spaced_variants(by_analysis["missing_periods"], remaining)
        )
    return tuple(selected)


def _evenly_spaced_variants(
    variants: Sequence[_SensitivityVariant],
    limit: int,
) -> tuple[_SensitivityVariant, ...]:
    """Select a deterministic whole-history sample under a bounded budget."""
    if limit <= 0 or not variants:
        return ()
    if len(variants) <= limit:
        return tuple(variants)
    if limit == 1:
        return (variants[len(variants) // 2],)
    indices = {
        round(offset * (len(variants) - 1) / (limit - 1))
        for offset in range(limit)
    }
    return tuple(variants[index] for index in sorted(indices))


def _stabilize_boundaries(
    points: tuple[_PeriodPoint, ...],
    features: tuple[str, ...],
    candidates: tuple[_Candidate, ...],
    variants: tuple[_SensitivityVariant, ...],
    config: FeedEpochFitConfigV1,
) -> tuple[tuple[FeedEpochBoundaryV1, ...], FeedEpochStabilityV1]:
    run_counts: Counter[str] = Counter()
    usable_counts: Counter[str] = Counter()
    variant_candidates: list[tuple[str, tuple[_Candidate, ...]]] = []
    for variant in variants:
        run_counts[variant.analysis] += 1
        detected = _detect_boundaries(variant.points, variant.features, config)
        usable_counts[variant.analysis] += 1
        variant_candidates.append((variant.analysis, detected))
    for analysis in _STABILITY_ANALYSES:
        run_counts.setdefault(analysis, 0)
        usable_counts.setdefault(analysis, 0)

    positions = {point.period: index for index, point in enumerate(points)}
    boundaries: list[FeedEpochBoundaryV1] = []
    unstable_ids: list[str] = []
    limitations: list[str] = []
    for index, candidate in enumerate(candidates, start=1):
        boundary_id = f"boundary-{index:03d}"
        matches_by_analysis: Counter[str] = Counter()
        matched_intervals = [
            (candidate.left_end_utc_ms, candidate.right_start_utc_ms)
        ]
        for analysis, detected in variant_candidates:
            match = _matching_candidate(candidate, detected, positions, config)
            if match is None:
                continue
            matches_by_analysis[analysis] += 1
            matched_intervals.append(
                (match.left_end_utc_ms, match.right_start_utc_ms)
            )
        support_by_analysis = {
            analysis: (
                matches_by_analysis[analysis] / usable_counts[analysis]
                if usable_counts[analysis]
                else 0.0
            )
            for analysis in sorted(run_counts)
        }
        total_runs = sum(usable_counts.values())
        support = (
            sum(matches_by_analysis.values()) / total_runs
            if total_runs
            else 0.0
        )
        available_support = [
            support_by_analysis[name]
            for name in support_by_analysis
            if usable_counts[name]
        ]
        gate_support = min(available_support) if available_support else 0.0
        if gate_support < config.min_boundary_support:
            unstable_ids.append(boundary_id)
        uncertainty_values = [
            value for interval in matched_intervals for value in interval
        ]
        uncertainty_start = min(uncertainty_values)
        uncertainty_end = max(uncertainty_values)
        if uncertainty_end < uncertainty_start:
            uncertainty_start, uncertainty_end = (
                uncertainty_end,
                uncertainty_start,
            )
        central = (
            candidate.left_end_utc_ms
            + (candidate.right_start_utc_ms - candidate.left_end_utc_ms) // 2
        )
        confidence = (
            min(
                1.0,
                (candidate.score / max(config.min_change_score, 1e-12)) / 2.0,
            )
            * support
        )
        contributing = tuple(
            name
            for name, _ in sorted(
                candidate.feature_differences.items(),
                key=lambda item: (-item[1], item[0]),
            )[:8]
        )
        boundaries.append(
            FeedEpochBoundaryV1(
                boundary_id=boundary_id,
                left_period=candidate.left_period,
                right_period=candidate.right_period,
                central_timestamp_utc_ms=central,
                uncertainty_start_utc_ms=uncertainty_start,
                uncertainty_end_utc_ms=uncertainty_end,
                change_score=_rounded(candidate.score, config.rounding_digits),
                confidence=_rounded(confidence, config.rounding_digits),
                support=_rounded(support, config.rounding_digits),
                support_by_analysis={
                    name: _rounded(value, config.rounding_digits)
                    for name, value in support_by_analysis.items()
                },
                contributing_features=contributing,
                transition_label=(
                    f"transition:epoch-{index:03d}:epoch-{index + 1:03d}"
                ),
            )
        )

    missing_analyses = [
        analysis for analysis, count in usable_counts.items() if count == 0
    ]
    limitations.extend(
        f"{analysis}_sensitivity_unavailable" for analysis in missing_analyses
    )
    if unstable_ids:
        limitations.append("unstable_boundaries")
    extra_boundary_run_count = sum(
        1
        for _, detected in variant_candidates
        if len(detected) > len(candidates)
    )
    if extra_boundary_run_count:
        limitations.append("perturbations_produced_extra_boundaries")
    if unstable_ids:
        status = "fail"
    elif missing_analyses:
        status = "limited"
    else:
        status = "pass"
    return (
        tuple(boundaries),
        FeedEpochStabilityV1(
            status=status,
            run_counts=dict(run_counts),
            usable_run_counts=dict(usable_counts),
            unstable_boundary_ids=tuple(unstable_ids),
            limitations=tuple(sorted(set(limitations))),
        ),
    )


def _matching_candidate(
    baseline: _Candidate,
    detected: Sequence[_Candidate],
    positions: Mapping[str, int],
    config: FeedEpochFitConfigV1,
) -> _Candidate | None:
    baseline_position = positions[baseline.right_period]
    ranked = sorted(
        detected,
        key=lambda candidate: (
            abs(
                positions.get(candidate.right_period, -10_000)
                - baseline_position
            ),
            -candidate.score,
            candidate.right_period,
        ),
    )
    if not ranked:
        return None
    candidate = ranked[0]
    position = positions.get(candidate.right_period)
    if position is None:
        return None
    if (
        abs(position - baseline_position)
        > config.boundary_match_tolerance_periods
    ):
        return None
    return candidate


def _epoch_intervals(
    points: tuple[_PeriodPoint, ...],
    boundaries: tuple[FeedEpochBoundaryV1, ...],
) -> tuple[FeedEpochIntervalV1, ...]:
    boundary_periods = [boundary.right_period for boundary in boundaries]
    period_positions = {
        point.period: index for index, point in enumerate(points)
    }
    split_positions = [period_positions[period] for period in boundary_periods]
    starts = [0, *split_positions]
    ends = [position - 1 for position in split_positions] + [len(points) - 1]
    epochs: list[FeedEpochIntervalV1] = []
    for index, (start_index, end_index) in enumerate(
        zip(starts, ends, strict=True), start=1
    ):
        segment = points[start_index : end_index + 1]
        epoch_id = f"epoch-{index:03d}"
        epochs.append(
            FeedEpochIntervalV1(
                epoch_id=epoch_id,
                label=epoch_id,
                period_start=segment[0].period,
                period_end=segment[-1].period,
                start_timestamp_utc_ms=segment[0].start_utc_ms,
                end_timestamp_utc_ms=segment[-1].end_utc_ms,
                evidence_count=sum(point.evidence_count for point in segment),
            )
        )
    return tuple(epochs)


def _definition_lineage(
    evidence: Sequence[FeedEpochEvidenceV1],
    features: Sequence[str],
    config: FeedEpochFitConfigV1,
) -> dict[str, JSONValue]:
    sources: list[JSONValue] = [
        {
            "evidence_id": item.evidence_id,
            "fingerprint_id": item.fingerprint_id,
            "source_artifact_sha256": item.source_artifact_sha256,
            "source_hash_basis": item.source_hash_basis,
            "symbol": item.symbol,
            "period": item.period,
            "source_kind": item.source_kind,
        }
        for item in evidence
    ]
    canonical_sources: list[JSONValue] = []
    for item in evidence:
        components = _sequence(item.quality.get("component_sources"))
        if components:
            canonical_sources.extend(
                dict(_mapping(component)) for component in components
            )
        else:
            canonical_sources.append(
                {
                    "fingerprint_id": item.fingerprint_id,
                    "source_artifact_sha256": item.source_artifact_sha256,
                    "source_hash_basis": item.source_hash_basis,
                    "symbol": item.symbol,
                    "period": item.period,
                    "source_kind": item.source_kind,
                }
            )
    canonical_sources.sort(
        key=lambda source: (
            str(_mapping(source).get("symbol", "")),
            str(_mapping(source).get("period", "")),
            str(_mapping(source).get("fingerprint_id", "")),
        )
    )
    feature_sources: dict[str, JSONValue] = {}
    for feature in features:
        paths: list[JSONValue] = []
        paths.extend(
            sorted(
                {
                    path
                    for item in evidence
                    for path in item.feature_provenance.get(feature, ())
                }
            )
        )
        feature_sources[feature] = paths
    conditioning_status_counts = Counter(
        str(item.conditioning.get("calendar_status", "unavailable"))
        for item in evidence
    )
    source_kind_counts = Counter(item.source_kind for item in evidence)
    result: dict[str, JSONValue] = {
        "config_id": config.config_id,
        "source_count": len(evidence),
        "sources": sources,
        "canonical_source_count": len(canonical_sources),
        "canonical_sources": canonical_sources,
        "source_kind_counts": dict(sorted(source_kind_counts.items())),
        "conditioning_status_counts": dict(
            sorted(conditioning_status_counts.items())
        ),
        "fingerprint_schema_versions": [TIME_SERIES_FINGERPRINT_SCHEMA_VERSION],
        "feature_provenance": feature_sources,
        "panel_normalization": "within_symbol_robust_then_period_median",
    }
    result["lineage_sha256"] = _payload_sha256(_lineage_hash_material(result))
    return result


def _lineage_hash_material(
    lineage: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    """Return every lineage field except its self-referential digest."""
    return {
        str(name): value
        for name, value in sorted(lineage.items())
        if name != "lineage_sha256"
    }


def _price_precision_digits(distribution: Mapping[str, Any]) -> float | None:
    precision = _mapping(distribution.get("precision"))
    columns = _mapping(precision.get("column_decimal_place_counts"))
    counts: Counter[int] = Counter()
    for column in ("bid", "ask"):
        for digits, count in _mapping(columns.get(column)).items():
            try:
                counts[int(str(digits))] += _nonnegative_int(count)
            except ValueError:
                continue
    if not counts:
        return None
    return float(
        sorted(counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
    )


def _conditioned_spread_median(payload: Mapping[str, Any]) -> float | None:
    conditional = _mapping(payload.get("conditional_distributions"))
    by_session = _mapping(conditional.get("by_active_session"))
    values: list[float] = []
    for row in by_session.values():
        spread = _mapping(_mapping(row).get("spread"))
        value = _optional_finite_float(spread.get("median"))
        if value is not None:
            values.append(value)
    return float(median(values)) if values else None


def _quantile_value(
    summary: Mapping[str, Any], quantile: float
) -> float | None:
    quantiles = _mapping(summary.get("quantiles"))
    candidates = (
        str(quantile),
        f"{quantile:.2f}",
        f"p{int(round(quantile * 100))}",
    )
    for key in candidates:
        if key in quantiles:
            return _optional_finite_float(quantiles[key])
    return None


def _payload_sha256(value: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(value).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _stable_id(prefix: str, value: Mapping[str, JSONValue]) -> str:
    encoded = canonical_contract_json(value).encode("utf-8")
    return f"{prefix}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _required_sha256_id(value: Any, name: str) -> str:
    text = _required_text(value, name)
    digest = text.removeprefix("sha256:")
    if not _SHA256_RE.fullmatch(digest):
        raise ValueError(f"{name} must be a sha256 identifier")
    return "sha256:" + digest


def _required_text(value: Any, name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{name} must not be empty")
    return text


def _valid_period(value: str) -> bool:
    if not _PERIOD_RE.fullmatch(value):
        return False
    return len(value) == 4 or 1 <= int(value[4:]) <= 12


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if isinstance(value, float) and not value.is_integer():
        raise ValueError(f"{name} must be an integer")
    return result


def _nonnegative_int(value: Any) -> int:
    try:
        return max(0, _strict_int(value, "value"))
    except ValueError:
        return 0


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be finite")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(result):
        raise ValueError(f"{name} must be finite")
    return result


def _optional_finite_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _bounded_int(
    value: Any,
    name: str,
    minimum: int,
    maximum: int,
) -> int:
    result = _strict_int(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


def _bounded_float(
    value: Any,
    name: str,
    minimum: float,
    maximum: float,
) -> float:
    result = _finite_float(value, name)
    if not minimum <= result <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return result


def _rounded(value: float, digits: int) -> float:
    result = round(float(value), digits)
    return 0.0 if result == 0.0 else result


def _mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return {str(key): item for key, item in value.items()}


def _sequence(value: Any) -> tuple[Any, ...]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return tuple(value)
    return ()


def _count_mapping(value: Mapping[str, int]) -> dict[str, int]:
    return {
        str(name): _bounded_int(
            count, str(name), 0, MAX_FEED_EPOCH_SENSITIVITY_RUNS
        )
        for name, count in sorted(value.items())
    }


def _count_mapping_unbounded(value: Any) -> dict[str, int]:
    return {
        str(name): _bounded_int(
            count,
            str(name),
            0,
            MAX_FEED_EPOCH_EVIDENCE,
        )
        for name, count in sorted(_mapping(value).items())
    }
