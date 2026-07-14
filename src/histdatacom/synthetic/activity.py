"""Honest activity and liquidity-proxy evidence for reconstructed events.

The final event schema records quote deliveries, not centralized FX trades.
This module therefore derives bounded activity metadata from immutable
``SyntheticEventV1`` rows without adding a fabricated volume column.  The
same contracts work over in-memory streams and projected batches from the
committed Parquet product.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar, cast

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.benchmark import ReverseDegradationScorecardV1
from histdatacom.synthetic.contracts import (
    SYNTHETIC_EVENT_SCHEMA_VERSION,
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
)
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.persistence import (
    iter_reconstruction_event_batches,
    verify_reconstruction_publication,
)

RECONSTRUCTION_ACTIVITY_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-activity-policy.v1"
)
RECONSTRUCTION_ACTIVITY_METRIC_SCHEMA_VERSION = (
    "histdatacom.reconstruction-activity-metric.v1"
)
RECONSTRUCTION_ACTIVITY_SLICE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-activity-slice.v1"
)
RECONSTRUCTION_ACTIVITY_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-activity-manifest.v1"
)
RECONSTRUCTION_ACTIVITY_BENCHMARK_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-activity-benchmark-evidence.v1"
)

DEFAULT_ACTIVITY_MAX_SYMBOLS = 64
DEFAULT_ACTIVITY_MAX_SLICES = 192
DEFAULT_ACTIVITY_MAX_PROVENANCE_VALUES = 256
DEFAULT_ACTIVITY_MAX_PAYLOAD_BYTES = 1_048_576
DEFAULT_ACTIVITY_ROUNDING_DIGITS = 12
DEFAULT_ACTIVITY_BATCH_SIZE = 8_192
MAX_ACTIVITY_SYMBOLS = 256
MAX_ACTIVITY_SLICES = 768
MAX_ACTIVITY_PROVENANCE_VALUES = 4_096
MAX_ACTIVITY_PAYLOAD_BYTES = 4_194_304
MAX_ACTIVITY_TEXT = 1_024
NANOSECONDS_PER_SECOND = 1_000_000_000

_EnumT = TypeVar("_EnumT", bound=Enum)

ACTIVITY_EVENT_COLUMNS = (
    "event_id",
    "origin",
    "symbol",
    "event_time_ns",
    "event_sequence",
    "bid",
    "ask",
    "run_id",
    "ensemble_member_id",
    "source_version_id",
    "generator_id",
    "generator_version",
    "generator_config_id",
    "reference_id",
    "motif_id",
    "feed_epoch_id",
    "broker_profile_id",
    "constraint_set_id",
    "confidence",
)

ACTIVITY_REVERSE_DEGRADATION_METRICS = (
    "event_count_relative_error",
    "intensity_relative_error",
    "interarrival_hist_l1",
    "burst_rate_absolute_error",
    "quiet_rate_absolute_error",
    "spread_mean_relative_error",
    "spread_hist_l1",
)


class ActivitySliceScope(str, Enum):
    """Which final-event population one activity slice describes."""

    OBSERVED = "observed"
    SYNTHETIC = "synthetic"
    MERGED = "merged"

    @classmethod
    def from_value(
        cls, value: str | "ActivitySliceScope"
    ) -> "ActivitySliceScope":
        """Return a strict normalized activity scope."""
        return _enum_value(cls, value, "activity slice scope")


class ActivityVolumeState(str, Enum):
    """Honest handling state for fields sometimes mislabeled as volume."""

    UNAVAILABLE = "unavailable"
    OMITTED = "omitted"
    OBSERVED_SOURCE_SIZE = "observed_source_size"
    BROKER_SUPPLIED_SIZE = "broker_supplied_size"
    SYNTHETIC_ACTIVITY_PROXY = "synthetic_activity_proxy"

    @classmethod
    def from_value(
        cls, value: str | "ActivityVolumeState"
    ) -> "ActivityVolumeState":
        """Return strict volume handling without implying traded volume."""
        return _enum_value(cls, value, "activity volume state")


class ActivityMetricSemantics(str, Enum):
    """Scientific meaning of one derived activity metric."""

    QUOTE_ACTIVITY = "quote_activity"
    CADENCE = "cadence"
    SPREAD_LIQUIDITY_PROXY = "spread_liquidity_proxy"
    EVENT_CONFIDENCE = "event_confidence"

    @classmethod
    def from_value(
        cls, value: str | "ActivityMetricSemantics"
    ) -> "ActivityMetricSemantics":
        """Return strict activity metric semantics."""
        return _enum_value(cls, value, "activity metric semantics")


class ActivityAggregationSemantics(str, Enum):
    """How a metric must be projected into a larger interval."""

    COUNT = "count"
    INTERVAL_DURATION = "interval_duration"
    RECOMPUTE_RATE = "recompute_rate"
    SUPPORT_WEIGHTED_MEAN = "support_weighted_mean"
    MINIMUM = "minimum"
    MAXIMUM = "maximum"

    @classmethod
    def from_value(
        cls, value: str | "ActivityAggregationSemantics"
    ) -> "ActivityAggregationSemantics":
        """Return strict aggregation semantics."""
        return _enum_value(cls, value, "activity aggregation semantics")


_METRIC_DEFINITIONS: dict[
    str,
    tuple[str, ActivityAggregationSemantics, ActivityMetricSemantics],
] = {
    "event_count": (
        "event",
        ActivityAggregationSemantics.COUNT,
        ActivityMetricSemantics.QUOTE_ACTIVITY,
    ),
    "quote_update_count": (
        "quote_update",
        ActivityAggregationSemantics.COUNT,
        ActivityMetricSemantics.QUOTE_ACTIVITY,
    ),
    "exposure_duration_ns": (
        "nanosecond",
        ActivityAggregationSemantics.INTERVAL_DURATION,
        ActivityMetricSemantics.QUOTE_ACTIVITY,
    ),
    "tick_intensity_per_second": (
        "event_per_second",
        ActivityAggregationSemantics.RECOMPUTE_RATE,
        ActivityMetricSemantics.CADENCE,
    ),
    "mean_interarrival_ns": (
        "nanosecond",
        ActivityAggregationSemantics.SUPPORT_WEIGHTED_MEAN,
        ActivityMetricSemantics.CADENCE,
    ),
    "min_interarrival_ns": (
        "nanosecond",
        ActivityAggregationSemantics.MINIMUM,
        ActivityMetricSemantics.CADENCE,
    ),
    "max_interarrival_ns": (
        "nanosecond",
        ActivityAggregationSemantics.MAXIMUM,
        ActivityMetricSemantics.CADENCE,
    ),
    "price_change_count": (
        "transition",
        ActivityAggregationSemantics.COUNT,
        ActivityMetricSemantics.QUOTE_ACTIVITY,
    ),
    "stale_quote_count": (
        "transition",
        ActivityAggregationSemantics.COUNT,
        ActivityMetricSemantics.QUOTE_ACTIVITY,
    ),
    "stale_quote_rate": (
        "ratio",
        ActivityAggregationSemantics.RECOMPUTE_RATE,
        ActivityMetricSemantics.QUOTE_ACTIVITY,
    ),
    "mean_spread": (
        "price",
        ActivityAggregationSemantics.SUPPORT_WEIGHTED_MEAN,
        ActivityMetricSemantics.SPREAD_LIQUIDITY_PROXY,
    ),
    "min_spread": (
        "price",
        ActivityAggregationSemantics.MINIMUM,
        ActivityMetricSemantics.SPREAD_LIQUIDITY_PROXY,
    ),
    "max_spread": (
        "price",
        ActivityAggregationSemantics.MAXIMUM,
        ActivityMetricSemantics.SPREAD_LIQUIDITY_PROXY,
    ),
    "mean_event_confidence": (
        "probability",
        ActivityAggregationSemantics.SUPPORT_WEIGHTED_MEAN,
        ActivityMetricSemantics.EVENT_CONFIDENCE,
    ),
}


def activity_metric_definitions() -> dict[str, JSONValue]:
    """Return the immutable metric dictionary in JSON-compatible form."""
    return {
        name: {
            "unit": definition[0],
            "aggregation": definition[1].value,
            "semantics": definition[2].value,
        }
        for name, definition in sorted(_METRIC_DEFINITIONS.items())
    }


def activity_bar_projection_semantics() -> dict[str, JSONValue]:
    """Return the exact #18 projection rules for derived candlestick bars."""
    return {
        "tick_count": {
            "source_metric": "event_count",
            "operation": "sum",
            "unit": "event",
        },
        "quote_update_count": {
            "source_metric": "quote_update_count",
            "operation": "sum",
            "unit": "quote_update",
        },
        "activity_duration_ns": {
            "source_metric": "exposure_duration_ns",
            "operation": "recompute_from_bar_event_bounds",
            "unit": "nanosecond",
        },
        "tick_intensity_per_second": {
            "source_metric": "event_count",
            "operation": "recompute_count_divided_by_activity_duration",
            "unit": "event_per_second",
        },
        "price_change_count": {
            "source_metric": "price_change_count",
            "operation": "sum_with_boundary_carry",
            "unit": "transition",
        },
        "stale_quote_count": {
            "source_metric": "stale_quote_count",
            "operation": "sum_with_boundary_carry",
            "unit": "transition",
        },
        "stale_quote_rate": {
            "source_metric": "stale_quote_count",
            "operation": "recompute_over_quote_transitions",
            "unit": "ratio",
        },
        "mean_spread": {
            "source_metric": "mean_spread",
            "operation": "event_support_weighted_mean",
            "weight_metric": "event_count",
            "unit": "price",
        },
        "volume": {
            "source_metric": None,
            "operation": "unavailable_unless_separately_sourced",
            "unit": None,
        },
    }


@dataclass(frozen=True, slots=True)
class ReconstructionActivityPolicyV1:
    """Bounded deterministic aggregation and honest volume policy."""

    scopes: tuple[ActivitySliceScope, ...] = (
        ActivitySliceScope.OBSERVED,
        ActivitySliceScope.SYNTHETIC,
        ActivitySliceScope.MERGED,
    )
    volume_state: ActivityVolumeState = ActivityVolumeState.UNAVAILABLE
    rounding_digits: int = DEFAULT_ACTIVITY_ROUNDING_DIGITS
    max_symbols: int = DEFAULT_ACTIVITY_MAX_SYMBOLS
    max_slices: int = DEFAULT_ACTIVITY_MAX_SLICES
    max_provenance_values: int = DEFAULT_ACTIVITY_MAX_PROVENANCE_VALUES
    max_payload_bytes: int = DEFAULT_ACTIVITY_MAX_PAYLOAD_BYTES
    policy_id: str = ""
    schema_version: str = RECONSTRUCTION_ACTIVITY_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_ACTIVITY_POLICY_SCHEMA_VERSION,
            "reconstruction activity policy",
        )
        scopes = tuple(
            sorted(
                {ActivitySliceScope.from_value(item) for item in self.scopes},
                key=lambda item: item.value,
            )
        )
        if not scopes:
            raise ValueError("activity policy requires at least one scope")
        object.__setattr__(self, "scopes", scopes)
        object.__setattr__(
            self,
            "volume_state",
            ActivityVolumeState.from_value(self.volume_state),
        )
        rounding = _bounded_int(self.rounding_digits, "rounding_digits", 0, 15)
        object.__setattr__(self, "rounding_digits", rounding)
        for name, maximum in (
            ("max_symbols", MAX_ACTIVITY_SYMBOLS),
            ("max_slices", MAX_ACTIVITY_SLICES),
            ("max_provenance_values", MAX_ACTIVITY_PROVENANCE_VALUES),
            ("max_payload_bytes", MAX_ACTIVITY_PAYLOAD_BYTES),
        ):
            value = _bounded_int(getattr(self, name), name, 1, maximum)
            object.__setattr__(self, name, value)
        if self.max_slices < len(scopes):
            raise ValueError("max_slices is smaller than configured scopes")
        expected = _stable_id("activity-policy", self.identity_payload())
        supplied = _optional_text(self.policy_id)
        if supplied is not None and supplied != expected:
            raise ValueError("activity policy_id differs from its content")
        object.__setattr__(self, "policy_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic policy identity."""
        return {
            "schema_version": self.schema_version,
            "scopes": [item.value for item in self.scopes],
            "volume_state": self.volume_state.value,
            "rounding_digits": self.rounding_digits,
            "max_symbols": self.max_symbols,
            "max_slices": self.max_slices,
            "max_provenance_values": self.max_provenance_values,
            "max_payload_bytes": self.max_payload_bytes,
            "output_mode": "derived_metadata",
            "event_schema_augmented": False,
            "centralized_traded_volume_claim": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic policy JSON."""
        return {**self.identity_payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionActivityPolicyV1":
        """Restore and verify an activity policy."""
        _require_schema(data, RECONSTRUCTION_ACTIVITY_POLICY_SCHEMA_VERSION)
        _require_derived(data, "output_mode", "derived_metadata")
        _require_derived(data, "event_schema_augmented", False)
        _require_derived(data, "centralized_traded_volume_claim", False)
        return cls(
            scopes=tuple(
                ActivitySliceScope.from_value(str(item))
                for item in _sequence(data.get("scopes"), "scopes")
            ),
            volume_state=ActivityVolumeState.from_value(
                str(data.get("volume_state", ""))
            ),
            rounding_digits=_strict_int(
                data.get("rounding_digits"), "rounding_digits"
            ),
            max_symbols=_strict_int(data.get("max_symbols"), "max_symbols"),
            max_slices=_strict_int(data.get("max_slices"), "max_slices"),
            max_provenance_values=_strict_int(
                data.get("max_provenance_values"), "max_provenance_values"
            ),
            max_payload_bytes=_strict_int(
                data.get("max_payload_bytes"), "max_payload_bytes"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionActivityPolicyV1":
        """Restore a policy from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionActivityMetricV1:
    """One unit-bearing activity or liquidity-proxy estimate."""

    name: str
    value: int | float | None
    unit: str
    aggregation: ActivityAggregationSemantics
    semantics: ActivityMetricSemantics
    support_count: int
    confidence: float | None = None
    limitations: tuple[str, ...] = ()
    schema_version: str = RECONSTRUCTION_ACTIVITY_METRIC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_ACTIVITY_METRIC_SCHEMA_VERSION,
            "reconstruction activity metric",
        )
        name = _required_text(self.name)
        if name not in _METRIC_DEFINITIONS:
            raise ValueError("unsupported reconstruction activity metric")
        object.__setattr__(self, "name", name)
        expected_unit, expected_aggregation, expected_semantics = (
            _METRIC_DEFINITIONS[name]
        )
        unit = _required_text(self.unit)
        aggregation = ActivityAggregationSemantics.from_value(self.aggregation)
        semantics = ActivityMetricSemantics.from_value(self.semantics)
        if (
            unit != expected_unit
            or aggregation is not expected_aggregation
            or semantics is not expected_semantics
        ):
            raise ValueError("activity metric definition differs")
        object.__setattr__(self, "unit", unit)
        object.__setattr__(self, "aggregation", aggregation)
        object.__setattr__(self, "semantics", semantics)
        if self.value is not None:
            if isinstance(self.value, bool) or not isinstance(
                self.value, (int, float)
            ):
                raise ValueError("activity metric value must be numeric")
            if not math.isfinite(float(self.value)):
                raise ValueError("activity metric value must be finite")
            if float(self.value) < 0.0:
                raise ValueError("activity metric value must be non-negative")
        object.__setattr__(
            self,
            "support_count",
            _bounded_int(self.support_count, "support_count", 0, 2**63 - 1),
        )
        if self.value is None and self.support_count:
            raise ValueError("unavailable metric cannot claim support")
        if self.confidence is not None:
            confidence = _finite_float(self.confidence, "confidence")
            if not 0.0 <= confidence <= 1.0:
                raise ValueError("activity metric confidence is out of range")
            object.__setattr__(self, "confidence", confidence)
        object.__setattr__(
            self, "limitations", _normalized_text_tuple(self.limitations)
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return stable metric metadata."""
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "value": self.value,
            "unit": self.unit,
            "aggregation": self.aggregation.value,
            "semantics": self.semantics.value,
            "support_count": self.support_count,
            "confidence": self.confidence,
            "limitations": list(self.limitations),
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionActivityMetricV1":
        """Restore a strict activity metric."""
        _require_schema(data, RECONSTRUCTION_ACTIVITY_METRIC_SCHEMA_VERSION)
        raw_value = data.get("value")
        if raw_value is not None and (
            isinstance(raw_value, bool)
            or not isinstance(raw_value, (int, float))
        ):
            raise ValueError("activity metric value must be numeric")
        return cls(
            name=str(data.get("name", "")),
            value=raw_value,
            unit=str(data.get("unit", "")),
            aggregation=ActivityAggregationSemantics.from_value(
                str(data.get("aggregation", ""))
            ),
            semantics=ActivityMetricSemantics.from_value(
                str(data.get("semantics", ""))
            ),
            support_count=_strict_int(
                data.get("support_count"), "support_count"
            ),
            confidence=_optional_float(data.get("confidence"), "confidence"),
            limitations=_string_tuple(data.get("limitations"), "limitations"),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionActivitySliceV1:
    """One symbol/origin activity summary with bounded provenance."""

    symbol: str
    scope: ActivitySliceScope
    start_event_time_ns: int
    end_event_time_ns: int
    metrics: tuple[ReconstructionActivityMetricV1, ...]
    source_version_ids: tuple[str, ...]
    generator_ids: tuple[str, ...]
    generator_versions: tuple[str, ...]
    generator_config_ids: tuple[str, ...]
    reference_ids: tuple[str, ...]
    motif_ids: tuple[str, ...]
    feed_epoch_ids: tuple[str, ...]
    broker_profile_ids: tuple[str, ...]
    constraint_set_ids: tuple[str, ...]
    stream_ids: tuple[str, ...]
    event_content_sha256: str
    confidence_support_count: int
    limitations: tuple[str, ...] = ()
    slice_id: str = ""
    schema_version: str = RECONSTRUCTION_ACTIVITY_SLICE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_ACTIVITY_SLICE_SCHEMA_VERSION,
            "reconstruction activity slice",
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        object.__setattr__(
            self, "scope", ActivitySliceScope.from_value(self.scope)
        )
        start = _strict_int(self.start_event_time_ns, "start_event_time_ns")
        end = _strict_int(self.end_event_time_ns, "end_event_time_ns")
        if end < start:
            raise ValueError("activity slice time bounds are reversed")
        object.__setattr__(self, "start_event_time_ns", start)
        object.__setattr__(self, "end_event_time_ns", end)
        metrics = tuple(sorted(self.metrics, key=lambda item: item.name))
        if any(
            not isinstance(item, ReconstructionActivityMetricV1)
            for item in metrics
        ):
            raise TypeError("activity slice metrics require v1 contracts")
        if {item.name for item in metrics} != set(_METRIC_DEFINITIONS):
            raise ValueError("activity slice metric coverage differs")
        object.__setattr__(self, "metrics", metrics)
        for name in (
            "source_version_ids",
            "generator_ids",
            "generator_versions",
            "generator_config_ids",
            "reference_ids",
            "motif_ids",
            "feed_epoch_ids",
            "broker_profile_ids",
            "constraint_set_ids",
            "stream_ids",
        ):
            object.__setattr__(
                self, name, _normalized_text_tuple(getattr(self, name))
            )
        if not self.source_version_ids:
            raise ValueError("activity slice requires source version identity")
        object.__setattr__(
            self,
            "event_content_sha256",
            _required_sha256(self.event_content_sha256, "event_content_sha256"),
        )
        object.__setattr__(
            self,
            "confidence_support_count",
            _bounded_int(
                self.confidence_support_count,
                "confidence_support_count",
                0,
                self.event_count,
            ),
        )
        object.__setattr__(
            self, "limitations", _normalized_text_tuple(self.limitations)
        )
        expected = _stable_id("activity-slice", self.identity_payload())
        supplied = _optional_text(self.slice_id)
        if supplied is not None and supplied != expected:
            raise ValueError("activity slice_id differs from its content")
        object.__setattr__(self, "slice_id", expected)

    @property
    def metric_by_name(self) -> dict[str, ReconstructionActivityMetricV1]:
        """Return metrics keyed by stable name."""
        return {item.name: item for item in self.metrics}

    @property
    def event_count(self) -> int:
        """Return the number of delivered quote events."""
        value = self.metric_by_name["event_count"].value
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError("activity event_count is not integral")
        return value

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic slice evidence."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "scope": self.scope.value,
            "start_event_time_ns": self.start_event_time_ns,
            "end_event_time_ns": self.end_event_time_ns,
            "metrics": [item.to_dict() for item in self.metrics],
            "source_version_ids": list(self.source_version_ids),
            "generator_ids": list(self.generator_ids),
            "generator_versions": list(self.generator_versions),
            "generator_config_ids": list(self.generator_config_ids),
            "reference_ids": list(self.reference_ids),
            "motif_ids": list(self.motif_ids),
            "feed_epoch_ids": list(self.feed_epoch_ids),
            "broker_profile_ids": list(self.broker_profile_ids),
            "constraint_set_ids": list(self.constraint_set_ids),
            "stream_ids": list(self.stream_ids),
            "event_content_sha256": self.event_content_sha256,
            "confidence_support_count": self.confidence_support_count,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return stable slice metadata."""
        return {**self.identity_payload(), "slice_id": self.slice_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionActivitySliceV1":
        """Restore and verify an activity slice."""
        _require_schema(data, RECONSTRUCTION_ACTIVITY_SLICE_SCHEMA_VERSION)
        return cls(
            symbol=str(data.get("symbol", "")),
            scope=ActivitySliceScope.from_value(str(data.get("scope", ""))),
            start_event_time_ns=_strict_int(
                data.get("start_event_time_ns"), "start_event_time_ns"
            ),
            end_event_time_ns=_strict_int(
                data.get("end_event_time_ns"), "end_event_time_ns"
            ),
            metrics=tuple(
                ReconstructionActivityMetricV1.from_dict(item)
                for item in _mapping_sequence(data.get("metrics"), "metrics")
            ),
            source_version_ids=_string_tuple(
                data.get("source_version_ids"), "source_version_ids"
            ),
            generator_ids=_string_tuple(
                data.get("generator_ids"), "generator_ids"
            ),
            generator_versions=_string_tuple(
                data.get("generator_versions"), "generator_versions"
            ),
            generator_config_ids=_string_tuple(
                data.get("generator_config_ids"), "generator_config_ids"
            ),
            reference_ids=_string_tuple(
                data.get("reference_ids"), "reference_ids"
            ),
            motif_ids=_string_tuple(data.get("motif_ids"), "motif_ids"),
            feed_epoch_ids=_string_tuple(
                data.get("feed_epoch_ids"), "feed_epoch_ids"
            ),
            broker_profile_ids=_string_tuple(
                data.get("broker_profile_ids"), "broker_profile_ids"
            ),
            constraint_set_ids=_string_tuple(
                data.get("constraint_set_ids"), "constraint_set_ids"
            ),
            stream_ids=_string_tuple(data.get("stream_ids"), "stream_ids"),
            event_content_sha256=str(data.get("event_content_sha256", "")),
            confidence_support_count=_strict_int(
                data.get("confidence_support_count"),
                "confidence_support_count",
            ),
            limitations=_string_tuple(data.get("limitations"), "limitations"),
            slice_id=str(data.get("slice_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionActivityBenchmarkEvidenceV1:
    """Activity-specific evidence extracted from the shared benchmark."""

    scorecard_id: str
    candidate_score_ids: tuple[str, ...]
    split_kinds: tuple[str, ...]
    metric_support_counts: Mapping[str, int]
    metric_mean_errors: Mapping[str, float]
    mean_restoration_gain_vs_degraded: float
    promotion_eligible_candidate_count: int
    calibration_supported_candidate_count: int
    execution_failure_count: int
    evidence_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_ACTIVITY_BENCHMARK_EVIDENCE_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_ACTIVITY_BENCHMARK_EVIDENCE_SCHEMA_VERSION,
            "reconstruction activity benchmark evidence",
        )
        object.__setattr__(
            self, "scorecard_id", _required_text(self.scorecard_id)
        )
        candidate_ids = _normalized_text_tuple(self.candidate_score_ids)
        if not candidate_ids:
            raise ValueError("activity benchmark evidence requires candidates")
        object.__setattr__(self, "candidate_score_ids", candidate_ids)
        split_kinds = _normalized_text_tuple(self.split_kinds)
        if not split_kinds:
            raise ValueError("activity benchmark evidence requires split kinds")
        object.__setattr__(self, "split_kinds", split_kinds)
        supports = {
            _required_text(name): _bounded_int(
                value, f"metric_support_counts.{name}", 1, 2**63 - 1
            )
            for name, value in self.metric_support_counts.items()
        }
        errors = {
            _required_text(name): _nonnegative_float(
                value, f"metric_mean_errors.{name}"
            )
            for name, value in self.metric_mean_errors.items()
        }
        required = set(ACTIVITY_REVERSE_DEGRADATION_METRICS)
        if set(supports) != required or set(errors) != required:
            raise ValueError("activity benchmark metric coverage differs")
        object.__setattr__(
            self, "metric_support_counts", dict(sorted(supports.items()))
        )
        object.__setattr__(
            self, "metric_mean_errors", dict(sorted(errors.items()))
        )
        object.__setattr__(
            self,
            "mean_restoration_gain_vs_degraded",
            _finite_float(
                self.mean_restoration_gain_vs_degraded,
                "mean_restoration_gain_vs_degraded",
            ),
        )
        for name in (
            "promotion_eligible_candidate_count",
            "calibration_supported_candidate_count",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_int(getattr(self, name), name, 0, len(candidate_ids)),
            )
        object.__setattr__(
            self,
            "execution_failure_count",
            _bounded_int(
                self.execution_failure_count,
                "execution_failure_count",
                0,
                2**63 - 1,
            ),
        )
        expected = _stable_id("activity-benchmark", self.identity_payload())
        supplied = _optional_text(self.evidence_id)
        if supplied is not None and supplied != expected:
            raise ValueError("activity benchmark evidence_id differs")
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return bounded benchmark evidence without selecting a winner."""
        return {
            "schema_version": self.schema_version,
            "scorecard_id": self.scorecard_id,
            "candidate_score_ids": list(self.candidate_score_ids),
            "split_kinds": list(self.split_kinds),
            "metric_support_counts": dict(self.metric_support_counts),
            "metric_mean_errors": dict(self.metric_mean_errors),
            "mean_restoration_gain_vs_degraded": (
                self.mean_restoration_gain_vs_degraded
            ),
            "promotion_eligible_candidate_count": (
                self.promotion_eligible_candidate_count
            ),
            "calibration_supported_candidate_count": (
                self.calibration_supported_candidate_count
            ),
            "execution_failure_count": self.execution_failure_count,
            "automatic_winner": False,
            "winner_candidate_id": None,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return stable benchmark evidence."""
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionActivityBenchmarkEvidenceV1":
        """Restore activity benchmark evidence."""
        _require_schema(
            data, RECONSTRUCTION_ACTIVITY_BENCHMARK_EVIDENCE_SCHEMA_VERSION
        )
        _require_derived(data, "automatic_winner", False)
        _require_derived(data, "winner_candidate_id", None)
        return cls(
            scorecard_id=str(data.get("scorecard_id", "")),
            candidate_score_ids=_string_tuple(
                data.get("candidate_score_ids"), "candidate_score_ids"
            ),
            split_kinds=_string_tuple(data.get("split_kinds"), "split_kinds"),
            metric_support_counts={
                str(name): _strict_int(value, str(name))
                for name, value in _mapping(
                    data.get("metric_support_counts"),
                    "metric_support_counts",
                ).items()
            },
            metric_mean_errors={
                str(name): _finite_float(value, str(name))
                for name, value in _mapping(
                    data.get("metric_mean_errors"), "metric_mean_errors"
                ).items()
            },
            mean_restoration_gain_vs_degraded=_finite_float(
                data.get("mean_restoration_gain_vs_degraded"),
                "mean_restoration_gain_vs_degraded",
            ),
            promotion_eligible_candidate_count=_strict_int(
                data.get("promotion_eligible_candidate_count"),
                "promotion_eligible_candidate_count",
            ),
            calibration_supported_candidate_count=_strict_int(
                data.get("calibration_supported_candidate_count"),
                "calibration_supported_candidate_count",
            ),
            execution_failure_count=_strict_int(
                data.get("execution_failure_count"), "execution_failure_count"
            ),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionActivityManifestV1:
    """Bounded derived activity metadata for one final event population."""

    run_id: str
    ensemble_member_id: str
    information_mode: InformationMode
    information_manifest_id: str
    as_of_ns: int | None
    policy: ReconstructionActivityPolicyV1
    slices: tuple[ReconstructionActivitySliceV1, ...]
    input_content_sha256: str
    window_id: str | None = None
    synchronization_unit_id: str | None = None
    product_manifest_id: str | None = None
    calibration_report_id: str | None = None
    benchmark_evidence: ReconstructionActivityBenchmarkEvidenceV1 | None = None
    manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_ACTIVITY_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_ACTIVITY_MANIFEST_SCHEMA_VERSION,
            "reconstruction activity manifest",
        )
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self, "ensemble_member_id", _required_text(self.ensemble_member_id)
        )
        mode = InformationMode.from_value(self.information_mode)
        object.__setattr__(self, "information_mode", mode)
        object.__setattr__(
            self,
            "information_manifest_id",
            _required_text(self.information_manifest_id),
        )
        as_of = _optional_int(self.as_of_ns, "as_of_ns")
        if mode is InformationMode.EX_ANTE_SIMULATION and as_of is None:
            raise ValueError("ex-ante activity evidence requires as_of_ns")
        if mode is InformationMode.EX_POST_RECONSTRUCTION and as_of is not None:
            raise ValueError("ex-post activity evidence rejects as_of_ns")
        object.__setattr__(self, "as_of_ns", as_of)
        if not isinstance(self.policy, ReconstructionActivityPolicyV1):
            raise TypeError("activity manifest requires a v1 policy")
        slices = tuple(
            sorted(
                self.slices, key=lambda item: (item.symbol, item.scope.value)
            )
        )
        if not slices or len(slices) > self.policy.max_slices:
            raise ValueError(
                "activity manifest slice count is empty or unbounded"
            )
        if any(
            not isinstance(item, ReconstructionActivitySliceV1)
            for item in slices
        ):
            raise TypeError("activity manifest slices require v1 contracts")
        if len({(item.symbol, item.scope) for item in slices}) != len(slices):
            raise ValueError("activity manifest contains duplicate slices")
        if any(item.scope not in self.policy.scopes for item in slices):
            raise ValueError("activity manifest scope is outside policy")
        symbols = {item.symbol for item in slices}
        if len(symbols) > self.policy.max_symbols:
            raise ValueError("activity manifest symbol count is unbounded")
        _validate_slice_reconciliation(slices)
        object.__setattr__(self, "slices", slices)
        object.__setattr__(
            self,
            "input_content_sha256",
            _required_sha256(self.input_content_sha256, "input_content_sha256"),
        )
        for name in (
            "window_id",
            "synchronization_unit_id",
            "product_manifest_id",
            "calibration_report_id",
        ):
            object.__setattr__(self, name, _optional_text(getattr(self, name)))
        if self.benchmark_evidence is not None and not isinstance(
            self.benchmark_evidence,
            ReconstructionActivityBenchmarkEvidenceV1,
        ):
            raise TypeError("activity benchmark evidence requires v1 contract")
        expected = _stable_id("activity-manifest", self.identity_payload())
        supplied = _optional_text(self.manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("activity manifest_id differs from its content")
        object.__setattr__(self, "manifest_id", expected)
        if len(self.to_json().encode("utf-8")) > self.policy.max_payload_bytes:
            raise ValueError("activity manifest exceeds policy payload limit")

    @property
    def symbols(self) -> tuple[str, ...]:
        """Return represented symbols."""
        return tuple(sorted({item.symbol for item in self.slices}))

    @property
    def event_count(self) -> int:
        """Return total events without double-counting origin slices."""
        merged = [
            item
            for item in self.slices
            if item.scope is ActivitySliceScope.MERGED
        ]
        if merged:
            return sum(item.event_count for item in merged)
        return sum(
            item.event_count
            for item in self.slices
            if item.scope
            in (ActivitySliceScope.OBSERVED, ActivitySliceScope.SYNTHETIC)
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return complete scientific and provenance identity."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "ensemble_member_id": self.ensemble_member_id,
            "information_mode": self.information_mode.value,
            "information_manifest_id": self.information_manifest_id,
            "as_of_ns": self.as_of_ns,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "product_manifest_id": self.product_manifest_id,
            "calibration_report_id": self.calibration_report_id,
            "policy": self.policy.to_dict(),
            "slices": [item.to_dict() for item in self.slices],
            "input_content_sha256": self.input_content_sha256,
            "benchmark_evidence": (
                self.benchmark_evidence.to_dict()
                if self.benchmark_evidence is not None
                else None
            ),
            "metric_definitions": activity_metric_definitions(),
            "bar_projection_semantics": activity_bar_projection_semantics(),
            "output_mode": "derived_metadata",
            "event_schema_augmented": False,
            "centralized_traded_volume_claim": False,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic activity-manifest JSON."""
        return {
            **self.identity_payload(),
            "symbols": list(self.symbols),
            "event_count": self.event_count,
            "manifest_id": self.manifest_id,
        }

    def to_json(self) -> str:
        """Return canonical compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionActivityManifestV1":
        """Restore and reconcile an activity manifest."""
        _require_schema(data, RECONSTRUCTION_ACTIVITY_MANIFEST_SCHEMA_VERSION)
        _require_derived(
            data, "event_schema_version", SYNTHETIC_EVENT_SCHEMA_VERSION
        )
        _require_derived(
            data, "metric_definitions", activity_metric_definitions()
        )
        _require_derived(
            data,
            "bar_projection_semantics",
            activity_bar_projection_semantics(),
        )
        _require_derived(data, "output_mode", "derived_metadata")
        _require_derived(data, "event_schema_augmented", False)
        _require_derived(data, "centralized_traded_volume_claim", False)
        _require_derived(data, "automatic_winner", False)
        benchmark = data.get("benchmark_evidence")
        manifest = cls(
            run_id=str(data.get("run_id", "")),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            information_mode=InformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            information_manifest_id=str(
                data.get("information_manifest_id", "")
            ),
            as_of_ns=_optional_int(data.get("as_of_ns"), "as_of_ns"),
            policy=ReconstructionActivityPolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            slices=tuple(
                ReconstructionActivitySliceV1.from_dict(item)
                for item in _mapping_sequence(data.get("slices"), "slices")
            ),
            input_content_sha256=str(data.get("input_content_sha256", "")),
            window_id=_mapping_optional_text(data, "window_id"),
            synchronization_unit_id=_mapping_optional_text(
                data, "synchronization_unit_id"
            ),
            product_manifest_id=_mapping_optional_text(
                data, "product_manifest_id"
            ),
            calibration_report_id=_mapping_optional_text(
                data, "calibration_report_id"
            ),
            benchmark_evidence=(
                ReconstructionActivityBenchmarkEvidenceV1.from_dict(
                    _mapping(benchmark, "benchmark_evidence")
                )
                if benchmark is not None
                else None
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(data, "symbols", list(manifest.symbols))
        _require_derived(data, "event_count", manifest.event_count)
        return manifest

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionActivityManifestV1":
        """Restore an activity manifest from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class _ActivityEventView:
    """Minimal projected event fields required by the online accumulator."""

    event_id: str
    origin: SyntheticEventOrigin
    symbol: str
    event_time_ns: int
    event_sequence: int
    bid: float
    ask: float
    run_id: str
    ensemble_member_id: str
    source_version_id: str
    generator_id: str | None
    generator_version: str | None
    generator_config_id: str | None
    reference_id: str | None
    motif_id: str | None
    feed_epoch_id: str | None
    broker_profile_id: str | None
    constraint_set_id: str | None
    confidence: float | None
    stream_ids: tuple[str, ...] = ()


@dataclass(slots=True)
class _ActivitySliceState:
    """Constant-size numeric and bounded-provenance state for one slice."""

    symbol: str
    scope: ActivitySliceScope
    policy: ReconstructionActivityPolicyV1
    event_count: int = 0
    first_time_ns: int | None = None
    last_time_ns: int | None = None
    last_bid: float | None = None
    last_ask: float | None = None
    interarrival_count: int = 0
    interarrival_total_ns: int = 0
    min_interarrival_ns: int | None = None
    max_interarrival_ns: int | None = None
    price_change_count: int = 0
    stale_quote_count: int = 0
    spread_total: float = 0.0
    min_spread: float | None = None
    max_spread: float | None = None
    confidence_total: float = 0.0
    confidence_count: int = 0
    source_version_ids: set[str] = field(default_factory=set)
    generator_ids: set[str] = field(default_factory=set)
    generator_versions: set[str] = field(default_factory=set)
    generator_config_ids: set[str] = field(default_factory=set)
    reference_ids: set[str] = field(default_factory=set)
    motif_ids: set[str] = field(default_factory=set)
    feed_epoch_ids: set[str] = field(default_factory=set)
    broker_profile_ids: set[str] = field(default_factory=set)
    constraint_set_ids: set[str] = field(default_factory=set)
    stream_ids: set[str] = field(default_factory=set)
    digest: Any = field(
        default_factory=lambda: hashlib.sha256(b"activity-v1\n")
    )

    def add(self, event: _ActivityEventView) -> None:
        """Consume one ordered event using constant numeric state."""
        if self.last_time_ns is not None:
            gap = event.event_time_ns - self.last_time_ns
            if gap < 0:
                raise ValueError("activity events are not time ordered")
            self.interarrival_count += 1
            self.interarrival_total_ns += gap
            self.min_interarrival_ns = (
                gap
                if self.min_interarrival_ns is None
                else min(self.min_interarrival_ns, gap)
            )
            self.max_interarrival_ns = (
                gap
                if self.max_interarrival_ns is None
                else max(self.max_interarrival_ns, gap)
            )
            if event.bid == self.last_bid and event.ask == self.last_ask:
                self.stale_quote_count += 1
            else:
                self.price_change_count += 1
        else:
            self.first_time_ns = event.event_time_ns
        self.last_time_ns = event.event_time_ns
        self.last_bid = event.bid
        self.last_ask = event.ask
        self.event_count += 1
        spread = event.ask - event.bid
        self.spread_total += spread
        self.min_spread = (
            spread if self.min_spread is None else min(self.min_spread, spread)
        )
        self.max_spread = (
            spread if self.max_spread is None else max(self.max_spread, spread)
        )
        if event.confidence is not None:
            self.confidence_total += event.confidence
            self.confidence_count += 1
        self._add_provenance("source_version_ids", event.source_version_id)
        for name in (
            "generator_id",
            "generator_version",
            "generator_config_id",
            "reference_id",
            "motif_id",
            "feed_epoch_id",
            "broker_profile_id",
            "constraint_set_id",
        ):
            value = getattr(event, name)
            if value is not None:
                self._add_provenance(name + "s", value)
        for stream_id in event.stream_ids:
            self._add_provenance("stream_ids", stream_id)
        row = {
            "event_id": event.event_id,
            "origin": event.origin.value,
            "symbol": event.symbol,
            "event_time_ns": event.event_time_ns,
            "event_sequence": event.event_sequence,
            "bid": event.bid,
            "ask": event.ask,
            "run_id": event.run_id,
            "ensemble_member_id": event.ensemble_member_id,
            "source_version_id": event.source_version_id,
            "generator_id": event.generator_id,
            "generator_version": event.generator_version,
            "generator_config_id": event.generator_config_id,
            "reference_id": event.reference_id,
            "motif_id": event.motif_id,
            "feed_epoch_id": event.feed_epoch_id,
            "broker_profile_id": event.broker_profile_id,
            "constraint_set_id": event.constraint_set_id,
            "confidence": event.confidence,
        }
        self.digest.update(canonical_contract_json(row).encode("utf-8"))
        self.digest.update(b"\n")

    def _add_provenance(self, name: str, value: str) -> None:
        values = cast(set[str], getattr(self, name))
        values.add(_required_text(value))
        if len(values) > self.policy.max_provenance_values:
            raise ValueError(f"activity {name} exceeds provenance limit")

    def finalize(self) -> ReconstructionActivitySliceV1:
        """Freeze the online state into a strict bounded slice."""
        if (
            not self.event_count
            or self.first_time_ns is None
            or self.last_time_ns is None
            or self.min_spread is None
            or self.max_spread is None
        ):
            raise ValueError("cannot finalize an empty activity slice")
        duration = self.last_time_ns - self.first_time_ns
        mean_confidence = (
            self.confidence_total / self.confidence_count
            if self.confidence_count
            else None
        )
        limitations = ["centralized_traded_volume_unavailable"]
        if duration == 0:
            limitations.append("zero_exposure_duration")
        if self.confidence_count < self.event_count:
            limitations.append("event_confidence_partial_or_unavailable")
        if not self.stream_ids:
            limitations.append("stream_identity_unavailable")
        values: dict[str, tuple[int | float | None, int, tuple[str, ...]]] = {
            "event_count": (self.event_count, self.event_count, ()),
            "quote_update_count": (self.event_count, self.event_count, ()),
            "exposure_duration_ns": (duration, self.event_count, ()),
            "tick_intensity_per_second": (
                (
                    self.event_count * NANOSECONDS_PER_SECOND / duration
                    if duration
                    else None
                ),
                self.event_count if duration else 0,
                () if duration else ("zero_exposure_duration",),
            ),
            "mean_interarrival_ns": (
                (
                    self.interarrival_total_ns / self.interarrival_count
                    if self.interarrival_count
                    else None
                ),
                self.interarrival_count,
                () if self.interarrival_count else ("no_quote_transition",),
            ),
            "min_interarrival_ns": (
                self.min_interarrival_ns,
                self.interarrival_count,
                () if self.interarrival_count else ("no_quote_transition",),
            ),
            "max_interarrival_ns": (
                self.max_interarrival_ns,
                self.interarrival_count,
                () if self.interarrival_count else ("no_quote_transition",),
            ),
            "price_change_count": (
                self.price_change_count,
                self.interarrival_count,
                (),
            ),
            "stale_quote_count": (
                self.stale_quote_count,
                self.interarrival_count,
                (),
            ),
            "stale_quote_rate": (
                (
                    self.stale_quote_count / self.interarrival_count
                    if self.interarrival_count
                    else None
                ),
                self.interarrival_count,
                () if self.interarrival_count else ("no_quote_transition",),
            ),
            "mean_spread": (
                self.spread_total / self.event_count,
                self.event_count,
                ("spread_is_a_liquidity_proxy_not_traded_volume",),
            ),
            "min_spread": (
                self.min_spread,
                self.event_count,
                ("spread_is_a_liquidity_proxy_not_traded_volume",),
            ),
            "max_spread": (
                self.max_spread,
                self.event_count,
                ("spread_is_a_liquidity_proxy_not_traded_volume",),
            ),
            "mean_event_confidence": (
                mean_confidence,
                self.confidence_count,
                (
                    ()
                    if self.confidence_count == self.event_count
                    else ("event_confidence_partial_or_unavailable",)
                ),
            ),
        }
        metrics = tuple(
            _activity_metric(
                name,
                _rounded_optional(value[0], self.policy.rounding_digits),
                value[1],
                value[2],
            )
            for name, value in values.items()
        )
        return ReconstructionActivitySliceV1(
            symbol=self.symbol,
            scope=self.scope,
            start_event_time_ns=self.first_time_ns,
            end_event_time_ns=self.last_time_ns,
            metrics=metrics,
            source_version_ids=tuple(self.source_version_ids),
            generator_ids=tuple(self.generator_ids),
            generator_versions=tuple(self.generator_versions),
            generator_config_ids=tuple(self.generator_config_ids),
            reference_ids=tuple(self.reference_ids),
            motif_ids=tuple(self.motif_ids),
            feed_epoch_ids=tuple(self.feed_epoch_ids),
            broker_profile_ids=tuple(self.broker_profile_ids),
            constraint_set_ids=tuple(self.constraint_set_ids),
            stream_ids=tuple(self.stream_ids),
            event_content_sha256=self.digest.hexdigest(),
            confidence_support_count=self.confidence_count,
            limitations=tuple(limitations),
        )


class _ActivityAccumulator:
    """Route ordered events into bounded symbol/origin slice states."""

    def __init__(
        self,
        *,
        run_id: str,
        ensemble_member_id: str,
        policy: ReconstructionActivityPolicyV1,
    ) -> None:
        self.run_id = _required_text(run_id)
        self.ensemble_member_id = _required_text(ensemble_member_id)
        self.policy = policy
        self.states: dict[
            tuple[str, ActivitySliceScope], _ActivitySliceState
        ] = {}
        self.last_positions: dict[str, tuple[int, int]] = {}
        self.symbols: set[str] = set()

    def add(self, event: _ActivityEventView) -> None:
        if event.run_id != self.run_id:
            raise ValueError("activity event run_id differs")
        if event.ensemble_member_id != self.ensemble_member_id:
            raise ValueError("activity event ensemble member differs")
        symbol = _normalized_symbol(event.symbol)
        position = (event.event_time_ns, event.event_sequence)
        previous = self.last_positions.get(symbol)
        if previous is not None and position <= previous:
            raise ValueError(
                "activity events are not strictly ordered per symbol"
            )
        self.last_positions[symbol] = position
        self.symbols.add(symbol)
        if len(self.symbols) > self.policy.max_symbols:
            raise ValueError("activity symbols exceed policy limit")
        origin_scope = (
            ActivitySliceScope.OBSERVED
            if event.origin is SyntheticEventOrigin.OBSERVED
            else ActivitySliceScope.SYNTHETIC
        )
        scopes = (origin_scope, ActivitySliceScope.MERGED)
        for scope in scopes:
            if scope not in self.policy.scopes:
                continue
            key = (symbol, scope)
            state = self.states.get(key)
            if state is None:
                if len(self.states) >= self.policy.max_slices:
                    raise ValueError("activity slices exceed policy limit")
                state = _ActivitySliceState(symbol, scope, self.policy)
                self.states[key] = state
            state.add(event)

    def finalize(self) -> tuple[ReconstructionActivitySliceV1, ...]:
        if not self.states:
            raise ValueError("activity aggregation requires events")
        return tuple(state.finalize() for state in self.states.values())


def summarize_reconstruction_activity(
    events: Iterable[SyntheticEventV1],
    *,
    run_id: str,
    ensemble_member_id: str,
    information_mode: InformationMode,
    information_manifest_id: str,
    as_of_ns: int | None = None,
    policy: ReconstructionActivityPolicyV1 | None = None,
    stream_ids_by_symbol: Mapping[str, Iterable[str]] | None = None,
    window_id: str | None = None,
    synchronization_unit_id: str | None = None,
    product_manifest_id: str | None = None,
    calibration_report_id: str | None = None,
    benchmark_evidence: ReconstructionActivityBenchmarkEvidenceV1 | None = None,
) -> ReconstructionActivityManifestV1:
    """Aggregate ordered narrow events without retaining their rows."""
    selected_policy = policy or ReconstructionActivityPolicyV1()
    stream_map = {
        _normalized_symbol(symbol): _normalized_text_tuple(values)
        for symbol, values in (stream_ids_by_symbol or {}).items()
    }
    views = (
        _activity_event_view(
            event,
            stream_ids=stream_map.get(event.symbol, ()),
        )
        for event in events
    )
    return _summarize_activity_views(
        views,
        run_id=run_id,
        ensemble_member_id=ensemble_member_id,
        information_mode=information_mode,
        information_manifest_id=information_manifest_id,
        as_of_ns=as_of_ns,
        policy=selected_policy,
        window_id=window_id,
        synchronization_unit_id=synchronization_unit_id,
        product_manifest_id=product_manifest_id,
        calibration_report_id=calibration_report_id,
        benchmark_evidence=benchmark_evidence,
    )


def summarize_reconstruction_activity_streams(
    streams: Iterable[SyntheticEventStreamV1],
    *,
    information_mode: InformationMode,
    information_manifest_id: str,
    as_of_ns: int | None = None,
    policy: ReconstructionActivityPolicyV1 | None = None,
    window_id: str | None = None,
    synchronization_unit_id: str | None = None,
    product_manifest_id: str | None = None,
    calibration_report_id: str | None = None,
    benchmark_evidence: ReconstructionActivityBenchmarkEvidenceV1 | None = None,
) -> ReconstructionActivityManifestV1:
    """Aggregate one compatible stream group in bounded numeric state."""
    stream_tuple = tuple(streams)
    if not stream_tuple:
        raise ValueError("activity aggregation requires streams")
    if any(
        not isinstance(item, SyntheticEventStreamV1) for item in stream_tuple
    ):
        raise TypeError("activity aggregation requires v1 streams")
    run_ids = {item.run_id for item in stream_tuple}
    member_ids = {item.ensemble_member_id for item in stream_tuple}
    if len(run_ids) != 1 or len(member_ids) != 1:
        raise ValueError("activity streams differ in run or ensemble member")
    stream_map = {item.symbol: (item.stream_id,) for item in stream_tuple}
    return summarize_reconstruction_activity(
        (event for stream in stream_tuple for event in stream.events),
        run_id=next(iter(run_ids)),
        ensemble_member_id=next(iter(member_ids)),
        information_mode=information_mode,
        information_manifest_id=information_manifest_id,
        as_of_ns=as_of_ns,
        policy=policy,
        stream_ids_by_symbol=stream_map,
        window_id=window_id,
        synchronization_unit_id=synchronization_unit_id,
        product_manifest_id=product_manifest_id,
        calibration_report_id=calibration_report_id,
        benchmark_evidence=benchmark_evidence,
    )


def summarize_committed_reconstruction_activity(
    manifest_path: str | Path,
    *,
    information_mode: InformationMode,
    information_manifest_id: str,
    as_of_ns: int | None = None,
    policy: ReconstructionActivityPolicyV1 | None = None,
    calibration_report_id: str | None = None,
    benchmark_evidence: ReconstructionActivityBenchmarkEvidenceV1 | None = None,
    batch_size: int = DEFAULT_ACTIVITY_BATCH_SIZE,
) -> ReconstructionActivityManifestV1:
    """Stream projected final Parquet columns into compact activity evidence."""
    product = verify_reconstruction_publication(manifest_path)
    size = _bounded_int(batch_size, "batch_size", 1, 1_000_000)
    stream_map: dict[str, tuple[str, ...]] = {}
    for symbol in product.symbols:
        stream_map[symbol] = _normalized_text_tuple(
            partition.stream_id
            for partition in product.partitions
            if partition.symbol == symbol
        )

    def views() -> Iterable[_ActivityEventView]:
        for batch in iter_reconstruction_event_batches(
            manifest_path,
            columns=ACTIVITY_EVENT_COLUMNS,
            batch_size=size,
        ):
            for row in batch.to_pylist():
                mapping = _mapping(row, "activity event row")
                symbol = _normalized_symbol(str(mapping.get("symbol", "")))
                yield _activity_event_view_from_mapping(
                    mapping,
                    stream_ids=stream_map.get(symbol, ()),
                )

    return _summarize_activity_views(
        views(),
        run_id=product.run_id,
        ensemble_member_id=product.ensemble_member_id,
        information_mode=information_mode,
        information_manifest_id=information_manifest_id,
        as_of_ns=as_of_ns,
        policy=policy or ReconstructionActivityPolicyV1(),
        window_id=product.window_id,
        synchronization_unit_id=product.synchronization_unit_id,
        product_manifest_id=product.manifest_id,
        calibration_report_id=calibration_report_id,
        benchmark_evidence=benchmark_evidence,
    )


def reconstruction_activity_benchmark_evidence(
    scorecard: ReverseDegradationScorecardV1,
    *,
    rounding_digits: int = DEFAULT_ACTIVITY_ROUNDING_DIGITS,
) -> ReconstructionActivityBenchmarkEvidenceV1:
    """Extract the canonical activity metrics from shared benchmark evidence."""
    if not isinstance(scorecard, ReverseDegradationScorecardV1):
        raise TypeError("activity evidence requires a v1 benchmark scorecard")
    rounding = _bounded_int(rounding_digits, "rounding_digits", 0, 15)
    totals = dict.fromkeys(ACTIVITY_REVERSE_DEGRADATION_METRICS, 0.0)
    supports = dict.fromkeys(ACTIVITY_REVERSE_DEGRADATION_METRICS, 0)
    restoration: list[float] = []
    calibration_supported = 0
    execution_failures = 0
    for candidate in scorecard.candidate_scores:
        for slice_score in candidate.slice_scores:
            for name in ACTIVITY_REVERSE_DEGRADATION_METRICS:
                if name not in slice_score.metrics:
                    raise ValueError(
                        f"benchmark slice lacks activity metric {name}"
                    )
                value = _nonnegative_float(
                    slice_score.metrics[name], f"benchmark.{name}"
                )
                totals[name] += value
                supports[name] += 1
            restoration.append(
                _finite_float(
                    slice_score.metrics.get(
                        "restoration_gain_vs_degraded", 0.0
                    ),
                    "restoration_gain_vs_degraded",
                )
            )
        support_intervals = candidate.uncertainty_metrics.get(
            "support_interval_count", 0
        )
        if isinstance(support_intervals, int) and support_intervals > 0:
            calibration_supported += 1
        failures = candidate.execution_summary.get("failure_count", 0)
        if not isinstance(failures, int) or isinstance(failures, bool):
            raise ValueError("benchmark failure_count must be integral")
        execution_failures += failures
    mean_errors = {
        name: round(totals[name] / supports[name], rounding)
        for name in ACTIVITY_REVERSE_DEGRADATION_METRICS
    }
    return ReconstructionActivityBenchmarkEvidenceV1(
        scorecard_id=scorecard.scorecard_id,
        candidate_score_ids=tuple(
            item.candidate_score_id for item in scorecard.candidate_scores
        ),
        split_kinds=tuple(
            item.split_kind.value for item in scorecard.candidate_scores
        ),
        metric_support_counts=supports,
        metric_mean_errors=mean_errors,
        mean_restoration_gain_vs_degraded=round(
            sum(restoration) / len(restoration), rounding
        ),
        promotion_eligible_candidate_count=sum(
            item.promotion_eligible for item in scorecard.candidate_scores
        ),
        calibration_supported_candidate_count=calibration_supported,
        execution_failure_count=execution_failures,
    )


def _summarize_activity_views(
    views: Iterable[_ActivityEventView],
    *,
    run_id: str,
    ensemble_member_id: str,
    information_mode: InformationMode,
    information_manifest_id: str,
    as_of_ns: int | None,
    policy: ReconstructionActivityPolicyV1,
    window_id: str | None,
    synchronization_unit_id: str | None,
    product_manifest_id: str | None,
    calibration_report_id: str | None,
    benchmark_evidence: ReconstructionActivityBenchmarkEvidenceV1 | None,
) -> ReconstructionActivityManifestV1:
    if policy.volume_state in (
        ActivityVolumeState.OBSERVED_SOURCE_SIZE,
        ActivityVolumeState.BROKER_SUPPLIED_SIZE,
    ):
        raise ValueError(
            "final event schema has no source-supported size fields"
        )
    accumulator = _ActivityAccumulator(
        run_id=run_id,
        ensemble_member_id=ensemble_member_id,
        policy=policy,
    )
    for event in views:
        accumulator.add(event)
    slices = accumulator.finalize()
    input_hash = _content_sha256(
        {
            "slice_content": [
                [item.symbol, item.scope.value, item.event_content_sha256]
                for item in sorted(
                    slices, key=lambda value: (value.symbol, value.scope.value)
                )
            ]
        }
    )
    return ReconstructionActivityManifestV1(
        run_id=run_id,
        ensemble_member_id=ensemble_member_id,
        information_mode=information_mode,
        information_manifest_id=information_manifest_id,
        as_of_ns=as_of_ns,
        policy=policy,
        slices=slices,
        input_content_sha256=input_hash,
        window_id=window_id,
        synchronization_unit_id=synchronization_unit_id,
        product_manifest_id=product_manifest_id,
        calibration_report_id=calibration_report_id,
        benchmark_evidence=benchmark_evidence,
    )


def _activity_event_view(
    event: SyntheticEventV1,
    *,
    stream_ids: tuple[str, ...],
) -> _ActivityEventView:
    if not isinstance(event, SyntheticEventV1):
        raise TypeError("activity aggregation requires v1 events")
    return _ActivityEventView(
        event_id=event.event_id,
        origin=event.origin,
        symbol=event.symbol,
        event_time_ns=event.event_time_ns,
        event_sequence=event.event_sequence,
        bid=event.bid,
        ask=event.ask,
        run_id=event.run_id,
        ensemble_member_id=event.ensemble_member_id,
        source_version_id=event.source_version_id,
        generator_id=event.generator_id,
        generator_version=event.generator_version,
        generator_config_id=event.generator_config_id,
        reference_id=event.reference_id,
        motif_id=event.motif_id,
        feed_epoch_id=event.feed_epoch_id,
        broker_profile_id=event.broker_profile_id,
        constraint_set_id=event.constraint_set_id,
        confidence=event.confidence,
        stream_ids=stream_ids,
    )


def _activity_event_view_from_mapping(
    data: Mapping[str, Any],
    *,
    stream_ids: tuple[str, ...],
) -> _ActivityEventView:
    bid = _positive_float(data.get("bid"), "bid")
    ask = _positive_float(data.get("ask"), "ask")
    if ask < bid:
        raise ValueError("activity ask must not be below bid")
    return _ActivityEventView(
        event_id=_required_text(str(data.get("event_id", ""))),
        origin=SyntheticEventOrigin.from_value(str(data.get("origin", ""))),
        symbol=_normalized_symbol(str(data.get("symbol", ""))),
        event_time_ns=_strict_int(data.get("event_time_ns"), "event_time_ns"),
        event_sequence=_strict_int(
            data.get("event_sequence"), "event_sequence"
        ),
        bid=bid,
        ask=ask,
        run_id=_required_text(str(data.get("run_id", ""))),
        ensemble_member_id=_required_text(
            str(data.get("ensemble_member_id", ""))
        ),
        source_version_id=_required_text(
            str(data.get("source_version_id", ""))
        ),
        generator_id=_mapping_optional_text(data, "generator_id"),
        generator_version=_mapping_optional_text(data, "generator_version"),
        generator_config_id=_mapping_optional_text(data, "generator_config_id"),
        reference_id=_mapping_optional_text(data, "reference_id"),
        motif_id=_mapping_optional_text(data, "motif_id"),
        feed_epoch_id=_mapping_optional_text(data, "feed_epoch_id"),
        broker_profile_id=_mapping_optional_text(data, "broker_profile_id"),
        constraint_set_id=_mapping_optional_text(data, "constraint_set_id"),
        confidence=_optional_float(data.get("confidence"), "confidence"),
        stream_ids=stream_ids,
    )


def _activity_metric(
    name: str,
    value: int | float | None,
    support_count: int,
    limitations: tuple[str, ...],
) -> ReconstructionActivityMetricV1:
    unit, aggregation, semantics = _METRIC_DEFINITIONS[name]
    return ReconstructionActivityMetricV1(
        name=name,
        value=value,
        unit=unit,
        aggregation=aggregation,
        semantics=semantics,
        support_count=support_count,
        limitations=limitations,
    )


def _validate_slice_reconciliation(
    slices: Sequence[ReconstructionActivitySliceV1],
) -> None:
    by_symbol: dict[str, dict[ActivitySliceScope, int]] = {}
    for item in slices:
        by_symbol.setdefault(item.symbol, {})[item.scope] = item.event_count
    for scopes in by_symbol.values():
        if ActivitySliceScope.MERGED not in scopes:
            continue
        origin_total = sum(
            scopes.get(scope, 0)
            for scope in (
                ActivitySliceScope.OBSERVED,
                ActivitySliceScope.SYNTHETIC,
            )
        )
        if (
            ActivitySliceScope.OBSERVED in scopes
            and ActivitySliceScope.SYNTHETIC in scopes
            and scopes[ActivitySliceScope.MERGED] != origin_total
        ):
            raise ValueError("activity merged/origin counts do not reconcile")


def _rounded_optional(
    value: int | float | None, digits: int
) -> int | float | None:
    if value is None or isinstance(value, int):
        return value
    return round(value, digits)


def _stable_id(kind: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{kind}:sha256:{digest}"


def _content_sha256(payload: Mapping[str, JSONValue]) -> str:
    return hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()


def _enum_value(cls: type[_EnumT], value: str | _EnumT, name: str) -> _EnumT:
    if isinstance(value, cls):
        return value
    try:
        return cls(str(value).strip().lower())
    except ValueError as err:
        raise ValueError(f"unsupported {name}") from err


def _required_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("expected text")
    normalized = value.strip()
    if not normalized or len(normalized) > MAX_ACTIVITY_TEXT:
        raise ValueError("text is empty or too long")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return _required_text(value)


def _normalized_text_tuple(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _normalized_symbol(value: str) -> str:
    symbol = _required_text(value).lower()
    if not symbol.isalnum() or len(symbol) > 32:
        raise ValueError("unsupported activity symbol")
    return symbol


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _optional_int(value: Any, name: str) -> int | None:
    if value is None:
        return None
    return _strict_int(value, name)


def _bounded_int(value: Any, name: str, minimum: int, maximum: int) -> int:
    selected = _strict_int(value, name)
    if selected < minimum or selected > maximum:
        raise ValueError(f"{name} is outside the supported range")
    return selected


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be numeric")
    selected = float(value)
    if not math.isfinite(selected):
        raise ValueError(f"{name} must be finite")
    return selected


def _positive_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected <= 0.0:
        raise ValueError(f"{name} must be positive")
    return selected


def _nonnegative_float(value: Any, name: str) -> float:
    selected = _finite_float(value, name)
    if selected < 0.0:
        raise ValueError(f"{name} must be non-negative")
    return selected


def _optional_float(value: Any, name: str) -> float | None:
    if value is None:
        return None
    return _finite_float(value, name)


def _required_sha256(value: Any, name: str) -> str:
    text = _required_text(value)
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ValueError(f"{name} must be lowercase SHA-256")
    return text


def _require_version(actual: str, expected: str, name: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {name} schema")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError(f"unsupported schema version; expected {expected}")


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived activity field {name} differs")


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{name} must be a mapping")
    return cast(Mapping[str, Any], value)


def _mapping_sequence(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} must be a sequence")
    return tuple(_mapping(item, name) for item in value)


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise ValueError(f"{name} must be a sequence")
    return value


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    selected = _sequence(value, name)
    if any(not isinstance(item, str) for item in selected):
        raise ValueError(f"{name} must contain strings")
    return tuple(cast(Sequence[str], selected))


def _mapping_optional_text(data: Mapping[str, Any], name: str) -> str | None:
    return _optional_text(data.get(name))


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError) as err:
        raise ValueError("invalid activity contract JSON") from err
    return _mapping(value, "activity JSON")


__all__ = [
    "ACTIVITY_EVENT_COLUMNS",
    "ACTIVITY_REVERSE_DEGRADATION_METRICS",
    "DEFAULT_ACTIVITY_BATCH_SIZE",
    "DEFAULT_ACTIVITY_MAX_PAYLOAD_BYTES",
    "DEFAULT_ACTIVITY_MAX_PROVENANCE_VALUES",
    "DEFAULT_ACTIVITY_MAX_SLICES",
    "DEFAULT_ACTIVITY_MAX_SYMBOLS",
    "DEFAULT_ACTIVITY_ROUNDING_DIGITS",
    "RECONSTRUCTION_ACTIVITY_BENCHMARK_EVIDENCE_SCHEMA_VERSION",
    "RECONSTRUCTION_ACTIVITY_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_ACTIVITY_METRIC_SCHEMA_VERSION",
    "RECONSTRUCTION_ACTIVITY_POLICY_SCHEMA_VERSION",
    "RECONSTRUCTION_ACTIVITY_SLICE_SCHEMA_VERSION",
    "ActivityAggregationSemantics",
    "ActivityMetricSemantics",
    "ActivitySliceScope",
    "ActivityVolumeState",
    "ReconstructionActivityBenchmarkEvidenceV1",
    "ReconstructionActivityManifestV1",
    "ReconstructionActivityMetricV1",
    "ReconstructionActivityPolicyV1",
    "ReconstructionActivitySliceV1",
    "activity_bar_projection_semantics",
    "activity_metric_definitions",
    "reconstruction_activity_benchmark_evidence",
    "summarize_committed_reconstruction_activity",
    "summarize_reconstruction_activity",
    "summarize_reconstruction_activity_streams",
]
