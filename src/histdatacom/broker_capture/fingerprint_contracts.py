"""Immutable contracts for broker-delivery fingerprints and drift evidence.

These profiles describe one broker observation/delivery system over an explicit
support interval.  They are compact fitted artifacts, not augmented capture
rows and not claims about the whole market.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeVar, cast

from histdatacom.broker_capture.contracts import (
    BROKER_CAPTURE_COLLECTOR_VERSION,
    canonical_capture_json,
)
from histdatacom.runtime_contracts import JSONValue

BROKER_DELIVERY_FIT_CONFIG_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-fit-config.v1"
)
BROKER_CAPTURE_ELIGIBILITY_SCHEMA_VERSION = (
    "histdatacom.broker-capture-eligibility.v1"
)
BROKER_DELIVERY_CAPTURE_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-capture-evidence.v1"
)
BROKER_DELIVERY_CONDITION_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-condition.v1"
)
BROKER_DELIVERY_METRIC_SCHEMA_VERSION = "histdatacom.broker-delivery-metric.v1"
BROKER_DELIVERY_CELL_SCHEMA_VERSION = "histdatacom.broker-delivery-cell.v1"
BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-fingerprint.v1"
)
BROKER_DELIVERY_DRIFT_CONFIG_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-drift-config.v1"
)
BROKER_DELIVERY_METRIC_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-metric-comparison.v1"
)
BROKER_DELIVERY_FINGERPRINT_COMPARISON_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-fingerprint-comparison.v1"
)
BROKER_DELIVERY_FINGERPRINT_ARTIFACT_KIND = "broker_delivery_fingerprint_json"

MAX_BROKER_DELIVERY_CAPTURES = 64
MAX_BROKER_DELIVERY_CELLS = 2_048
MAX_BROKER_DELIVERY_METRICS_PER_CELL = 256
MAX_BROKER_DELIVERY_COMPARISONS = 8_192
MAX_BROKER_DELIVERY_COMPARISON_CANDIDATES = (
    2 * MAX_BROKER_DELIVERY_CELLS * MAX_BROKER_DELIVERY_METRICS_PER_CELL
)
MAX_BROKER_DELIVERY_REASONS = 128
MAX_BROKER_DELIVERY_CATEGORIES = 256
MAX_BROKER_DELIVERY_TEXT = 1_024
MAX_BROKER_DELIVERY_EVENTS = 100_000_000
MAX_BROKER_DELIVERY_SAMPLES = 65_536
MAX_BROKER_DELIVERY_CONTEXT_EVENTS = 16_384
INT64_MAX = 2**63 - 1

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$")
_CONDITION_DIMENSIONS = frozenset(
    {"symbol", "session", "overlap", "special", "holiday", "event", "lifecycle"}
)
_EnumT = TypeVar("_EnumT", bound=Enum)


class BrokerCaptureEligibilityStatus(str, Enum):
    """Whether a capture may be used for profile fitting."""

    ELIGIBLE = "eligible"
    LIMITED = "limited"
    INELIGIBLE = "ineligible"

    @classmethod
    def from_value(
        cls, value: str | "BrokerCaptureEligibilityStatus"
    ) -> "BrokerCaptureEligibilityStatus":
        return _enum_value(cls, value, "capture eligibility status")


class BrokerDeliverySupportStatus(str, Enum):
    """Support decision for one conditioned fingerprint cell."""

    SUPPORTED = "supported"
    BACKED_OFF = "backed_off"
    UNSUPPORTED = "unsupported"

    @classmethod
    def from_value(
        cls, value: str | "BrokerDeliverySupportStatus"
    ) -> "BrokerDeliverySupportStatus":
        return _enum_value(cls, value, "broker delivery support status")


class BrokerDeliveryDriftStatus(str, Enum):
    """Evidence-backed state for one stratified metric comparison."""

    STABLE = "stable"
    SAMPLING_NOISE = "sampling_noise"
    MATERIAL_DRIFT = "material_drift"
    UNSUPPORTED = "unsupported"

    @classmethod
    def from_value(
        cls, value: str | "BrokerDeliveryDriftStatus"
    ) -> "BrokerDeliveryDriftStatus":
        return _enum_value(cls, value, "broker delivery drift status")


@dataclass(frozen=True, slots=True)
class BrokerDeliveryFitConfigV1:
    """Deterministic resource, support, health, and conditioning policy."""

    min_capture_events: int = 8
    min_quote_events: int = 3
    min_cell_support: int = 4
    max_capture_manifests: int = 16
    max_input_events: int = 10_000_000
    max_cells: int = 512
    max_samples_per_metric: int = 4_096
    max_transition_categories: int = 64
    max_market_context_events: int = 4_096
    max_market_matches_per_quote: int = 8
    burst_interval_ns: int = 100_000_000
    quiet_interval_ns: int = 5_000_000_000
    stale_max_interval_ns: int = 5_000_000_000
    post_lifecycle_quote_count: int = 8
    max_clock_correction_events: int = 32
    max_abs_clock_correction_ns: int = 60_000_000_000
    max_unexplained_wall_regressions: int = 0
    quantiles: tuple[float, ...] = (0.05, 0.25, 0.5, 0.75, 0.95, 0.99)
    supported_collector_versions: tuple[str, ...] = (
        BROKER_CAPTURE_COLLECTOR_VERSION,
    )
    supported_adapter_ids: tuple[str, ...] = ()
    fatal_limitation_prefixes: tuple[str, ...] = (
        "collector_failure",
        "quota",
        "retention",
        "backpressure",
    )
    rounding_digits: int = 9
    config_id: str = ""
    schema_version: str = BROKER_DELIVERY_FIT_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_DELIVERY_FIT_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported broker delivery fit config schema")
        for name, minimum, maximum in (
            ("min_capture_events", 1, MAX_BROKER_DELIVERY_EVENTS),
            ("min_quote_events", 1, MAX_BROKER_DELIVERY_EVENTS),
            ("min_cell_support", 1, MAX_BROKER_DELIVERY_EVENTS),
            ("max_capture_manifests", 1, MAX_BROKER_DELIVERY_CAPTURES),
            ("max_input_events", 1, MAX_BROKER_DELIVERY_EVENTS),
            ("max_cells", 1, MAX_BROKER_DELIVERY_CELLS),
            ("max_samples_per_metric", 2, MAX_BROKER_DELIVERY_SAMPLES),
            ("max_transition_categories", 1, MAX_BROKER_DELIVERY_CATEGORIES),
            (
                "max_market_context_events",
                1,
                MAX_BROKER_DELIVERY_CONTEXT_EVENTS,
            ),
            ("max_market_matches_per_quote", 1, MAX_BROKER_DELIVERY_CATEGORIES),
            ("burst_interval_ns", 1, INT64_MAX),
            ("quiet_interval_ns", 1, INT64_MAX),
            ("stale_max_interval_ns", 1, INT64_MAX),
            ("post_lifecycle_quote_count", 1, 1_000_000),
            ("max_clock_correction_events", 0, MAX_BROKER_DELIVERY_EVENTS),
            ("max_abs_clock_correction_ns", 1, INT64_MAX),
            ("max_unexplained_wall_regressions", 0, MAX_BROKER_DELIVERY_EVENTS),
        ):
            _bounded_int(getattr(self, name), name, minimum, maximum)
        if self.burst_interval_ns >= self.quiet_interval_ns:
            raise ValueError(
                "burst interval must be smaller than quiet interval"
            )
        quantiles = tuple(sorted(set(self.quantiles)))
        if not quantiles or any(not 0.0 < value < 1.0 for value in quantiles):
            raise ValueError(
                "fit quantiles must lie strictly between zero and one"
            )
        object.__setattr__(self, "quantiles", quantiles)
        for name in (
            "supported_collector_versions",
            "supported_adapter_ids",
            "fatal_limitation_prefixes",
        ):
            object.__setattr__(
                self,
                name,
                _bounded_text_tuple(
                    getattr(self, name), name, allow_empty=True
                ),
            )
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between zero and sixteen")
        expected = _stable_id(
            "broker-delivery-fit-config", self.identity_payload()
        )
        supplied = str(self.config_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("config_id does not match deterministic identity")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "min_capture_events": self.min_capture_events,
            "min_quote_events": self.min_quote_events,
            "min_cell_support": self.min_cell_support,
            "max_capture_manifests": self.max_capture_manifests,
            "max_input_events": self.max_input_events,
            "max_cells": self.max_cells,
            "max_samples_per_metric": self.max_samples_per_metric,
            "max_transition_categories": self.max_transition_categories,
            "max_market_context_events": self.max_market_context_events,
            "max_market_matches_per_quote": self.max_market_matches_per_quote,
            "burst_interval_ns": self.burst_interval_ns,
            "quiet_interval_ns": self.quiet_interval_ns,
            "stale_max_interval_ns": self.stale_max_interval_ns,
            "post_lifecycle_quote_count": self.post_lifecycle_quote_count,
            "max_clock_correction_events": self.max_clock_correction_events,
            "max_abs_clock_correction_ns": self.max_abs_clock_correction_ns,
            "max_unexplained_wall_regressions": (
                self.max_unexplained_wall_regressions
            ),
            "quantiles": list(self.quantiles),
            "supported_collector_versions": list(
                self.supported_collector_versions
            ),
            "supported_adapter_ids": list(self.supported_adapter_ids),
            "fatal_limitation_prefixes": list(self.fatal_limitation_prefixes),
            "rounding_digits": self.rounding_digits,
            "sampling_policy": "deterministic-bottom-hash",
            "capture_passes": ["health_and_integrity", "bounded_fit"],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerDeliveryFitConfigV1":
        _require_schema(data, BROKER_DELIVERY_FIT_CONFIG_SCHEMA_VERSION)
        return cls(
            min_capture_events=_strict_int(data.get("min_capture_events")),
            min_quote_events=_strict_int(data.get("min_quote_events")),
            min_cell_support=_strict_int(data.get("min_cell_support")),
            max_capture_manifests=_strict_int(
                data.get("max_capture_manifests")
            ),
            max_input_events=_strict_int(data.get("max_input_events")),
            max_cells=_strict_int(data.get("max_cells")),
            max_samples_per_metric=_strict_int(
                data.get("max_samples_per_metric")
            ),
            max_transition_categories=_strict_int(
                data.get("max_transition_categories")
            ),
            max_market_context_events=_strict_int(
                data.get("max_market_context_events")
            ),
            max_market_matches_per_quote=_strict_int(
                data.get("max_market_matches_per_quote")
            ),
            burst_interval_ns=_strict_int(data.get("burst_interval_ns")),
            quiet_interval_ns=_strict_int(data.get("quiet_interval_ns")),
            stale_max_interval_ns=_strict_int(
                data.get("stale_max_interval_ns")
            ),
            post_lifecycle_quote_count=_strict_int(
                data.get("post_lifecycle_quote_count")
            ),
            max_clock_correction_events=_strict_int(
                data.get("max_clock_correction_events")
            ),
            max_abs_clock_correction_ns=_strict_int(
                data.get("max_abs_clock_correction_ns")
            ),
            max_unexplained_wall_regressions=_strict_int(
                data.get("max_unexplained_wall_regressions")
            ),
            quantiles=tuple(
                _finite_float(value)
                for value in _sequence(data.get("quantiles"))
            ),
            supported_collector_versions=_string_tuple(
                data.get("supported_collector_versions")
            ),
            supported_adapter_ids=_string_tuple(
                data.get("supported_adapter_ids")
            ),
            fatal_limitation_prefixes=_string_tuple(
                data.get("fatal_limitation_prefixes")
            ),
            rounding_digits=_strict_int(data.get("rounding_digits")),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerCaptureEligibilityV1:
    """Immutable health and integrity decision for one capture manifest."""

    session_id: str
    manifest_id: str
    config_id: str
    status: BrokerCaptureEligibilityStatus
    reason_codes: tuple[str, ...]
    manifest_complete: bool
    inspection_clean: bool
    integrity_verified: bool
    event_count: int
    quote_count: int
    clock_correction_count: int
    max_abs_clock_correction_ns: int
    explained_wall_regression_count: int
    unexplained_wall_regression_count: int
    first_receive_time_utc_ns: int | None
    last_receive_time_utc_ns: int | None
    logical_content_sha256: str | None
    decision_id: str = ""
    schema_version: str = BROKER_CAPTURE_ELIGIBILITY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_CAPTURE_ELIGIBILITY_SCHEMA_VERSION:
            raise ValueError("unsupported broker capture eligibility schema")
        for name in ("session_id", "manifest_id", "config_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "status",
            BrokerCaptureEligibilityStatus.from_value(self.status),
        )
        reasons = _bounded_text_tuple(
            self.reason_codes, "eligibility reason", allow_empty=True
        )
        if len(reasons) > MAX_BROKER_DELIVERY_REASONS:
            raise ValueError("too many eligibility reasons")
        object.__setattr__(self, "reason_codes", reasons)
        for name in (
            "event_count",
            "quote_count",
            "clock_correction_count",
            "explained_wall_regression_count",
            "unexplained_wall_regression_count",
        ):
            _bounded_int(
                getattr(self, name), name, 0, MAX_BROKER_DELIVERY_EVENTS
            )
        _bounded_int(
            self.max_abs_clock_correction_ns,
            "max_abs_clock_correction_ns",
            0,
            INT64_MAX,
        )
        for name in ("first_receive_time_utc_ns", "last_receive_time_utc_ns"):
            value = getattr(self, name)
            if value is not None:
                _bounded_int(value, name, 0, INT64_MAX)
        if (
            self.first_receive_time_utc_ns is not None
            and self.last_receive_time_utc_ns is not None
            and self.last_receive_time_utc_ns < self.first_receive_time_utc_ns
            and not self.clock_correction_count
        ):
            raise ValueError(
                "reversed wall-clock support requires correction evidence"
            )
        digest = _optional_sha256(self.logical_content_sha256)
        object.__setattr__(self, "logical_content_sha256", digest)
        if self.integrity_verified and digest is None:
            raise ValueError(
                "verified eligibility requires a logical content hash"
            )
        if self.status is BrokerCaptureEligibilityStatus.ELIGIBLE and reasons:
            raise ValueError("eligible capture cannot carry reason codes")
        if (
            self.status is BrokerCaptureEligibilityStatus.INELIGIBLE
            and not reasons
        ):
            raise ValueError("ineligible capture requires reason codes")
        expected = _stable_id(
            "broker-capture-eligibility", self.identity_payload()
        )
        supplied = str(self.decision_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "decision_id does not match deterministic identity"
            )
        object.__setattr__(self, "decision_id", expected)

    @property
    def fit_allowed(self) -> bool:
        return self.status is not BrokerCaptureEligibilityStatus.INELIGIBLE

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "manifest_id": self.manifest_id,
            "config_id": self.config_id,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "manifest_complete": self.manifest_complete,
            "inspection_clean": self.inspection_clean,
            "integrity_verified": self.integrity_verified,
            "event_count": self.event_count,
            "quote_count": self.quote_count,
            "clock_correction_count": self.clock_correction_count,
            "max_abs_clock_correction_ns": self.max_abs_clock_correction_ns,
            "explained_wall_regression_count": (
                self.explained_wall_regression_count
            ),
            "unexplained_wall_regression_count": (
                self.unexplained_wall_regression_count
            ),
            "first_receive_time_utc_ns": self.first_receive_time_utc_ns,
            "last_receive_time_utc_ns": self.last_receive_time_utc_ns,
            "logical_content_sha256": self.logical_content_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerCaptureEligibilityV1":
        _require_schema(data, BROKER_CAPTURE_ELIGIBILITY_SCHEMA_VERSION)
        return cls(
            session_id=str(data.get("session_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            config_id=str(data.get("config_id", "")),
            status=BrokerCaptureEligibilityStatus.from_value(
                str(data.get("status", ""))
            ),
            reason_codes=_string_tuple(data.get("reason_codes")),
            manifest_complete=_strict_bool(data.get("manifest_complete")),
            inspection_clean=_strict_bool(data.get("inspection_clean")),
            integrity_verified=_strict_bool(data.get("integrity_verified")),
            event_count=_strict_int(data.get("event_count")),
            quote_count=_strict_int(data.get("quote_count")),
            clock_correction_count=_strict_int(
                data.get("clock_correction_count")
            ),
            max_abs_clock_correction_ns=_strict_int(
                data.get("max_abs_clock_correction_ns")
            ),
            explained_wall_regression_count=_strict_int(
                data.get("explained_wall_regression_count")
            ),
            unexplained_wall_regression_count=_strict_int(
                data.get("unexplained_wall_regression_count")
            ),
            first_receive_time_utc_ns=_optional_int(
                data.get("first_receive_time_utc_ns")
            ),
            last_receive_time_utc_ns=_optional_int(
                data.get("last_receive_time_utc_ns")
            ),
            logical_content_sha256=_optional_text(
                data.get("logical_content_sha256")
            ),
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerDeliveryCaptureEvidenceV1:
    """Compact strong lineage for one qualified capture."""

    session_id: str
    manifest_id: str
    eligibility_decision_id: str
    logical_content_sha256: str
    partition_hashes_sha256: str
    partition_count: int
    event_count: int
    first_receive_time_utc_ns: int
    last_receive_time_utc_ns: int
    evidence_id: str = ""
    schema_version: str = BROKER_DELIVERY_CAPTURE_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BROKER_DELIVERY_CAPTURE_EVIDENCE_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported broker delivery capture evidence schema"
            )
        for name in ("session_id", "manifest_id", "eligibility_decision_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in ("logical_content_sha256", "partition_hashes_sha256"):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name))
            )
        _bounded_int(self.partition_count, "partition_count", 1, INT64_MAX)
        _bounded_int(
            self.event_count, "event_count", 1, MAX_BROKER_DELIVERY_EVENTS
        )
        start = _bounded_int(
            self.first_receive_time_utc_ns,
            "first_receive_time_utc_ns",
            0,
            INT64_MAX,
        )
        end = _bounded_int(
            self.last_receive_time_utc_ns,
            "last_receive_time_utc_ns",
            0,
            INT64_MAX,
        )
        if end < start:
            raise ValueError("capture evidence wall-clock support is reversed")
        expected = _stable_id(
            "broker-delivery-capture-evidence", self.identity_payload()
        )
        supplied = str(self.evidence_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "evidence_id does not match deterministic identity"
            )
        object.__setattr__(self, "evidence_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "manifest_id": self.manifest_id,
            "eligibility_decision_id": self.eligibility_decision_id,
            "logical_content_sha256": self.logical_content_sha256,
            "partition_hashes_sha256": self.partition_hashes_sha256,
            "partition_count": self.partition_count,
            "event_count": self.event_count,
            "first_receive_time_utc_ns": self.first_receive_time_utc_ns,
            "last_receive_time_utc_ns": self.last_receive_time_utc_ns,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "evidence_id": self.evidence_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerDeliveryCaptureEvidenceV1":
        _require_schema(data, BROKER_DELIVERY_CAPTURE_EVIDENCE_SCHEMA_VERSION)
        return cls(
            session_id=str(data.get("session_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            eligibility_decision_id=str(
                data.get("eligibility_decision_id", "")
            ),
            logical_content_sha256=str(data.get("logical_content_sha256", "")),
            partition_hashes_sha256=str(
                data.get("partition_hashes_sha256", "")
            ),
            partition_count=_strict_int(data.get("partition_count")),
            event_count=_strict_int(data.get("event_count")),
            first_receive_time_utc_ns=_strict_int(
                data.get("first_receive_time_utc_ns")
            ),
            last_receive_time_utc_ns=_strict_int(
                data.get("last_receive_time_utc_ns")
            ),
            evidence_id=str(data.get("evidence_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerDeliveryConditionV1:
    """Canonical dimensions defining one conditioned delivery cell."""

    dimensions: dict[str, str]
    condition_id: str = ""
    schema_version: str = BROKER_DELIVERY_CONDITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_DELIVERY_CONDITION_SCHEMA_VERSION:
            raise ValueError("unsupported broker delivery condition schema")
        normalized: dict[str, str] = {}
        for raw_name, raw_value in sorted(self.dimensions.items()):
            name = _required_name(raw_name)
            if name not in _CONDITION_DIMENSIONS:
                raise ValueError(
                    f"unsupported broker delivery dimension: {name}"
                )
            value = _required_name(raw_value)
            normalized[name] = (
                value.upper() if name == "symbol" else value.lower()
            )
        if len(normalized) > len(_CONDITION_DIMENSIONS):
            raise ValueError(
                "broker delivery condition has too many dimensions"
            )
        object.__setattr__(self, "dimensions", normalized)
        expected = _stable_id(
            "broker-delivery-condition", self.identity_payload()
        )
        supplied = str(self.condition_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "condition_id does not match deterministic identity"
            )
        object.__setattr__(self, "condition_id", expected)

    @property
    def key(self) -> str:
        if not self.dimensions:
            return "global"
        return "|".join(
            f"{name}={value}" for name, value in self.dimensions.items()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dimensions": dict(self.dimensions),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "condition_id": self.condition_id,
            "key": self.key,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerDeliveryConditionV1":
        _require_schema(data, BROKER_DELIVERY_CONDITION_SCHEMA_VERSION)
        condition = cls(
            dimensions={
                str(name): str(value)
                for name, value in _mapping(data.get("dimensions")).items()
            },
            condition_id=str(data.get("condition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        supplied_key = str(data.get("key", ""))
        if supplied_key and supplied_key != condition.key:
            raise ValueError("condition key does not match dimensions")
        return condition


@dataclass(frozen=True, slots=True)
class BrokerDeliveryMetricV1:
    """One bounded estimate with sample support and uncertainty interval."""

    name: str
    kind: str
    unit: str
    support_count: int
    sample_count: int
    estimate: float | None
    lower: float | None
    upper: float | None
    minimum: float | None = None
    maximum: float | None = None
    quantiles: dict[str, float] = field(default_factory=dict)
    category_counts: dict[str, int] = field(default_factory=dict)
    limitations: tuple[str, ...] = ()
    metric_id: str = ""
    schema_version: str = BROKER_DELIVERY_METRIC_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_DELIVERY_METRIC_SCHEMA_VERSION:
            raise ValueError("unsupported broker delivery metric schema")
        for name in ("name", "kind", "unit"):
            object.__setattr__(self, name, _required_name(getattr(self, name)))
        support = _bounded_int(
            self.support_count, "support_count", 0, MAX_BROKER_DELIVERY_EVENTS
        )
        sample = _bounded_int(
            self.sample_count, "sample_count", 0, MAX_BROKER_DELIVERY_SAMPLES
        )
        if sample > support:
            raise ValueError("metric sample_count exceeds support_count")
        estimate = _optional_finite_float(self.estimate)
        lower = _optional_finite_float(self.lower)
        upper = _optional_finite_float(self.upper)
        if estimate is None:
            if lower is not None or upper is not None:
                raise ValueError(
                    "unavailable metric cannot have uncertainty bounds"
                )
        elif lower is None or upper is None or not lower <= estimate <= upper:
            raise ValueError(
                "metric estimate must lie inside uncertainty bounds"
            )
        minimum = _optional_finite_float(self.minimum)
        maximum = _optional_finite_float(self.maximum)
        if (minimum is None) != (maximum is None):
            raise ValueError("metric extrema must be supplied together")
        if minimum is not None and maximum is not None and maximum < minimum:
            raise ValueError("metric maximum precedes minimum")
        quantiles = {
            _required_name(str(name)): _finite_float(value)
            for name, value in sorted(self.quantiles.items())
        }
        categories = {
            _required_name(str(name)): _bounded_int(
                value, f"category {name}", 0, MAX_BROKER_DELIVERY_EVENTS
            )
            for name, value in sorted(self.category_counts.items())
        }
        if len(categories) > MAX_BROKER_DELIVERY_CATEGORIES:
            raise ValueError("metric has too many categories")
        limitations = _bounded_text_tuple(
            self.limitations, "metric limitation", allow_empty=True
        )
        if estimate is None and not limitations:
            raise ValueError("unavailable metric requires a limitation")
        object.__setattr__(self, "support_count", support)
        object.__setattr__(self, "sample_count", sample)
        object.__setattr__(self, "estimate", estimate)
        object.__setattr__(self, "lower", lower)
        object.__setattr__(self, "upper", upper)
        object.__setattr__(self, "minimum", minimum)
        object.__setattr__(self, "maximum", maximum)
        object.__setattr__(self, "quantiles", quantiles)
        object.__setattr__(self, "category_counts", categories)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("broker-delivery-metric", self.identity_payload())
        supplied = str(self.metric_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("metric_id does not match deterministic identity")
        object.__setattr__(self, "metric_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "kind": self.kind,
            "unit": self.unit,
            "support_count": self.support_count,
            "sample_count": self.sample_count,
            "estimate": self.estimate,
            "lower": self.lower,
            "upper": self.upper,
            "minimum": self.minimum,
            "maximum": self.maximum,
            "quantiles": dict(self.quantiles),
            "category_counts": dict(self.category_counts),
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "metric_id": self.metric_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerDeliveryMetricV1":
        _require_schema(data, BROKER_DELIVERY_METRIC_SCHEMA_VERSION)
        return cls(
            name=str(data.get("name", "")),
            kind=str(data.get("kind", "")),
            unit=str(data.get("unit", "")),
            support_count=_strict_int(data.get("support_count")),
            sample_count=_strict_int(data.get("sample_count")),
            estimate=_optional_float(data.get("estimate")),
            lower=_optional_float(data.get("lower")),
            upper=_optional_float(data.get("upper")),
            minimum=_optional_float(data.get("minimum")),
            maximum=_optional_float(data.get("maximum")),
            quantiles={
                str(name): _finite_float(value)
                for name, value in _mapping(data.get("quantiles")).items()
            },
            category_counts={
                str(name): _strict_int(value)
                for name, value in _mapping(data.get("category_counts")).items()
            },
            limitations=_string_tuple(data.get("limitations")),
            metric_id=str(data.get("metric_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerDeliveryCellV1:
    """One conditioned profile cell and its explicit backoff decision."""

    condition: BrokerDeliveryConditionV1
    support_count: int
    support_status: BrokerDeliverySupportStatus
    backoff_condition_ids: tuple[str, ...]
    effective_condition_id: str | None
    metrics: tuple[BrokerDeliveryMetricV1, ...]
    limitations: tuple[str, ...] = ()
    cell_id: str = ""
    schema_version: str = BROKER_DELIVERY_CELL_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_DELIVERY_CELL_SCHEMA_VERSION:
            raise ValueError("unsupported broker delivery cell schema")
        if not isinstance(self.condition, BrokerDeliveryConditionV1):
            raise TypeError("condition must be BrokerDeliveryConditionV1")
        support = _bounded_int(
            self.support_count, "support_count", 0, MAX_BROKER_DELIVERY_EVENTS
        )
        status = BrokerDeliverySupportStatus.from_value(self.support_status)
        backoffs = _bounded_ordered_text_tuple(
            self.backoff_condition_ids, "backoff condition", allow_empty=True
        )
        if len(set(backoffs)) != len(backoffs):
            raise ValueError("cell has duplicate backoff conditions")
        effective = _optional_text(self.effective_condition_id)
        if status is BrokerDeliverySupportStatus.SUPPORTED:
            if effective != self.condition.condition_id:
                raise ValueError("supported cell must select itself")
        elif status is BrokerDeliverySupportStatus.BACKED_OFF:
            if effective is None or effective not in backoffs:
                raise ValueError(
                    "backed-off cell must select a declared parent"
                )
        elif effective is not None:
            raise ValueError("unsupported cell cannot select an effective cell")
        metrics = tuple(sorted(self.metrics, key=lambda item: item.name))
        if len(metrics) > MAX_BROKER_DELIVERY_METRICS_PER_CELL:
            raise ValueError("broker delivery cell has too many metrics")
        if len({item.name for item in metrics}) != len(metrics):
            raise ValueError("broker delivery cell has duplicate metrics")
        limitations = _bounded_text_tuple(
            self.limitations, "cell limitation", allow_empty=True
        )
        object.__setattr__(self, "support_count", support)
        object.__setattr__(self, "support_status", status)
        object.__setattr__(self, "backoff_condition_ids", backoffs)
        object.__setattr__(self, "effective_condition_id", effective)
        object.__setattr__(self, "metrics", metrics)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id("broker-delivery-cell", self.identity_payload())
        supplied = str(self.cell_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("cell_id does not match deterministic identity")
        object.__setattr__(self, "cell_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "condition": self.condition.to_dict(),
            "support_count": self.support_count,
            "support_status": self.support_status.value,
            "backoff_condition_ids": list(self.backoff_condition_ids),
            "effective_condition_id": self.effective_condition_id,
            "metrics": [item.to_dict() for item in self.metrics],
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "cell_id": self.cell_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "BrokerDeliveryCellV1":
        _require_schema(data, BROKER_DELIVERY_CELL_SCHEMA_VERSION)
        return cls(
            condition=BrokerDeliveryConditionV1.from_dict(
                _mapping(data.get("condition"))
            ),
            support_count=_strict_int(data.get("support_count")),
            support_status=BrokerDeliverySupportStatus.from_value(
                str(data.get("support_status", ""))
            ),
            backoff_condition_ids=_string_tuple(
                data.get("backoff_condition_ids")
            ),
            effective_condition_id=_optional_text(
                data.get("effective_condition_id")
            ),
            metrics=tuple(
                BrokerDeliveryMetricV1.from_dict(item)
                for item in _mapping_sequence(data.get("metrics"))
            ),
            limitations=_string_tuple(data.get("limitations")),
            cell_id=str(data.get("cell_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerDeliveryFingerprintV1:
    """Immutable support-aware broker delivery profile."""

    adapter_id: str
    adapter_version: str
    adapter_config_sha256: str
    protocol: str
    environment_id: str
    server_id: str
    account_id_sha256: str | None
    collector_id: str
    collector_version: str
    fit_config: BrokerDeliveryFitConfigV1
    capture_evidence: tuple[BrokerDeliveryCaptureEvidenceV1, ...]
    eligibility_decisions: tuple[BrokerCaptureEligibilityV1, ...]
    support_start_utc_ns: int
    support_end_utc_ns: int
    effective_start_utc_ns: int
    effective_end_utc_ns: int | None
    cells: tuple[BrokerDeliveryCellV1, ...]
    supersedes_fingerprint_id: str | None = None
    limitations: tuple[str, ...] = ()
    fingerprint_id: str = ""
    schema_version: str = BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION:
            raise ValueError("unsupported broker delivery fingerprint schema")
        for name in (
            "adapter_id",
            "adapter_version",
            "protocol",
            "environment_id",
            "server_id",
            "collector_id",
            "collector_version",
        ):
            object.__setattr__(self, name, _required_name(getattr(self, name)))
        object.__setattr__(
            self,
            "adapter_config_sha256",
            _required_sha256(self.adapter_config_sha256),
        )
        object.__setattr__(
            self, "account_id_sha256", _optional_sha256(self.account_id_sha256)
        )
        if not isinstance(self.fit_config, BrokerDeliveryFitConfigV1):
            raise TypeError("fit_config must be BrokerDeliveryFitConfigV1")
        evidence = tuple(
            sorted(self.capture_evidence, key=lambda item: item.session_id)
        )
        decisions = tuple(
            sorted(self.eligibility_decisions, key=lambda item: item.session_id)
        )
        if (
            not evidence
            or len(evidence) > self.fit_config.max_capture_manifests
        ):
            raise ValueError(
                "fingerprint capture evidence is empty or unbounded"
            )
        if len(evidence) != len(decisions):
            raise ValueError("capture evidence and eligibility counts differ")
        if {item.session_id for item in evidence} != {
            item.session_id for item in decisions
        }:
            raise ValueError("capture evidence and eligibility sessions differ")
        if any(not item.fit_allowed for item in decisions):
            raise ValueError("fingerprint includes an ineligible capture")
        object.__setattr__(self, "capture_evidence", evidence)
        object.__setattr__(self, "eligibility_decisions", decisions)
        start = _bounded_int(
            self.support_start_utc_ns, "support_start_utc_ns", 0, INT64_MAX
        )
        end = _bounded_int(
            self.support_end_utc_ns, "support_end_utc_ns", 0, INT64_MAX
        )
        effective_start = _bounded_int(
            self.effective_start_utc_ns, "effective_start_utc_ns", 0, INT64_MAX
        )
        effective_end = self.effective_end_utc_ns
        if end < start:
            raise ValueError("fingerprint support interval is reversed")
        if effective_end is not None:
            _bounded_int(effective_end, "effective_end_utc_ns", 0, INT64_MAX)
            if effective_end <= effective_start:
                raise ValueError("fingerprint effective interval is empty")
        cells = tuple(sorted(self.cells, key=lambda item: item.condition.key))
        if not cells or len(cells) > self.fit_config.max_cells:
            raise ValueError("fingerprint cells are empty or unbounded")
        if len({item.condition.condition_id for item in cells}) != len(cells):
            raise ValueError("fingerprint has duplicate conditions")
        global_cells = [item for item in cells if not item.condition.dimensions]
        if len(global_cells) != 1:
            raise ValueError("fingerprint requires exactly one global cell")
        cell_ids = {item.condition.condition_id for item in cells}
        for cell in cells:
            if any(
                parent not in cell_ids for parent in cell.backoff_condition_ids
            ):
                raise ValueError(
                    "fingerprint cell references an absent backoff cell"
                )
        supersedes = _optional_text(self.supersedes_fingerprint_id)
        limitations = _bounded_text_tuple(
            self.limitations, "fingerprint limitation", allow_empty=True
        )
        object.__setattr__(self, "support_start_utc_ns", start)
        object.__setattr__(self, "support_end_utc_ns", end)
        object.__setattr__(self, "effective_start_utc_ns", effective_start)
        object.__setattr__(self, "effective_end_utc_ns", effective_end)
        object.__setattr__(self, "cells", cells)
        object.__setattr__(self, "supersedes_fingerprint_id", supersedes)
        object.__setattr__(self, "limitations", limitations)
        expected = _stable_id(
            "broker-delivery-fingerprint", self.identity_payload()
        )
        supplied = str(self.fingerprint_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "fingerprint_id does not match deterministic identity"
            )
        if supersedes == expected:
            raise ValueError("fingerprint cannot supersede itself")
        object.__setattr__(self, "fingerprint_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "adapter_config_sha256": self.adapter_config_sha256,
            "protocol": self.protocol,
            "environment_id": self.environment_id,
            "server_id": self.server_id,
            "account_id_sha256": self.account_id_sha256,
            "collector_id": self.collector_id,
            "collector_version": self.collector_version,
            "fit_config": self.fit_config.to_dict(),
            "capture_evidence": [
                item.to_dict() for item in self.capture_evidence
            ],
            "eligibility_decisions": [
                item.to_dict() for item in self.eligibility_decisions
            ],
            "support_start_utc_ns": self.support_start_utc_ns,
            "support_end_utc_ns": self.support_end_utc_ns,
            "effective_start_utc_ns": self.effective_start_utc_ns,
            "effective_end_utc_ns": self.effective_end_utc_ns,
            "cells": [item.to_dict() for item in self.cells],
            "supersedes_fingerprint_id": self.supersedes_fingerprint_id,
            "limitations": list(self.limitations),
            "profile_claim": "broker_observation_delivery_system_only",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "fingerprint_id": self.fingerprint_id,
        }

    def to_json(self) -> str:
        return str(canonical_capture_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerDeliveryFingerprintV1":
        _require_schema(data, BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION)
        fingerprint = cls(
            adapter_id=str(data.get("adapter_id", "")),
            adapter_version=str(data.get("adapter_version", "")),
            adapter_config_sha256=str(data.get("adapter_config_sha256", "")),
            protocol=str(data.get("protocol", "")),
            environment_id=str(data.get("environment_id", "")),
            server_id=str(data.get("server_id", "")),
            account_id_sha256=_optional_text(data.get("account_id_sha256")),
            collector_id=str(data.get("collector_id", "")),
            collector_version=str(data.get("collector_version", "")),
            fit_config=BrokerDeliveryFitConfigV1.from_dict(
                _mapping(data.get("fit_config"))
            ),
            capture_evidence=tuple(
                BrokerDeliveryCaptureEvidenceV1.from_dict(item)
                for item in _mapping_sequence(data.get("capture_evidence"))
            ),
            eligibility_decisions=tuple(
                BrokerCaptureEligibilityV1.from_dict(item)
                for item in _mapping_sequence(data.get("eligibility_decisions"))
            ),
            support_start_utc_ns=_strict_int(data.get("support_start_utc_ns")),
            support_end_utc_ns=_strict_int(data.get("support_end_utc_ns")),
            effective_start_utc_ns=_strict_int(
                data.get("effective_start_utc_ns")
            ),
            effective_end_utc_ns=_optional_int(
                data.get("effective_end_utc_ns")
            ),
            cells=tuple(
                BrokerDeliveryCellV1.from_dict(item)
                for item in _mapping_sequence(data.get("cells"))
            ),
            supersedes_fingerprint_id=_optional_text(
                data.get("supersedes_fingerprint_id")
            ),
            limitations=_string_tuple(data.get("limitations")),
            fingerprint_id=str(data.get("fingerprint_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        claim = str(data.get("profile_claim", ""))
        if claim and claim != "broker_observation_delivery_system_only":
            raise ValueError("unsupported broker delivery profile claim")
        return fingerprint

    @classmethod
    def from_json(cls, text: str) -> "BrokerDeliveryFingerprintV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class BrokerDeliveryDriftConfigV1:
    """Bounded statistical-effect policy for stratified profile comparison."""

    min_metric_support: int = 4
    relative_material_threshold: float = 0.25
    default_absolute_material_threshold: float = 0.0
    absolute_material_thresholds: dict[str, float] = field(
        default_factory=lambda: {
            "burst_interval_rate": 0.05,
            "exact_duplicate_rate": 0.05,
            "price_decimal_places": 0.5,
            "quiet_interval_rate": 0.05,
            "source_timestamp_precision_ns": 1.0,
            "spread": 0.00001,
            "stale_quote_rate": 0.05,
            "transition_rate": 0.05,
        }
    )
    max_comparisons: int = 2_048
    rounding_digits: int = 9
    config_id: str = ""
    schema_version: str = BROKER_DELIVERY_DRIFT_CONFIG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != BROKER_DELIVERY_DRIFT_CONFIG_SCHEMA_VERSION:
            raise ValueError("unsupported broker delivery drift config schema")
        _bounded_int(
            self.min_metric_support,
            "min_metric_support",
            1,
            MAX_BROKER_DELIVERY_EVENTS,
        )
        relative = _finite_float(self.relative_material_threshold)
        default_absolute = _finite_float(
            self.default_absolute_material_threshold
        )
        if relative < 0 or default_absolute < 0:
            raise ValueError("drift thresholds must be non-negative")
        thresholds = {
            _required_name(str(name)): _finite_float(value)
            for name, value in sorted(self.absolute_material_thresholds.items())
        }
        if any(value < 0 for value in thresholds.values()):
            raise ValueError("absolute drift thresholds must be non-negative")
        _bounded_int(
            self.max_comparisons,
            "max_comparisons",
            1,
            MAX_BROKER_DELIVERY_COMPARISONS,
        )
        if not 0 <= self.rounding_digits <= 16:
            raise ValueError("rounding_digits must be between zero and sixteen")
        object.__setattr__(self, "relative_material_threshold", relative)
        object.__setattr__(
            self, "default_absolute_material_threshold", default_absolute
        )
        object.__setattr__(self, "absolute_material_thresholds", thresholds)
        expected = _stable_id(
            "broker-delivery-drift-config", self.identity_payload()
        )
        supplied = str(self.config_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError("config_id does not match deterministic identity")
        object.__setattr__(self, "config_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "min_metric_support": self.min_metric_support,
            "relative_material_threshold": self.relative_material_threshold,
            "default_absolute_material_threshold": (
                self.default_absolute_material_threshold
            ),
            "absolute_material_thresholds": dict(
                self.absolute_material_thresholds
            ),
            "max_comparisons": self.max_comparisons,
            "rounding_digits": self.rounding_digits,
            "comparison_policy": "cell_and_metric_stratified_no_similarity_score",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "config_id": self.config_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerDeliveryDriftConfigV1":
        _require_schema(data, BROKER_DELIVERY_DRIFT_CONFIG_SCHEMA_VERSION)
        return cls(
            min_metric_support=_strict_int(data.get("min_metric_support")),
            relative_material_threshold=_finite_float(
                data.get("relative_material_threshold")
            ),
            default_absolute_material_threshold=_finite_float(
                data.get("default_absolute_material_threshold")
            ),
            absolute_material_thresholds={
                str(name): _finite_float(value)
                for name, value in _mapping(
                    data.get("absolute_material_thresholds")
                ).items()
            },
            max_comparisons=_strict_int(data.get("max_comparisons")),
            rounding_digits=_strict_int(data.get("rounding_digits")),
            config_id=str(data.get("config_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerDeliveryMetricComparisonV1:
    """One support-aware metric comparison in one explicit condition."""

    condition_id: str
    condition_key: str
    metric_name: str
    reference_support_count: int
    candidate_support_count: int
    reference_estimate: float | None
    candidate_estimate: float | None
    absolute_difference: float | None
    relative_difference: float | None
    combined_uncertainty: float | None
    status: BrokerDeliveryDriftStatus
    reason_codes: tuple[str, ...]
    comparison_id: str = ""
    schema_version: str = BROKER_DELIVERY_METRIC_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BROKER_DELIVERY_METRIC_COMPARISON_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported broker delivery metric comparison schema"
            )
        for name in ("condition_id", "condition_key", "metric_name"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        for name in ("reference_support_count", "candidate_support_count"):
            _bounded_int(
                getattr(self, name), name, 0, MAX_BROKER_DELIVERY_EVENTS
            )
        for name in (
            "reference_estimate",
            "candidate_estimate",
            "absolute_difference",
            "relative_difference",
            "combined_uncertainty",
        ):
            object.__setattr__(
                self, name, _optional_finite_float(getattr(self, name))
            )
        status = BrokerDeliveryDriftStatus.from_value(self.status)
        reasons = _bounded_text_tuple(
            self.reason_codes, "comparison reason", allow_empty=True
        )
        if status is not BrokerDeliveryDriftStatus.STABLE and not reasons:
            raise ValueError("non-stable comparison requires a reason")
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "reason_codes", reasons)
        expected = _stable_id(
            "broker-delivery-metric-comparison", self.identity_payload()
        )
        supplied = str(self.comparison_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "comparison_id does not match deterministic identity"
            )
        object.__setattr__(self, "comparison_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "condition_id": self.condition_id,
            "condition_key": self.condition_key,
            "metric_name": self.metric_name,
            "reference_support_count": self.reference_support_count,
            "candidate_support_count": self.candidate_support_count,
            "reference_estimate": self.reference_estimate,
            "candidate_estimate": self.candidate_estimate,
            "absolute_difference": self.absolute_difference,
            "relative_difference": self.relative_difference,
            "combined_uncertainty": self.combined_uncertainty,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "comparison_id": self.comparison_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerDeliveryMetricComparisonV1":
        _require_schema(data, BROKER_DELIVERY_METRIC_COMPARISON_SCHEMA_VERSION)
        return cls(
            condition_id=str(data.get("condition_id", "")),
            condition_key=str(data.get("condition_key", "")),
            metric_name=str(data.get("metric_name", "")),
            reference_support_count=_strict_int(
                data.get("reference_support_count")
            ),
            candidate_support_count=_strict_int(
                data.get("candidate_support_count")
            ),
            reference_estimate=_optional_float(data.get("reference_estimate")),
            candidate_estimate=_optional_float(data.get("candidate_estimate")),
            absolute_difference=_optional_float(
                data.get("absolute_difference")
            ),
            relative_difference=_optional_float(
                data.get("relative_difference")
            ),
            combined_uncertainty=_optional_float(
                data.get("combined_uncertainty")
            ),
            status=BrokerDeliveryDriftStatus.from_value(
                str(data.get("status", ""))
            ),
            reason_codes=_string_tuple(data.get("reason_codes")),
            comparison_id=str(data.get("comparison_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class BrokerDeliveryFingerprintComparisonV1:
    """Bounded stratified drift evidence with no aggregate similarity score."""

    reference_fingerprint_id: str
    candidate_fingerprint_id: str
    drift_config: BrokerDeliveryDriftConfigV1
    comparison_candidate_count: int
    comparisons: tuple[BrokerDeliveryMetricComparisonV1, ...]
    status_counts: dict[str, int]
    truncated: bool
    comparison_id: str = ""
    schema_version: str = BROKER_DELIVERY_FINGERPRINT_COMPARISON_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != BROKER_DELIVERY_FINGERPRINT_COMPARISON_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported broker delivery fingerprint comparison schema"
            )
        for name in ("reference_fingerprint_id", "candidate_fingerprint_id"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.reference_fingerprint_id == self.candidate_fingerprint_id:
            raise ValueError(
                "fingerprint comparison requires distinct profiles"
            )
        if not isinstance(self.drift_config, BrokerDeliveryDriftConfigV1):
            raise TypeError("drift_config must be BrokerDeliveryDriftConfigV1")
        candidate_count = _bounded_int(
            self.comparison_candidate_count,
            "comparison_candidate_count",
            0,
            MAX_BROKER_DELIVERY_COMPARISON_CANDIDATES,
        )
        comparisons = tuple(self.comparisons)
        if len(comparisons) > self.drift_config.max_comparisons:
            raise ValueError("fingerprint comparison exceeds configured bound")
        if len({item.comparison_id for item in comparisons}) != len(
            comparisons
        ):
            raise ValueError("fingerprint comparison has duplicate rows")
        counts = {
            str(name): _bounded_int(value, str(name), 0, candidate_count)
            for name, value in sorted(self.status_counts.items())
        }
        observed: dict[str, int] = {}
        for item in comparisons:
            observed[item.status.value] = observed.get(item.status.value, 0) + 1
        if counts != dict(sorted(observed.items())):
            raise ValueError(
                "fingerprint comparison status counts do not reconcile"
            )
        if self.truncated != (candidate_count > len(comparisons)):
            raise ValueError(
                "fingerprint comparison truncation flag is inconsistent"
            )
        object.__setattr__(self, "comparison_candidate_count", candidate_count)
        object.__setattr__(self, "comparisons", comparisons)
        object.__setattr__(self, "status_counts", counts)
        expected = _stable_id(
            "broker-delivery-fingerprint-comparison", self.identity_payload()
        )
        supplied = str(self.comparison_id or "").strip()
        if supplied and supplied != expected:
            raise ValueError(
                "comparison_id does not match deterministic identity"
            )
        object.__setattr__(self, "comparison_id", expected)

    @property
    def material_drift_count(self) -> int:
        return self.status_counts.get(
            BrokerDeliveryDriftStatus.MATERIAL_DRIFT.value, 0
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference_fingerprint_id": self.reference_fingerprint_id,
            "candidate_fingerprint_id": self.candidate_fingerprint_id,
            "drift_config": self.drift_config.to_dict(),
            "comparison_candidate_count": self.comparison_candidate_count,
            "comparisons": [item.to_dict() for item in self.comparisons],
            "status_counts": dict(self.status_counts),
            "truncated": self.truncated,
            "global_similarity_score": None,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "comparison_id": self.comparison_id}

    def to_json(self) -> str:
        return str(canonical_capture_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "BrokerDeliveryFingerprintComparisonV1":
        _require_schema(
            data, BROKER_DELIVERY_FINGERPRINT_COMPARISON_SCHEMA_VERSION
        )
        if data.get("global_similarity_score") is not None:
            raise ValueError("global similarity scores are not supported")
        return cls(
            reference_fingerprint_id=str(
                data.get("reference_fingerprint_id", "")
            ),
            candidate_fingerprint_id=str(
                data.get("candidate_fingerprint_id", "")
            ),
            drift_config=BrokerDeliveryDriftConfigV1.from_dict(
                _mapping(data.get("drift_config"))
            ),
            comparison_candidate_count=_strict_int(
                data.get("comparison_candidate_count")
            ),
            comparisons=tuple(
                BrokerDeliveryMetricComparisonV1.from_dict(item)
                for item in _mapping_sequence(data.get("comparisons"))
            ),
            status_counts={
                str(name): _strict_int(value)
                for name, value in _mapping(data.get("status_counts")).items()
            },
            truncated=_strict_bool(data.get("truncated")),
            comparison_id=str(data.get("comparison_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "BrokerDeliveryFingerprintComparisonV1":
        return cls.from_dict(_json_mapping(text))


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_capture_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _enum_value(
    enum_type: type[_EnumT], value: str | _EnumT, name: str
) -> _EnumT:
    if isinstance(value, enum_type):
        return value
    try:
        return enum_type(str(value).strip().lower())
    except ValueError as err:
        raise ValueError(f"unsupported {name}") from err


def _required_text(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("text value must be a string")
    text = value.strip()
    if not text or len(text) > MAX_BROKER_DELIVERY_TEXT:
        raise ValueError("text value is empty or unbounded")
    return text


def _required_name(value: object) -> str:
    text = _required_text(value)
    if not _NAME_RE.fullmatch(text):
        raise ValueError("name contains unsupported characters")
    return text


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("optional text value must be a string")
    text = value.strip()
    return _required_text(text) if text else None


def _required_sha256(value: object) -> str:
    text = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ValueError("value must be a lowercase SHA-256 digest")
    return text


def _optional_sha256(value: object) -> str | None:
    text = _optional_text(value)
    return None if text is None else _required_sha256(text)


def _bounded_int(value: object, name: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} is outside configured bounds")
    return value


def _strict_int(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("value must be an integer")
    return value


def _strict_bool(value: object) -> bool:
    if not isinstance(value, bool):
        raise TypeError("value must be a boolean")
    return value


def _optional_int(value: object) -> int | None:
    return None if value is None else _strict_int(value)


def _finite_float(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("value must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("value must be finite")
    return result


def _optional_finite_float(value: object) -> float | None:
    return None if value is None else _finite_float(value)


def _optional_float(value: object) -> float | None:
    return _optional_finite_float(value)


def _bounded_text_tuple(
    values: Sequence[object],
    name: str,
    *,
    allow_empty: bool,
) -> tuple[str, ...]:
    normalized = tuple(sorted({_required_text(value) for value in values}))
    if not allow_empty and not normalized:
        raise ValueError(f"{name} values are empty")
    if len(normalized) > MAX_BROKER_DELIVERY_REASONS:
        raise ValueError(f"{name} values are unbounded")
    return normalized


def _bounded_ordered_text_tuple(
    values: Sequence[object],
    name: str,
    *,
    allow_empty: bool,
) -> tuple[str, ...]:
    normalized = tuple(_required_text(value) for value in values)
    if not allow_empty and not normalized:
        raise ValueError(f"{name} values are empty")
    if len(normalized) > MAX_BROKER_DELIVERY_REASONS:
        raise ValueError(f"{name} values are unbounded")
    return normalized


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("value must be a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("value must be a sequence")
    return value


def _mapping_sequence(value: object) -> tuple[Mapping[str, Any], ...]:
    return tuple(_mapping(item) for item in _sequence(value))


def _string_tuple(value: object) -> tuple[str, ...]:
    values = _sequence(value)
    if any(not isinstance(item, str) for item in values):
        raise TypeError("sequence values must be strings")
    return tuple(values)


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if str(data.get("schema_version", "")) != expected:
        raise ValueError(f"unsupported schema version; expected {expected}")


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)


__all__ = [
    "BROKER_CAPTURE_ELIGIBILITY_SCHEMA_VERSION",
    "BROKER_DELIVERY_CAPTURE_EVIDENCE_SCHEMA_VERSION",
    "BROKER_DELIVERY_CELL_SCHEMA_VERSION",
    "BROKER_DELIVERY_CONDITION_SCHEMA_VERSION",
    "BROKER_DELIVERY_DRIFT_CONFIG_SCHEMA_VERSION",
    "BROKER_DELIVERY_FINGERPRINT_ARTIFACT_KIND",
    "BROKER_DELIVERY_FINGERPRINT_COMPARISON_SCHEMA_VERSION",
    "BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION",
    "BROKER_DELIVERY_FIT_CONFIG_SCHEMA_VERSION",
    "BROKER_DELIVERY_METRIC_COMPARISON_SCHEMA_VERSION",
    "BROKER_DELIVERY_METRIC_SCHEMA_VERSION",
    "BrokerCaptureEligibilityStatus",
    "BrokerCaptureEligibilityV1",
    "BrokerDeliveryCaptureEvidenceV1",
    "BrokerDeliveryCellV1",
    "BrokerDeliveryConditionV1",
    "BrokerDeliveryDriftConfigV1",
    "BrokerDeliveryDriftStatus",
    "BrokerDeliveryFingerprintComparisonV1",
    "BrokerDeliveryFingerprintV1",
    "BrokerDeliveryFitConfigV1",
    "BrokerDeliveryMetricComparisonV1",
    "BrokerDeliveryMetricV1",
    "BrokerDeliverySupportStatus",
]
