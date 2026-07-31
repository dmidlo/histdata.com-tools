"""Point-in-time synchronized cross-series evidence constraints.

The contracts in this module are provider-neutral.  The only executable v1
adapter compiles HistData.com ASCII/T source events and delegates descriptive
relationship statistics to the established ``fingerprint.cross_series``
surface.  Event identity is never reduced to timestamp: duplicate timestamps
remain distinct in alignment accounting and unsafe forward filling is never
performed.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from bisect import bisect_right
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, cast

from histdatacom.data_quality.symbols import (
    CROSS_SERIES_FINGERPRINT_METADATA_KEY,
    CrossInstrumentPointInput,
    CrossInstrumentSeriesInput,
    HistDataCrossInstrumentTolerance,
    HistDataCrossSeriesFingerprintRule,
)
from histdatacom.reconstruction_evidence import (
    CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
    ReconstructionEvidenceInformationMode,
)
from histdatacom.runtime_contracts import JSONScalar, JSONValue

CROSS_SERIES_CONSTRAINT_POLICY_SCHEMA_VERSION = (
    "histdatacom.cross-series-constraint-policy.v1"
)
CROSS_SERIES_SOURCE_BINDING_SCHEMA_VERSION = (
    "histdatacom.cross-series-source-binding.v1"
)
CROSS_SERIES_MEMBER_EVIDENCE_SCHEMA_VERSION = (
    "histdatacom.cross-series-member-evidence.v1"
)
CROSS_SERIES_ALIGNMENT_SUPPORT_SCHEMA_VERSION = (
    "histdatacom.cross-series-alignment-support.v1"
)
CROSS_SERIES_RESIDUAL_SUMMARY_SCHEMA_VERSION = (
    "histdatacom.cross-series-residual-summary.v1"
)
CROSS_SERIES_CONSTRAINT_WINDOW_SCHEMA_VERSION = (
    "histdatacom.cross-series-constraint-window.v1"
)
CROSS_SERIES_CONSTRAINT_BUNDLE_SCHEMA_VERSION = (
    "histdatacom.cross-series-constraint-bundle.v1"
)
CROSS_SERIES_CONSTRAINT_USE_SCHEMA_VERSION = (
    "histdatacom.cross-series-constraint-use.v1"
)

CROSS_SERIES_CONSTRAINT_POLICY_ARTIFACT_KIND = (
    "cross_series_constraint_policy_v1"
)
CROSS_SERIES_CONSTRAINT_BUNDLE_ARTIFACT_KIND = (
    "cross_series_constraint_bundle_v1"
)

CURRENT_HISTDATA_TRIANGLE = ("eurgbp", "eurusd", "gbpusd")
DEFAULT_NEAREST_PRIOR_MAX_AGE_NS = 5_000_000_000
DEFAULT_MAX_STALENESS_NS = 30_000_000_000
MAX_CROSS_SERIES_WINDOWS = 256
MAX_CROSS_SERIES_MEMBERS = 96
MAX_CROSS_SERIES_ALIGNMENT_SAMPLES = 64
MAX_CROSS_SERIES_LIMITATIONS = 64
MAX_CROSS_SERIES_EFFECTS = 32
MAX_CROSS_SERIES_TEXT = 1024

_PERIOD_RE = re.compile(r"^\d{6}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class CrossSeriesRelationKind(str, Enum):
    """Relationship represented by one synchronized constraint window."""

    TRIANGLE = "triangle"
    INVERSE = "inverse"
    TIMESTAMP_GRID = "timestamp_grid"
    RANGE_OVERLAP = "range_overlap"
    STALE_ALIGNMENT = "stale_alignment"


class CrossSeriesAlignmentPolicy(str, Enum):
    """Explicit bounded event alignment semantics."""

    EXACT_EVENT_SEQUENCE = "exact_event_sequence"
    NEAREST_PRIOR_BOUNDED = "nearest_prior_bounded"
    INTERVAL_OVERLAP = "interval_overlap"
    UNAVAILABLE = "unavailable"


class CrossSeriesConstraintStatus(str, Enum):
    """Readiness of one relation or complete compiled bundle."""

    READY = "ready"
    LIMITED = "limited"
    EXCLUDED = "excluded"
    CONTRADICTORY = "contradictory"
    INSUFFICIENT = "insufficient"
    UNAVAILABLE = "unavailable"


class CrossSeriesConstraintUseStatus(str, Enum):
    """How a first-party stage handled synchronized evidence."""

    APPLIED = "applied"
    REFUSED = "refused"
    NOT_APPLICABLE = "not_applicable"


_ALL_USE_SCOPES = (
    "normal_training",
    "anomaly_label",
    "proposal",
    "carving",
    "reconciliation",
    "validation",
)


@dataclass(frozen=True, slots=True)
class CrossSeriesConstraintPolicyV1:
    """Versioned alignment, tolerance, refusal, and boundedness policy."""

    supported_provider_ids: tuple[str, ...] = (
        CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
    )
    required_symbols: tuple[str, ...] = CURRENT_HISTDATA_TRIANGLE
    nearest_prior_max_age_ns: int = DEFAULT_NEAREST_PRIOR_MAX_AGE_NS
    max_staleness_ns: int = DEFAULT_MAX_STALENESS_NS
    minimum_alignment_support: int = 2
    triangular_warning_relative_tolerance: float = 0.005
    triangular_error_relative_tolerance: float = 0.05
    inverse_warning_relative_tolerance: float = 0.005
    inverse_error_relative_tolerance: float = 0.05
    minimum_common_timestamp_ratio: float = 0.5
    stale_forward_fill_min_run: int = 2
    maximum_contradiction_ratio: float = 0.25
    fail_closed_on_contradiction: bool = True
    fail_closed_on_incomplete_group: bool = True
    max_windows: int = MAX_CROSS_SERIES_WINDOWS
    max_alignment_samples: int = MAX_CROSS_SERIES_ALIGNMENT_SAMPLES
    policy_id: str = ""
    schema_version: str = CROSS_SERIES_CONSTRAINT_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_CONSTRAINT_POLICY_SCHEMA_VERSION,
            "cross-series constraint policy",
        )
        providers = _text_tuple(
            self.supported_provider_ids,
            "supported_provider_ids",
            maximum=16,
            lowercase=True,
        )
        symbols = _symbols(self.required_symbols)
        if not providers or not symbols:
            raise ValueError(
                "cross-series policy requires provider and symbols"
            )
        object.__setattr__(self, "supported_provider_ids", providers)
        object.__setattr__(self, "required_symbols", symbols)
        for name in (
            "nearest_prior_max_age_ns",
            "max_staleness_ns",
            "minimum_alignment_support",
            "stale_forward_fill_min_run",
            "max_windows",
            "max_alignment_samples",
        ):
            object.__setattr__(
                self, name, _positive_int(getattr(self, name), name)
            )
        if self.nearest_prior_max_age_ns > self.max_staleness_ns:
            raise ValueError("nearest-prior age exceeds maximum staleness")
        if self.max_windows > MAX_CROSS_SERIES_WINDOWS:
            raise ValueError("cross-series window bound exceeds v1 maximum")
        if self.max_windows < len(CrossSeriesRelationKind):
            raise ValueError(
                "cross-series window bound cannot hold one relation group"
            )
        if self.max_alignment_samples > MAX_CROSS_SERIES_ALIGNMENT_SAMPLES:
            raise ValueError("cross-series sample bound exceeds v1 maximum")
        for warning_name, error_name in (
            (
                "triangular_warning_relative_tolerance",
                "triangular_error_relative_tolerance",
            ),
            (
                "inverse_warning_relative_tolerance",
                "inverse_error_relative_tolerance",
            ),
        ):
            warning = _nonnegative_float(
                getattr(self, warning_name), warning_name
            )
            error = _nonnegative_float(getattr(self, error_name), error_name)
            if error < warning:
                raise ValueError(f"{error_name} must be >= {warning_name}")
            object.__setattr__(self, warning_name, warning)
            object.__setattr__(self, error_name, error)
        ratio = _unit_float(
            self.minimum_common_timestamp_ratio,
            "minimum_common_timestamp_ratio",
        )
        contradiction = _unit_float(
            self.maximum_contradiction_ratio,
            "maximum_contradiction_ratio",
        )
        object.__setattr__(self, "minimum_common_timestamp_ratio", ratio)
        object.__setattr__(self, "maximum_contradiction_ratio", contradiction)
        for name in (
            "fail_closed_on_contradiction",
            "fail_closed_on_incomplete_group",
        ):
            if type(getattr(self, name)) is not bool:
                raise TypeError(f"{name} must be boolean")
            if not getattr(self, name):
                raise ValueError(f"{name} must remain enabled in v1")
        expected = _stable_id("cross-series-constraint-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("cross-series policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def tolerance(self) -> HistDataCrossInstrumentTolerance:
        """Return the exact #331 tolerance surface bound by this policy."""
        return HistDataCrossInstrumentTolerance(
            triangular_warning_relative_tolerance=(
                self.triangular_warning_relative_tolerance
            ),
            triangular_error_relative_tolerance=(
                self.triangular_error_relative_tolerance
            ),
            inverse_warning_relative_tolerance=(
                self.inverse_warning_relative_tolerance
            ),
            inverse_error_relative_tolerance=(
                self.inverse_error_relative_tolerance
            ),
            minimum_common_timestamp_ratio=self.minimum_common_timestamp_ratio,
            stale_forward_fill_min_run=self.stale_forward_fill_min_run,
        )

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "supported_provider_ids": list(self.supported_provider_ids),
            "required_symbols": list(self.required_symbols),
            "nearest_prior_max_age_ns": self.nearest_prior_max_age_ns,
            "max_staleness_ns": self.max_staleness_ns,
            "minimum_alignment_support": self.minimum_alignment_support,
            "triangular_warning_relative_tolerance": (
                self.triangular_warning_relative_tolerance
            ),
            "triangular_error_relative_tolerance": (
                self.triangular_error_relative_tolerance
            ),
            "inverse_warning_relative_tolerance": (
                self.inverse_warning_relative_tolerance
            ),
            "inverse_error_relative_tolerance": (
                self.inverse_error_relative_tolerance
            ),
            "minimum_common_timestamp_ratio": (
                self.minimum_common_timestamp_ratio
            ),
            "stale_forward_fill_min_run": self.stale_forward_fill_min_run,
            "maximum_contradiction_ratio": self.maximum_contradiction_ratio,
            "fail_closed_on_contradiction": self.fail_closed_on_contradiction,
            "fail_closed_on_incomplete_group": (
                self.fail_closed_on_incomplete_group
            ),
            "max_windows": self.max_windows,
            "max_alignment_samples": self.max_alignment_samples,
            "provider_neutral_contract": True,
            "current_adapter": "histdata-ascii-tick-only",
            "timestamp_only_join_forbidden": True,
            "unbounded_forward_fill_forbidden": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CrossSeriesConstraintPolicyV1:
        _require_schema(data, CROSS_SERIES_CONSTRAINT_POLICY_SCHEMA_VERSION)
        _require_derived(data, "provider_neutral_contract", True)
        _require_derived(data, "current_adapter", "histdata-ascii-tick-only")
        _require_derived(data, "timestamp_only_join_forbidden", True)
        _require_derived(data, "unbounded_forward_fill_forbidden", True)
        return cls(
            supported_provider_ids=_string_tuple(
                data.get("supported_provider_ids")
            ),
            required_symbols=_string_tuple(data.get("required_symbols")),
            nearest_prior_max_age_ns=_strict_int(
                data.get("nearest_prior_max_age_ns"), "nearest_prior_max_age_ns"
            ),
            max_staleness_ns=_strict_int(
                data.get("max_staleness_ns"), "max_staleness_ns"
            ),
            minimum_alignment_support=_strict_int(
                data.get("minimum_alignment_support"),
                "minimum_alignment_support",
            ),
            triangular_warning_relative_tolerance=_finite_float(
                data.get("triangular_warning_relative_tolerance"),
                "triangular_warning_relative_tolerance",
            ),
            triangular_error_relative_tolerance=_finite_float(
                data.get("triangular_error_relative_tolerance"),
                "triangular_error_relative_tolerance",
            ),
            inverse_warning_relative_tolerance=_finite_float(
                data.get("inverse_warning_relative_tolerance"),
                "inverse_warning_relative_tolerance",
            ),
            inverse_error_relative_tolerance=_finite_float(
                data.get("inverse_error_relative_tolerance"),
                "inverse_error_relative_tolerance",
            ),
            minimum_common_timestamp_ratio=_finite_float(
                data.get("minimum_common_timestamp_ratio"),
                "minimum_common_timestamp_ratio",
            ),
            stale_forward_fill_min_run=_strict_int(
                data.get("stale_forward_fill_min_run"),
                "stale_forward_fill_min_run",
            ),
            maximum_contradiction_ratio=_finite_float(
                data.get("maximum_contradiction_ratio"),
                "maximum_contradiction_ratio",
            ),
            fail_closed_on_contradiction=_strict_bool(
                data.get("fail_closed_on_contradiction"),
                "fail_closed_on_contradiction",
            ),
            fail_closed_on_incomplete_group=_strict_bool(
                data.get("fail_closed_on_incomplete_group"),
                "fail_closed_on_incomplete_group",
            ),
            max_windows=_strict_int(data.get("max_windows"), "max_windows"),
            max_alignment_samples=_strict_int(
                data.get("max_alignment_samples"), "max_alignment_samples"
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> CrossSeriesConstraintPolicyV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class CrossSeriesSourceBindingV1:
    """Strong provider/dataset/series/partition binding for one source leg."""

    provider_id: str
    dataset_version_id: str
    symbol: str
    period: str
    series_id: str
    source_partition_id: str
    source_artifact_id: str
    source_artifact_sha256: str
    binding_id: str = ""
    schema_version: str = CROSS_SERIES_SOURCE_BINDING_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_SOURCE_BINDING_SCHEMA_VERSION,
            "cross-series source binding",
        )
        for name in (
            "provider_id",
            "dataset_version_id",
            "series_id",
            "source_partition_id",
            "source_artifact_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(self, "provider_id", self.provider_id.lower())
        object.__setattr__(self, "symbol", _symbol(self.symbol))
        object.__setattr__(self, "period", _period(self.period))
        object.__setattr__(
            self,
            "source_artifact_sha256",
            _sha256(self.source_artifact_sha256, "source_artifact_sha256"),
        )
        expected = _stable_id("cross-series-source-binding", self.payload())
        if self.binding_id and self.binding_id != expected:
            raise ValueError("cross-series source binding_id differs")
        object.__setattr__(self, "binding_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "provider_id": self.provider_id,
            "dataset_version_id": self.dataset_version_id,
            "symbol": self.symbol,
            "period": self.period,
            "series_id": self.series_id,
            "source_partition_id": self.source_partition_id,
            "source_artifact_id": self.source_artifact_id,
            "source_artifact_sha256": self.source_artifact_sha256,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "binding_id": self.binding_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CrossSeriesSourceBindingV1:
        _require_schema(data, CROSS_SERIES_SOURCE_BINDING_SCHEMA_VERSION)
        return cls(
            provider_id=str(data.get("provider_id", "")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            series_id=str(data.get("series_id", "")),
            source_partition_id=str(data.get("source_partition_id", "")),
            source_artifact_id=str(data.get("source_artifact_id", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            binding_id=str(data.get("binding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossSeriesMemberEvidenceV1:
    """Immutable coverage identity for one symbol/period member."""

    binding: CrossSeriesSourceBindingV1
    event_count: int
    unique_timestamp_count: int
    duplicate_timestamp_event_count: int
    coverage_start_ns: int
    coverage_end_ns: int
    event_identity_content_sha256: str
    event_quote_content_sha256: str
    member_id: str = ""
    schema_version: str = CROSS_SERIES_MEMBER_EVIDENCE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_MEMBER_EVIDENCE_SCHEMA_VERSION,
            "cross-series member evidence",
        )
        if not isinstance(self.binding, CrossSeriesSourceBindingV1):
            raise TypeError("member evidence requires a source binding")
        for name in (
            "event_count",
            "unique_timestamp_count",
            "duplicate_timestamp_event_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if not self.event_count or not self.unique_timestamp_count:
            raise ValueError("member evidence requires events")
        if self.unique_timestamp_count > self.event_count:
            raise ValueError("unique timestamps exceed event count")
        if self.duplicate_timestamp_event_count != (
            self.event_count - self.unique_timestamp_count
        ):
            raise ValueError("duplicate timestamp accounting differs")
        start = _int64(self.coverage_start_ns, "coverage_start_ns")
        end = _int64(self.coverage_end_ns, "coverage_end_ns")
        if end <= start:
            raise ValueError("member coverage is empty")
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        for name in (
            "event_identity_content_sha256",
            "event_quote_content_sha256",
        ):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        expected = _stable_id("cross-series-member-evidence", self.payload())
        if self.member_id and self.member_id != expected:
            raise ValueError("cross-series member_id differs")
        object.__setattr__(self, "member_id", expected)

    @property
    def symbol(self) -> str:
        return self.binding.symbol

    @property
    def period(self) -> str:
        return self.binding.period

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "binding": self.binding.to_dict(),
            "event_count": self.event_count,
            "unique_timestamp_count": self.unique_timestamp_count,
            "duplicate_timestamp_event_count": (
                self.duplicate_timestamp_event_count
            ),
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "event_identity_content_sha256": (
                self.event_identity_content_sha256
            ),
            "event_quote_content_sha256": self.event_quote_content_sha256,
            "timestamp_is_durable_identity": False,
            "event_identity_preserved": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "member_id": self.member_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CrossSeriesMemberEvidenceV1:
        _require_schema(data, CROSS_SERIES_MEMBER_EVIDENCE_SCHEMA_VERSION)
        _require_derived(data, "timestamp_is_durable_identity", False)
        _require_derived(data, "event_identity_preserved", True)
        return cls(
            binding=CrossSeriesSourceBindingV1.from_dict(
                _mapping(data.get("binding"))
            ),
            event_count=_strict_int(data.get("event_count"), "event_count"),
            unique_timestamp_count=_strict_int(
                data.get("unique_timestamp_count"), "unique_timestamp_count"
            ),
            duplicate_timestamp_event_count=_strict_int(
                data.get("duplicate_timestamp_event_count"),
                "duplicate_timestamp_event_count",
            ),
            coverage_start_ns=_strict_int(
                data.get("coverage_start_ns"), "coverage_start_ns"
            ),
            coverage_end_ns=_strict_int(
                data.get("coverage_end_ns"), "coverage_end_ns"
            ),
            event_identity_content_sha256=str(
                data.get("event_identity_content_sha256", "")
            ),
            event_quote_content_sha256=str(
                data.get("event_quote_content_sha256", "")
            ),
            member_id=str(data.get("member_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossSeriesAlignmentSupportV1:
    """Bounded identity-aware alignment accounting without inline rows."""

    policy: CrossSeriesAlignmentPolicy
    support_count: int
    probe_count: int
    unmatched_event_count_by_symbol: Mapping[str, int]
    stale_support_count: int
    maximum_observed_age_ns: int
    p95_observed_age_ns: int
    configured_tolerance_ns: int
    configured_max_age_ns: int
    support_start_ns: int | None
    support_end_ns: int | None
    recommended_event_time_ns: int | None
    alignment_content_sha256: str
    sample_alignment_ids: tuple[str, ...] = ()
    schema_version: str = CROSS_SERIES_ALIGNMENT_SUPPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_ALIGNMENT_SUPPORT_SCHEMA_VERSION,
            "cross-series alignment support",
        )
        object.__setattr__(
            self, "policy", CrossSeriesAlignmentPolicy(self.policy)
        )
        for name in (
            "support_count",
            "probe_count",
            "stale_support_count",
            "maximum_observed_age_ns",
            "p95_observed_age_ns",
            "configured_tolerance_ns",
            "configured_max_age_ns",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.support_count > self.probe_count:
            raise ValueError("alignment support exceeds probes")
        if self.stale_support_count > self.support_count:
            raise ValueError("stale alignment exceeds support")
        unmatched = {
            _symbol(name): _nonnegative_int(value, f"unmatched.{name}")
            for name, value in self.unmatched_event_count_by_symbol.items()
        }
        object.__setattr__(
            self,
            "unmatched_event_count_by_symbol",
            dict(sorted(unmatched.items())),
        )
        start = self.support_start_ns
        end = self.support_end_ns
        if (start is None) != (end is None):
            raise ValueError("alignment support bounds must both be present")
        if start is not None and end is not None:
            start = _int64(start, "support_start_ns")
            end = _int64(end, "support_end_ns")
            if end <= start:
                raise ValueError("alignment support bounds are empty")
            object.__setattr__(self, "support_start_ns", start)
            object.__setattr__(self, "support_end_ns", end)
        if self.support_count and start is None:
            raise ValueError("supported alignment lacks support bounds")
        if not self.support_count and start is not None:
            raise ValueError("unsupported alignment cannot have support bounds")
        if self.support_count and self.recommended_event_time_ns is None:
            raise ValueError(
                "supported alignment lacks a recommended event time"
            )
        if (
            not self.support_count
            and self.recommended_event_time_ns is not None
        ):
            raise ValueError(
                "unsupported alignment cannot recommend an event time"
            )
        if self.recommended_event_time_ns is not None:
            recommended = _int64(
                self.recommended_event_time_ns, "recommended_event_time_ns"
            )
            if start is None or end is None or not start <= recommended < end:
                raise ValueError(
                    "recommended event time lies outside alignment support"
                )
            object.__setattr__(
                self,
                "recommended_event_time_ns",
                recommended,
            )
        object.__setattr__(
            self,
            "alignment_content_sha256",
            _sha256(self.alignment_content_sha256, "alignment_content_sha256"),
        )
        samples = _text_tuple(
            self.sample_alignment_ids,
            "sample_alignment_ids",
            maximum=MAX_CROSS_SERIES_ALIGNMENT_SAMPLES,
        )
        object.__setattr__(self, "sample_alignment_ids", samples)
        if (
            self.policy is CrossSeriesAlignmentPolicy.UNAVAILABLE
            and self.support_count
        ):
            raise ValueError("unavailable alignment cannot have support")

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "policy": self.policy.value,
            "support_count": self.support_count,
            "probe_count": self.probe_count,
            "unmatched_event_count_by_symbol": dict(
                self.unmatched_event_count_by_symbol
            ),
            "stale_support_count": self.stale_support_count,
            "maximum_observed_age_ns": self.maximum_observed_age_ns,
            "p95_observed_age_ns": self.p95_observed_age_ns,
            "configured_tolerance_ns": self.configured_tolerance_ns,
            "configured_max_age_ns": self.configured_max_age_ns,
            "support_start_ns": self.support_start_ns,
            "support_end_ns": self.support_end_ns,
            "recommended_event_time_ns": self.recommended_event_time_ns,
            "alignment_content_sha256": self.alignment_content_sha256,
            "sample_alignment_ids": list(self.sample_alignment_ids),
            "timestamp_only_join": False,
            "forward_fill": False,
            "duplicate_timestamp_identity_preserved": True,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CrossSeriesAlignmentSupportV1:
        _require_schema(data, CROSS_SERIES_ALIGNMENT_SUPPORT_SCHEMA_VERSION)
        _require_derived(data, "timestamp_only_join", False)
        _require_derived(data, "forward_fill", False)
        _require_derived(data, "duplicate_timestamp_identity_preserved", True)
        support_start = data.get("support_start_ns")
        support_end = data.get("support_end_ns")
        recommended = data.get("recommended_event_time_ns")
        return cls(
            policy=CrossSeriesAlignmentPolicy(str(data.get("policy", ""))),
            support_count=_strict_int(
                data.get("support_count"), "support_count"
            ),
            probe_count=_strict_int(data.get("probe_count"), "probe_count"),
            unmatched_event_count_by_symbol={
                str(name): _strict_int(value, f"unmatched.{name}")
                for name, value in _mapping(
                    data.get("unmatched_event_count_by_symbol")
                ).items()
            },
            stale_support_count=_strict_int(
                data.get("stale_support_count"), "stale_support_count"
            ),
            maximum_observed_age_ns=_strict_int(
                data.get("maximum_observed_age_ns"), "maximum_observed_age_ns"
            ),
            p95_observed_age_ns=_strict_int(
                data.get("p95_observed_age_ns"), "p95_observed_age_ns"
            ),
            configured_tolerance_ns=_strict_int(
                data.get("configured_tolerance_ns"), "configured_tolerance_ns"
            ),
            configured_max_age_ns=_strict_int(
                data.get("configured_max_age_ns"), "configured_max_age_ns"
            ),
            support_start_ns=(
                _strict_int(support_start, "support_start_ns")
                if support_start is not None
                else None
            ),
            support_end_ns=(
                _strict_int(support_end, "support_end_ns")
                if support_end is not None
                else None
            ),
            recommended_event_time_ns=(
                _strict_int(recommended, "recommended_event_time_ns")
                if recommended is not None
                else None
            ),
            alignment_content_sha256=str(
                data.get("alignment_content_sha256", "")
            ),
            sample_alignment_ids=_string_tuple(
                data.get("sample_alignment_ids")
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossSeriesResidualSummaryV1:
    """Bounded #331 relative-disagreement distribution and thresholds."""

    compared_count: int
    warning_count: int
    error_count: int
    minimum: float | None
    mean: float | None
    p50_upper_bound: float | None
    p95_upper_bound: float | None
    p99_upper_bound: float | None
    maximum: float | None
    distribution_content_sha256: str
    calculation_basis: str = "fingerprint.cross_series"
    schema_version: str = CROSS_SERIES_RESIDUAL_SUMMARY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_RESIDUAL_SUMMARY_SCHEMA_VERSION,
            "cross-series residual summary",
        )
        for name in ("compared_count", "warning_count", "error_count"):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.warning_count + self.error_count > self.compared_count:
            raise ValueError("residual severity counts exceed comparisons")
        for name in (
            "minimum",
            "mean",
            "p50_upper_bound",
            "p95_upper_bound",
            "p99_upper_bound",
            "maximum",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _nonnegative_float(value, name))
        object.__setattr__(
            self,
            "distribution_content_sha256",
            _sha256(
                self.distribution_content_sha256,
                "distribution_content_sha256",
            ),
        )
        object.__setattr__(
            self,
            "calculation_basis",
            _required_text(self.calculation_basis, "calculation_basis"),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "compared_count": self.compared_count,
            "warning_count": self.warning_count,
            "error_count": self.error_count,
            "minimum": self.minimum,
            "mean": self.mean,
            "p50_upper_bound": self.p50_upper_bound,
            "p95_upper_bound": self.p95_upper_bound,
            "p99_upper_bound": self.p99_upper_bound,
            "maximum": self.maximum,
            "distribution_content_sha256": self.distribution_content_sha256,
            "calculation_basis": self.calculation_basis,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CrossSeriesResidualSummaryV1:
        _require_schema(data, CROSS_SERIES_RESIDUAL_SUMMARY_SCHEMA_VERSION)
        return cls(
            compared_count=_strict_int(
                data.get("compared_count"), "compared_count"
            ),
            warning_count=_strict_int(
                data.get("warning_count"), "warning_count"
            ),
            error_count=_strict_int(data.get("error_count"), "error_count"),
            minimum=_optional_float(data.get("minimum"), "minimum"),
            mean=_optional_float(data.get("mean"), "mean"),
            p50_upper_bound=_optional_float(
                data.get("p50_upper_bound"), "p50_upper_bound"
            ),
            p95_upper_bound=_optional_float(
                data.get("p95_upper_bound"), "p95_upper_bound"
            ),
            p99_upper_bound=_optional_float(
                data.get("p99_upper_bound"), "p99_upper_bound"
            ),
            maximum=_optional_float(data.get("maximum"), "maximum"),
            distribution_content_sha256=str(
                data.get("distribution_content_sha256", "")
            ),
            calculation_basis=str(data.get("calculation_basis", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossSeriesConstraintWindowV1:
    """One bounded point-in-time relationship constraint."""

    synchronization_unit_id: str
    evidence_window_id: str
    period: str
    relation_kind: CrossSeriesRelationKind
    relationship_id: str
    symbols: tuple[str, ...]
    member_ids: tuple[str, ...]
    limiting_symbols: tuple[str, ...]
    support_start_ns: int
    support_end_ns: int
    available_at_ns: int
    as_of_ns: int
    information_mode: ReconstructionEvidenceInformationMode
    alignment: CrossSeriesAlignmentSupportV1
    residual_summary: CrossSeriesResidualSummaryV1 | None
    status: CrossSeriesConstraintStatus
    usable_scopes: tuple[str, ...]
    excluded_scopes: tuple[str, ...]
    limitations: tuple[str, ...]
    source_fingerprint_schema_version: str
    source_fingerprint_content_sha256: str
    constraint_window_id: str = ""
    schema_version: str = CROSS_SERIES_CONSTRAINT_WINDOW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_CONSTRAINT_WINDOW_SCHEMA_VERSION,
            "cross-series constraint window",
        )
        for name in (
            "synchronization_unit_id",
            "evidence_window_id",
            "relationship_id",
            "source_fingerprint_schema_version",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(self, "period", _period(self.period))
        object.__setattr__(
            self, "relation_kind", CrossSeriesRelationKind(self.relation_kind)
        )
        symbols = _symbols(self.symbols)
        members = _text_tuple(self.member_ids, "member_ids", maximum=32)
        if not members:
            raise ValueError("constraint window requires member evidence")
        limiting = _symbols(self.limiting_symbols)
        if not set(limiting).issubset(symbols):
            raise ValueError("constraint limiting symbols are outside scope")
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "member_ids", members)
        object.__setattr__(self, "limiting_symbols", limiting)
        expected_relationship = _stable_id(
            "cross-series-relationship",
            {"kind": self.relation_kind.value, "symbols": list(symbols)},
        )
        if self.relationship_id != expected_relationship:
            raise ValueError("cross-series relationship_id differs")
        start = _int64(self.support_start_ns, "support_start_ns")
        end = _int64(self.support_end_ns, "support_end_ns")
        if end <= start:
            raise ValueError("cross-series constraint support is empty")
        object.__setattr__(self, "support_start_ns", start)
        object.__setattr__(self, "support_end_ns", end)
        for name in ("available_at_ns", "as_of_ns"):
            object.__setattr__(self, name, _int64(getattr(self, name), name))
        object.__setattr__(
            self,
            "information_mode",
            ReconstructionEvidenceInformationMode.from_value(
                self.information_mode
            ),
        )
        if not isinstance(self.alignment, CrossSeriesAlignmentSupportV1):
            raise TypeError("constraint window requires alignment support")
        if self.alignment.support_count and (
            self.alignment.support_start_ns != start
            or self.alignment.support_end_ns != end
        ):
            raise ValueError("constraint and alignment support bounds differ")
        if self.residual_summary is not None and not isinstance(
            self.residual_summary, CrossSeriesResidualSummaryV1
        ):
            raise TypeError("constraint residual summary has wrong type")
        object.__setattr__(
            self, "status", CrossSeriesConstraintStatus(self.status)
        )
        usable = _use_scopes(self.usable_scopes)
        excluded = _use_scopes(self.excluded_scopes)
        if set(usable).intersection(excluded):
            raise ValueError("constraint usable and excluded scopes overlap")
        object.__setattr__(self, "usable_scopes", usable)
        object.__setattr__(self, "excluded_scopes", excluded)
        limitations = _text_tuple(
            self.limitations,
            "limitations",
            maximum=MAX_CROSS_SERIES_LIMITATIONS,
        )
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(
            self,
            "source_fingerprint_content_sha256",
            _sha256(
                self.source_fingerprint_content_sha256,
                "source_fingerprint_content_sha256",
            ),
        )
        expected = _stable_id("cross-series-constraint-window", self.payload())
        if self.constraint_window_id and self.constraint_window_id != expected:
            raise ValueError("cross-series constraint_window_id differs")
        object.__setattr__(self, "constraint_window_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "synchronization_unit_id": self.synchronization_unit_id,
            "evidence_window_id": self.evidence_window_id,
            "period": self.period,
            "relation_kind": self.relation_kind.value,
            "relationship_id": self.relationship_id,
            "symbols": list(self.symbols),
            "member_ids": list(self.member_ids),
            "limiting_symbols": list(self.limiting_symbols),
            "support_start_ns": self.support_start_ns,
            "support_end_ns": self.support_end_ns,
            "available_at_ns": self.available_at_ns,
            "as_of_ns": self.as_of_ns,
            "information_mode": self.information_mode.value,
            "alignment": self.alignment.to_dict(),
            "residual_summary": (
                self.residual_summary.to_dict()
                if self.residual_summary is not None
                else None
            ),
            "status": self.status.value,
            "usable_scopes": list(self.usable_scopes),
            "excluded_scopes": list(self.excluded_scopes),
            "limitations": list(self.limitations),
            "source_fingerprint_schema_version": (
                self.source_fingerprint_schema_version
            ),
            "source_fingerprint_content_sha256": (
                self.source_fingerprint_content_sha256
            ),
            "observed_anchors_mutable": False,
            "timestamp_only_join": False,
            "unbounded_forward_fill": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.payload(),
            "constraint_window_id": self.constraint_window_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CrossSeriesConstraintWindowV1:
        _require_schema(data, CROSS_SERIES_CONSTRAINT_WINDOW_SCHEMA_VERSION)
        _require_derived(data, "observed_anchors_mutable", False)
        _require_derived(data, "timestamp_only_join", False)
        _require_derived(data, "unbounded_forward_fill", False)
        residual = data.get("residual_summary")
        return cls(
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            evidence_window_id=str(data.get("evidence_window_id", "")),
            period=str(data.get("period", "")),
            relation_kind=CrossSeriesRelationKind(
                str(data.get("relation_kind", ""))
            ),
            relationship_id=str(data.get("relationship_id", "")),
            symbols=_string_tuple(data.get("symbols")),
            member_ids=_string_tuple(data.get("member_ids")),
            limiting_symbols=_string_tuple(data.get("limiting_symbols")),
            support_start_ns=_strict_int(
                data.get("support_start_ns"), "support_start_ns"
            ),
            support_end_ns=_strict_int(
                data.get("support_end_ns"), "support_end_ns"
            ),
            available_at_ns=_strict_int(
                data.get("available_at_ns"), "available_at_ns"
            ),
            as_of_ns=_strict_int(data.get("as_of_ns"), "as_of_ns"),
            information_mode=ReconstructionEvidenceInformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            alignment=CrossSeriesAlignmentSupportV1.from_dict(
                _mapping(data.get("alignment"))
            ),
            residual_summary=(
                CrossSeriesResidualSummaryV1.from_dict(_mapping(residual))
                if residual is not None
                else None
            ),
            status=CrossSeriesConstraintStatus(str(data.get("status", ""))),
            usable_scopes=_string_tuple(data.get("usable_scopes")),
            excluded_scopes=_string_tuple(data.get("excluded_scopes")),
            limitations=_string_tuple(data.get("limitations")),
            source_fingerprint_schema_version=str(
                data.get("source_fingerprint_schema_version", "")
            ),
            source_fingerprint_content_sha256=str(
                data.get("source_fingerprint_content_sha256", "")
            ),
            constraint_window_id=str(data.get("constraint_window_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CrossSeriesConstraintBundleV1:
    """Complete bounded constraint evidence for one synchronization unit."""

    synchronization_unit_id: str
    evidence_window_id: str
    source_provider_id: str
    dataset_version_ids: tuple[str, ...]
    symbols: tuple[str, ...]
    support_start_ns: int
    support_end_ns: int
    available_at_ns: int
    as_of_ns: int
    information_mode: ReconstructionEvidenceInformationMode
    policy_id: str
    members: tuple[CrossSeriesMemberEvidenceV1, ...]
    windows: tuple[CrossSeriesConstraintWindowV1, ...]
    status: CrossSeriesConstraintStatus
    limitations: tuple[str, ...] = ()
    omitted_window_count: int = 0
    bundle_id: str = ""
    schema_version: str = CROSS_SERIES_CONSTRAINT_BUNDLE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_CONSTRAINT_BUNDLE_SCHEMA_VERSION,
            "cross-series constraint bundle",
        )
        for name in (
            "synchronization_unit_id",
            "evidence_window_id",
            "source_provider_id",
            "policy_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self, "source_provider_id", self.source_provider_id.lower()
        )
        datasets = _text_tuple(
            self.dataset_version_ids, "dataset_version_ids", maximum=32
        )
        symbols = _symbols(self.symbols)
        if not datasets or not symbols:
            raise ValueError("constraint bundle requires dataset and symbols")
        object.__setattr__(self, "dataset_version_ids", datasets)
        object.__setattr__(self, "symbols", symbols)
        start = _int64(self.support_start_ns, "support_start_ns")
        end = _int64(self.support_end_ns, "support_end_ns")
        if end <= start:
            raise ValueError("constraint bundle support is empty")
        object.__setattr__(self, "support_start_ns", start)
        object.__setattr__(self, "support_end_ns", end)
        for name in ("available_at_ns", "as_of_ns"):
            object.__setattr__(self, name, _int64(getattr(self, name), name))
        object.__setattr__(
            self,
            "information_mode",
            ReconstructionEvidenceInformationMode.from_value(
                self.information_mode
            ),
        )
        members = tuple(sorted(self.members, key=lambda item: item.member_id))
        windows = tuple(
            sorted(self.windows, key=lambda item: item.constraint_window_id)
        )
        if len(members) > MAX_CROSS_SERIES_MEMBERS:
            raise ValueError("constraint bundle member count is unbounded")
        if len(windows) > MAX_CROSS_SERIES_WINDOWS:
            raise ValueError("constraint bundle window count is unbounded")
        if len({item.member_id for item in members}) != len(members):
            raise ValueError("constraint bundle has duplicate members")
        if len({item.constraint_window_id for item in windows}) != len(windows):
            raise ValueError("constraint bundle has duplicate windows")
        for member in members:
            if member.binding.provider_id != self.source_provider_id:
                raise ValueError("constraint member provider differs")
            if member.binding.dataset_version_id not in datasets:
                raise ValueError("constraint member dataset differs")
            if member.symbol not in symbols:
                raise ValueError("constraint member symbol differs")
        member_ids = {item.member_id for item in members}
        member_by_id = {item.member_id: item for item in members}
        for window in windows:
            if window.synchronization_unit_id != self.synchronization_unit_id:
                raise ValueError(
                    "constraint window synchronization unit differs"
                )
            if window.evidence_window_id != self.evidence_window_id:
                raise ValueError("constraint window evidence ID differs")
            if not set(window.member_ids).issubset(member_ids):
                raise ValueError("constraint window refers to unknown member")
            if not set(window.symbols).issubset(symbols):
                raise ValueError("constraint window symbols exceed bundle")
            if any(
                member_by_id[member_id].period != window.period
                for member_id in window.member_ids
            ):
                raise ValueError("constraint window member period differs")
            if window.support_start_ns < start or window.support_end_ns > end:
                raise ValueError("constraint window exceeds bundle support")
            if (
                window.available_at_ns != self.available_at_ns
                or window.as_of_ns != self.as_of_ns
                or window.information_mode is not self.information_mode
            ):
                raise ValueError(
                    "constraint window information boundary differs"
                )
        object.__setattr__(self, "members", members)
        object.__setattr__(self, "windows", windows)
        selected_status = CrossSeriesConstraintStatus(self.status)
        expected_status = _bundle_status(windows, symbols, members)
        if selected_status is not expected_status:
            raise ValueError("cross-series constraint bundle status differs")
        object.__setattr__(self, "status", selected_status)
        object.__setattr__(
            self,
            "limitations",
            _text_tuple(
                self.limitations,
                "limitations",
                maximum=MAX_CROSS_SERIES_LIMITATIONS,
            ),
        )
        object.__setattr__(
            self,
            "omitted_window_count",
            _nonnegative_int(self.omitted_window_count, "omitted_window_count"),
        )
        expected = _stable_id("cross-series-constraint-bundle", self.payload())
        if self.bundle_id and self.bundle_id != expected:
            raise ValueError("cross-series constraint bundle_id differs")
        object.__setattr__(self, "bundle_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "synchronization_unit_id": self.synchronization_unit_id,
            "evidence_window_id": self.evidence_window_id,
            "source_provider_id": self.source_provider_id,
            "dataset_version_ids": list(self.dataset_version_ids),
            "symbols": list(self.symbols),
            "support_start_ns": self.support_start_ns,
            "support_end_ns": self.support_end_ns,
            "available_at_ns": self.available_at_ns,
            "as_of_ns": self.as_of_ns,
            "information_mode": self.information_mode.value,
            "policy_id": self.policy_id,
            "members": [item.to_dict() for item in self.members],
            "windows": [item.to_dict() for item in self.windows],
            "status": self.status.value,
            "limitations": list(self.limitations),
            "omitted_window_count": self.omitted_window_count,
            "bounded_sidecar": True,
            "full_tick_rows_embedded": False,
            "observed_anchors_mutable": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "bundle_id": self.bundle_id}

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> CrossSeriesConstraintBundleV1:
        _require_schema(data, CROSS_SERIES_CONSTRAINT_BUNDLE_SCHEMA_VERSION)
        _require_derived(data, "bounded_sidecar", True)
        _require_derived(data, "full_tick_rows_embedded", False)
        _require_derived(data, "observed_anchors_mutable", False)
        return cls(
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            evidence_window_id=str(data.get("evidence_window_id", "")),
            source_provider_id=str(data.get("source_provider_id", "")),
            dataset_version_ids=_string_tuple(data.get("dataset_version_ids")),
            symbols=_string_tuple(data.get("symbols")),
            support_start_ns=_strict_int(
                data.get("support_start_ns"), "support_start_ns"
            ),
            support_end_ns=_strict_int(
                data.get("support_end_ns"), "support_end_ns"
            ),
            available_at_ns=_strict_int(
                data.get("available_at_ns"), "available_at_ns"
            ),
            as_of_ns=_strict_int(data.get("as_of_ns"), "as_of_ns"),
            information_mode=ReconstructionEvidenceInformationMode.from_value(
                str(data.get("information_mode", ""))
            ),
            policy_id=str(data.get("policy_id", "")),
            members=tuple(
                CrossSeriesMemberEvidenceV1.from_dict(_mapping(item))
                for item in _sequence(data.get("members"))
            ),
            windows=tuple(
                CrossSeriesConstraintWindowV1.from_dict(_mapping(item))
                for item in _sequence(data.get("windows"))
            ),
            status=CrossSeriesConstraintStatus(str(data.get("status", ""))),
            limitations=_string_tuple(data.get("limitations")),
            omitted_window_count=_strict_int(
                data.get("omitted_window_count", 0), "omitted_window_count"
            ),
            bundle_id=str(data.get("bundle_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> CrossSeriesConstraintBundleV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class CrossSeriesConstraintUseV1:
    """One explicit stage use/refusal of synchronized constraint windows."""

    stage: str
    status: CrossSeriesConstraintUseStatus
    bundle_ids: tuple[str, ...]
    consumed_window_ids: tuple[str, ...]
    used_at_ns: int
    reason: str
    effects: Mapping[str, JSONScalar] = field(default_factory=dict)
    decision_id: str = ""
    schema_version: str = CROSS_SERIES_CONSTRAINT_USE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            CROSS_SERIES_CONSTRAINT_USE_SCHEMA_VERSION,
            "cross-series constraint use",
        )
        object.__setattr__(self, "stage", _required_text(self.stage, "stage"))
        object.__setattr__(
            self, "status", CrossSeriesConstraintUseStatus(self.status)
        )
        bundles = _text_tuple(self.bundle_ids, "bundle_ids", maximum=64)
        windows = _text_tuple(
            self.consumed_window_ids,
            "consumed_window_ids",
            maximum=MAX_CROSS_SERIES_WINDOWS,
        )
        if not bundles:
            raise ValueError("cross-series use requires a bundle")
        object.__setattr__(self, "bundle_ids", bundles)
        object.__setattr__(self, "consumed_window_ids", windows)
        object.__setattr__(
            self, "used_at_ns", _int64(self.used_at_ns, "used_at_ns")
        )
        object.__setattr__(
            self, "reason", _required_text(self.reason, "reason")
        )
        if len(self.effects) > MAX_CROSS_SERIES_EFFECTS:
            raise ValueError("cross-series use effects exceed v1 bound")
        effects = {
            _required_text(str(name), "effect name"): _json_scalar(value)
            for name, value in sorted(self.effects.items())
        }
        object.__setattr__(self, "effects", effects)
        expected = _stable_id("cross-series-constraint-use", self.payload())
        if self.decision_id and self.decision_id != expected:
            raise ValueError("cross-series constraint decision_id differs")
        object.__setattr__(self, "decision_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "stage": self.stage,
            "status": self.status.value,
            "bundle_ids": list(self.bundle_ids),
            "consumed_window_ids": list(self.consumed_window_ids),
            "used_at_ns": self.used_at_ns,
            "reason": self.reason,
            "effects": dict(self.effects),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CrossSeriesConstraintUseV1:
        _require_schema(data, CROSS_SERIES_CONSTRAINT_USE_SCHEMA_VERSION)
        return cls(
            stage=str(data.get("stage", "")),
            status=CrossSeriesConstraintUseStatus(str(data.get("status", ""))),
            bundle_ids=_string_tuple(data.get("bundle_ids")),
            consumed_window_ids=_string_tuple(data.get("consumed_window_ids")),
            used_at_ns=_strict_int(data.get("used_at_ns"), "used_at_ns"),
            reason=str(data.get("reason", "")),
            effects={
                str(name): _json_scalar(value)
                for name, value in _mapping(data.get("effects")).items()
            },
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def compile_histdata_cross_series_constraints(
    events_by_symbol: Mapping[str, Sequence[Any]],
    *,
    source_bindings: Sequence[CrossSeriesSourceBindingV1],
    synchronization_unit_id: str,
    evidence_window_id: str,
    dataset_version_ids: Sequence[str],
    support_start_ns: int,
    support_end_ns: int,
    available_at_ns: int,
    as_of_ns: int,
    information_mode: object,
    policy: CrossSeriesConstraintPolicyV1 | None = None,
) -> CrossSeriesConstraintBundleV1:
    """Compile one HistData-only bundle from immutable observed source events."""
    selected = policy or CrossSeriesConstraintPolicyV1()
    provider = CURRENT_EVIDENCE_SOURCE_PROVIDER_ID
    if selected.supported_provider_ids != (provider,):
        raise ValueError(
            "current cross-series compiler supports only HistData.com"
        )
    if selected.required_symbols != CURRENT_HISTDATA_TRIANGLE:
        raise ValueError(
            "current cross-series compiler requires the HistData triangle"
        )
    mode = ReconstructionEvidenceInformationMode.from_value(information_mode)
    start = _int64(support_start_ns, "support_start_ns")
    end = _int64(support_end_ns, "support_end_ns")
    available = _int64(available_at_ns, "available_at_ns")
    as_of = _int64(as_of_ns, "as_of_ns")
    if end <= start:
        raise ValueError("cross-series compiler support is empty")
    bindings = tuple(source_bindings)
    if any(
        not isinstance(item, CrossSeriesSourceBindingV1) for item in bindings
    ):
        raise TypeError("cross-series compiler requires source bindings")
    binding_by_key = {
        (item.symbol, item.period, item.series_id): item for item in bindings
    }
    if len(binding_by_key) != len(bindings):
        raise ValueError("cross-series source bindings are duplicated")
    if any(item.provider_id != provider for item in bindings):
        raise ValueError("current cross-series adapter accepts HistData only")
    dataset_ids = _text_tuple(
        tuple(dataset_version_ids), "dataset_version_ids", maximum=32
    )
    if not dataset_ids:
        raise ValueError("cross-series compiler requires dataset versions")
    if any(item.dataset_version_id not in dataset_ids for item in bindings):
        raise ValueError("source binding dataset is absent from bundle scope")
    if any(item.symbol not in selected.required_symbols for item in bindings):
        raise ValueError("source binding symbol is outside policy scope")

    normalized_events: dict[str, tuple[Any, ...]] = {}
    future_withheld = False
    for raw_symbol, raw_events in events_by_symbol.items():
        symbol = _symbol(raw_symbol)
        if symbol in normalized_events:
            raise ValueError("cross-series event symbols normalize ambiguously")
        if symbol not in selected.required_symbols:
            raise ValueError(
                "cross-series event symbol is outside policy scope"
            )
        raw_event_tuple = tuple(raw_events)
        if any(_event_symbol(item) != symbol for item in raw_event_tuple):
            raise ValueError("cross-series event symbol differs from its leg")
        if any(_event_origin(item) != "observed" for item in raw_event_tuple):
            raise ValueError(
                "cross-series compiler accepts observed events only"
            )
        selected_events = tuple(sorted(raw_event_tuple, key=_event_order_key))
        if any(
            _event_time_ns(item) < start or _event_time_ns(item) >= end
            for item in selected_events
        ):
            raise ValueError("cross-series source event lies outside support")
        visible = tuple(
            item
            for item in selected_events
            if mode
            is ReconstructionEvidenceInformationMode.EX_POST_RECONSTRUCTION
            or _event_time_ns(item) <= as_of
        )
        future_withheld = future_withheld or len(visible) != len(
            selected_events
        )
        normalized_events[symbol] = visible

    periods = tuple(
        sorted(
            {
                _event_period(item)
                for events in normalized_events.values()
                for item in events
            }
        )
    )
    members: list[CrossSeriesMemberEvidenceV1] = []
    windows: list[CrossSeriesConstraintWindowV1] = []
    bundle_limitations: list[str] = []
    if future_withheld:
        bundle_limitations.append(
            "future_cross_series_events_withheld_in_ex_ante_mode"
        )
    effective_available = available

    for period in periods:
        period_events = {
            symbol: tuple(
                item for item in events if _event_period(item) == period
            )
            for symbol, events in normalized_events.items()
        }
        period_events = {
            symbol: events for symbol, events in period_events.items() if events
        }
        period_members: list[CrossSeriesMemberEvidenceV1] = []
        for symbol, events in sorted(period_events.items()):
            series_ids = {_event_series_id(item) for item in events}
            if len(series_ids) != 1:
                raise ValueError(
                    "symbol/period events span multiple series IDs"
                )
            series_id = next(iter(series_ids))
            binding = binding_by_key.get((symbol, period, series_id))
            if binding is None:
                raise ValueError(
                    "cross-series source event lacks strong binding"
                )
            member = _member_evidence(binding, events)
            period_members.append(member)
            members.append(member)
        compiled = _compile_period_windows(
            period_events,
            members=period_members,
            policy=selected,
            synchronization_unit_id=synchronization_unit_id,
            evidence_window_id=evidence_window_id,
            period=period,
            support_start_ns=start,
            support_end_ns=end,
            available_at_ns=effective_available,
            as_of_ns=as_of,
            information_mode=mode,
        )
        windows.extend(compiled)

    relation_count = len(CrossSeriesRelationKind)
    retained_capacity = selected.max_windows - (
        selected.max_windows % relation_count
    )
    omitted = max(0, len(windows) - retained_capacity)
    retained_windows = tuple(windows[:retained_capacity])
    if omitted:
        bundle_limitations.append("constraint_windows_truncated_by_policy")
    if not retained_windows:
        bundle_limitations.append("no_cross_series_window_evidence")
    status = _bundle_status(
        retained_windows, selected.required_symbols, members
    )
    if status is CrossSeriesConstraintStatus.UNAVAILABLE:
        bundle_limitations.append("required_cross_series_evidence_unavailable")
    return CrossSeriesConstraintBundleV1(
        synchronization_unit_id=synchronization_unit_id,
        evidence_window_id=evidence_window_id,
        source_provider_id=provider,
        dataset_version_ids=dataset_ids,
        symbols=selected.required_symbols,
        support_start_ns=start,
        support_end_ns=end,
        available_at_ns=effective_available,
        as_of_ns=as_of,
        information_mode=mode,
        policy_id=selected.policy_id,
        members=tuple(members),
        windows=retained_windows,
        status=status,
        limitations=tuple(bundle_limitations),
        omitted_window_count=omitted,
    )


def cross_series_constraint_use(
    bundles: Sequence[CrossSeriesConstraintBundleV1],
    *,
    stage: str,
    used_at_ns: int,
    policy: CrossSeriesConstraintPolicyV1 | None = None,
) -> CrossSeriesConstraintUseV1:
    """Resolve explicit stage eligibility without hiding anomalous evidence."""
    if not bundles:
        raise ValueError("cross-series constraint use requires bundles")
    selected = policy or CrossSeriesConstraintPolicyV1()
    if any(item.policy_id != selected.policy_id for item in bundles):
        raise ValueError("cross-series bundles differ from supplied policy")
    used_at = _int64(used_at_ns, "used_at_ns")
    scope = _stage_scope(stage)
    visible = tuple(
        window
        for bundle in bundles
        for window in bundle.windows
        if window.available_at_ns <= used_at and window.as_of_ns <= used_at
    )
    relevant = tuple(
        item
        for item in visible
        if scope in item.usable_scopes or scope in item.excluded_scopes
    )
    critical = tuple(
        item
        for item in relevant
        if item.relation_kind
        in {
            CrossSeriesRelationKind.TRIANGLE,
            CrossSeriesRelationKind.RANGE_OVERLAP,
        }
        and item.status
        in {
            CrossSeriesConstraintStatus.CONTRADICTORY,
            CrossSeriesConstraintStatus.INSUFFICIENT,
            CrossSeriesConstraintStatus.UNAVAILABLE,
        }
    )
    contradiction_count = sum(
        item.status is CrossSeriesConstraintStatus.CONTRADICTORY
        for item in relevant
    )
    limited_count = sum(
        item.status is CrossSeriesConstraintStatus.LIMITED for item in relevant
    )
    has_relevant = bool(relevant)
    eligible = has_relevant and not bool(critical)
    effects: dict[str, JSONScalar] = {
        "normal_training_eligible": eligible,
        "anomaly_label_eligible": bool(critical) or limited_count > 0,
        "proposal_eligible": eligible,
        "carving_eligible": eligible,
        "reconciliation_eligible": eligible,
        "validation_eligible": eligible,
        "synchronized_proposal_required": True,
        "exact_event_time_reconciliation_required": True,
        "max_staleness_ns": selected.max_staleness_ns,
        "contradictory_window_count": contradiction_count,
        "limited_window_count": limited_count,
    }
    nonblocking_evidence_scope = scope in {"normal_training", "anomaly_label"}
    production_scope = scope in {
        "proposal",
        "carving",
        "reconciliation",
        "validation",
    }
    if bool(critical) and not nonblocking_evidence_scope:
        status = CrossSeriesConstraintUseStatus.REFUSED
        reason = (
            "cross-series evidence refused unsupported or contradictory use"
        )
    elif not relevant and production_scope:
        status = CrossSeriesConstraintUseStatus.REFUSED
        reason = "cross-series evidence is not point-in-time available"
    elif not relevant:
        status = CrossSeriesConstraintUseStatus.NOT_APPLICABLE
        reason = "no cross-series constraint was available for this stage"
    else:
        status = CrossSeriesConstraintUseStatus.APPLIED
        reason = (
            "cross-series anomaly retained but excluded from normal training"
            if critical and scope == "normal_training"
            else "available synchronized constraints conditioned the stage"
        )
    return CrossSeriesConstraintUseV1(
        stage=stage,
        status=status,
        bundle_ids=tuple(item.bundle_id for item in bundles),
        consumed_window_ids=tuple(
            item.constraint_window_id for item in relevant
        ),
        used_at_ns=used_at,
        reason=reason,
        effects=effects,
    )


def require_constraint_support_for_synchronization_time(
    bundles: Sequence[CrossSeriesConstraintBundleV1],
    event_time_ns: int,
) -> None:
    """Require a proposal synchronization time inside usable triangle support."""
    timestamp = _int64(event_time_ns, "event_time_ns")
    supported = any(
        window.relation_kind is CrossSeriesRelationKind.TRIANGLE
        and window.status
        in {
            CrossSeriesConstraintStatus.READY,
            CrossSeriesConstraintStatus.LIMITED,
        }
        and window.alignment.recommended_event_time_ns == timestamp
        and window.support_start_ns <= timestamp < window.support_end_ns
        for bundle in bundles
        for window in bundle.windows
    )
    if not supported:
        raise ValueError(
            "proposal synchronization time lacks constraint support"
        )


def select_constraint_synchronization_time(
    bundles: Sequence[CrossSeriesConstraintBundleV1],
    *,
    start_ns: int,
    end_ns: int,
) -> tuple[int, str]:
    """Select one explicitly supported triangle instant and its window ID."""
    start = _int64(start_ns, "start_ns")
    end = _int64(end_ns, "end_ns")
    if end <= start:
        raise ValueError("synchronization selection interval is empty")
    center_twice = start + end
    candidates: list[tuple[tuple[int, int, int, str], int, str]] = []
    for bundle in bundles:
        for window in bundle.windows:
            recommended = window.alignment.recommended_event_time_ns
            if (
                window.relation_kind is not CrossSeriesRelationKind.TRIANGLE
                or window.status
                not in {
                    CrossSeriesConstraintStatus.READY,
                    CrossSeriesConstraintStatus.LIMITED,
                }
                or recommended is None
                or not start <= recommended < end
                or not window.support_start_ns
                <= recommended
                < window.support_end_ns
            ):
                continue
            score = (
                (
                    0
                    if window.alignment.policy
                    is CrossSeriesAlignmentPolicy.EXACT_EVENT_SEQUENCE
                    else 1
                ),
                0 if window.status is CrossSeriesConstraintStatus.READY else 1,
                abs(2 * recommended - center_twice),
                window.constraint_window_id,
            )
            candidates.append((score, recommended, window.constraint_window_id))
    if not candidates:
        raise ValueError(
            "no supported cross-series synchronization instant lies in core"
        )
    _, selected_time, window_id = min(candidates)
    require_constraint_support_for_synchronization_time(bundles, selected_time)
    return selected_time, window_id


def read_cross_series_constraint_policy(
    path: str,
) -> CrossSeriesConstraintPolicyV1:
    with open(path, encoding="utf-8") as stream:
        return CrossSeriesConstraintPolicyV1.from_json(stream.read())


def read_cross_series_constraint_bundle(
    path: str,
) -> CrossSeriesConstraintBundleV1:
    with open(path, encoding="utf-8") as stream:
        return CrossSeriesConstraintBundleV1.from_json(stream.read())


def _compile_period_windows(
    events_by_symbol: Mapping[str, Sequence[Any]],
    *,
    members: Sequence[CrossSeriesMemberEvidenceV1],
    policy: CrossSeriesConstraintPolicyV1,
    synchronization_unit_id: str,
    evidence_window_id: str,
    period: str,
    support_start_ns: int,
    support_end_ns: int,
    available_at_ns: int,
    as_of_ns: int,
    information_mode: ReconstructionEvidenceInformationMode,
) -> tuple[CrossSeriesConstraintWindowV1, ...]:
    series_inputs = tuple(
        _fingerprint_series_input(symbol, period, events)
        for symbol, events in sorted(events_by_symbol.items())
    )
    fingerprint = HistDataCrossSeriesFingerprintRule(
        tolerance=policy.tolerance()
    ).evaluate_series(series_inputs)
    payload = _json_value_mapping(
        fingerprint.metadata.get(CROSS_SERIES_FINGERPRINT_METADATA_KEY)
    )
    fingerprint_sha = _content_sha256(payload)
    fingerprint_version = str(
        payload.get("schema_version")
        or "histdatacom.cross-series-fingerprint.v1"
    )
    group = _fingerprint_group(payload, period)
    triangle = _json_value_mapping(payload.get("triangular_consistency"))
    inverse = _json_value_mapping(payload.get("inverse_consistency"))
    stale = _json_value_mapping(payload.get("stale_join_risk"))
    grid = _json_value_mapping(group.get("timestamp_grid"))
    coverage = _json_value_mapping(group.get("coverage_ranges"))
    exact = _exact_alignment(events_by_symbol, policy)
    nearest = _nearest_prior_alignment(events_by_symbol, policy)
    overlap = _interval_overlap_alignment(events_by_symbol)
    residual = _residual_summary(triangle)
    member_ids = tuple(item.member_id for item in members)
    symbols = tuple(sorted(events_by_symbol)) or policy.required_symbols
    limiting_symbols = _limiting_symbols(coverage, events_by_symbol)
    complete = set(events_by_symbol) == set(policy.required_symbols)
    evidence_start_ns = max(
        support_start_ns,
        min(item.coverage_start_ns for item in members),
    )
    evidence_end_ns = min(
        support_end_ns,
        max(item.coverage_end_ns for item in members),
    )
    if evidence_end_ns <= evidence_start_ns:
        raise ValueError("period evidence lies outside bundle support")
    common = {
        "synchronization_unit_id": synchronization_unit_id,
        "evidence_window_id": evidence_window_id,
        "period": period,
        "symbols": symbols,
        "member_ids": member_ids,
        "limiting_symbols": limiting_symbols,
        "available_at_ns": available_at_ns,
        "as_of_ns": as_of_ns,
        "information_mode": information_mode,
        "source_fingerprint_schema_version": fingerprint_version,
        "source_fingerprint_content_sha256": fingerprint_sha,
    }

    def relation_common(
        alignment: CrossSeriesAlignmentSupportV1,
    ) -> dict[str, Any]:
        return {
            **common,
            "support_start_ns": (
                alignment.support_start_ns
                if alignment.support_start_ns is not None
                else evidence_start_ns
            ),
            "support_end_ns": (
                alignment.support_end_ns
                if alignment.support_end_ns is not None
                else evidence_end_ns
            ),
        }

    triangle_status, triangle_limitations = _triangle_status(
        triangle,
        complete=complete,
        exact=exact,
        nearest=nearest,
        policy=policy,
    )
    triangle_alignment = (
        exact
        if exact.support_count >= policy.minimum_alignment_support
        else nearest
    )
    triangle_usable, triangle_excluded = _scopes_for_status(triangle_status)
    common_ratio = _optional_numeric(grid.get("common_timestamp_ratio"))
    grid_status = (
        CrossSeriesConstraintStatus.INSUFFICIENT
        if not complete or not exact.support_count
        else (
            CrossSeriesConstraintStatus.LIMITED
            if common_ratio is None
            or common_ratio < policy.minimum_common_timestamp_ratio
            else CrossSeriesConstraintStatus.READY
        )
    )
    unequal_ranges = coverage.get("unequal_ranges") is True
    overlap_status = (
        CrossSeriesConstraintStatus.INSUFFICIENT
        if not complete or not overlap.support_count
        else (
            CrossSeriesConstraintStatus.LIMITED
            if unequal_ranges
            else CrossSeriesConstraintStatus.READY
        )
    )
    stale_count = _json_int(stale.get("risk_count"))
    stale_status = (
        CrossSeriesConstraintStatus.INSUFFICIENT
        if not complete or not nearest.support_count
        else (
            CrossSeriesConstraintStatus.LIMITED
            if stale_count or nearest.stale_support_count
            else CrossSeriesConstraintStatus.READY
        )
    )
    inverse_candidates = _json_int(inverse.get("candidate_count"))
    inverse_status = (
        CrossSeriesConstraintStatus.UNAVAILABLE
        if not inverse_candidates
        else _consistency_status(inverse)
    )
    inverse_residual = _residual_summary(inverse)
    return (
        CrossSeriesConstraintWindowV1(
            relation_kind=CrossSeriesRelationKind.TRIANGLE,
            relationship_id=_stable_id(
                "cross-series-relationship",
                {"kind": "triangle", "symbols": list(symbols)},
            ),
            alignment=triangle_alignment,
            residual_summary=residual,
            status=triangle_status,
            usable_scopes=triangle_usable,
            excluded_scopes=triangle_excluded,
            limitations=triangle_limitations,
            **relation_common(triangle_alignment),
        ),
        CrossSeriesConstraintWindowV1(
            relation_kind=CrossSeriesRelationKind.INVERSE,
            relationship_id=_stable_id(
                "cross-series-relationship",
                {"kind": "inverse", "symbols": list(symbols)},
            ),
            alignment=(
                exact if inverse_candidates else _unavailable_alignment(symbols)
            ),
            residual_summary=inverse_residual,
            status=inverse_status,
            usable_scopes=(
                _scopes_for_status(inverse_status)[0]
                if inverse_candidates
                else ()
            ),
            excluded_scopes=(
                _scopes_for_status(inverse_status)[1]
                if inverse_candidates
                else ()
            ),
            limitations=(
                ()
                if inverse_candidates
                else ("no_inverse_pair_in_current_triangle",)
            ),
            **relation_common(
                exact if inverse_candidates else _unavailable_alignment(symbols)
            ),
        ),
        CrossSeriesConstraintWindowV1(
            relation_kind=CrossSeriesRelationKind.TIMESTAMP_GRID,
            relationship_id=_stable_id(
                "cross-series-relationship",
                {"kind": "timestamp_grid", "symbols": list(symbols)},
            ),
            alignment=exact,
            residual_summary=None,
            status=grid_status,
            usable_scopes=_scopes_for_status(grid_status)[0],
            excluded_scopes=_scopes_for_status(grid_status)[1],
            limitations=(
                ("sparse_exact_timestamp_grid",)
                if grid_status is CrossSeriesConstraintStatus.LIMITED
                else ()
            ),
            **relation_common(exact),
        ),
        CrossSeriesConstraintWindowV1(
            relation_kind=CrossSeriesRelationKind.RANGE_OVERLAP,
            relationship_id=_stable_id(
                "cross-series-relationship",
                {"kind": "range_overlap", "symbols": list(symbols)},
            ),
            alignment=overlap,
            residual_summary=None,
            status=overlap_status,
            usable_scopes=_scopes_for_status(overlap_status)[0],
            excluded_scopes=_scopes_for_status(overlap_status)[1],
            limitations=(
                ("unequal_member_coverage_ranges",)
                if unequal_ranges
                else (() if complete else ("partial_cross_series_group",))
            ),
            **relation_common(overlap),
        ),
        CrossSeriesConstraintWindowV1(
            relation_kind=CrossSeriesRelationKind.STALE_ALIGNMENT,
            relationship_id=_stable_id(
                "cross-series-relationship",
                {"kind": "stale_alignment", "symbols": list(symbols)},
            ),
            alignment=nearest,
            residual_summary=None,
            status=stale_status,
            usable_scopes=_scopes_for_status(stale_status)[0],
            excluded_scopes=_scopes_for_status(stale_status)[1],
            limitations=(
                ("bounded_stale_join_risk_detected",)
                if stale_count or nearest.stale_support_count
                else ()
            ),
            **relation_common(nearest),
        ),
    )


def _member_evidence(
    binding: CrossSeriesSourceBindingV1, events: Sequence[Any]
) -> CrossSeriesMemberEvidenceV1:
    timestamps = tuple(_event_time_ns(item) for item in events)
    identities = [_event_identity_payload(item) for item in events]
    quotes = [
        {
            **identity,
            "bid": _event_bid(item),
            "ask": _event_ask(item),
        }
        for identity, item in zip(identities, events, strict=True)
    ]
    return CrossSeriesMemberEvidenceV1(
        binding=binding,
        event_count=len(events),
        unique_timestamp_count=len(set(timestamps)),
        duplicate_timestamp_event_count=len(events) - len(set(timestamps)),
        coverage_start_ns=min(timestamps),
        coverage_end_ns=max(timestamps) + 1,
        event_identity_content_sha256=_content_sha256(identities),
        event_quote_content_sha256=_content_sha256(quotes),
    )


def _fingerprint_series_input(
    symbol: str, period: str, events: Sequence[Any]
) -> CrossInstrumentSeriesInput:
    series_ids = {_event_series_id(item) for item in events}
    if len(series_ids) != 1:
        raise ValueError("fingerprint series spans multiple immutable series")
    return CrossInstrumentSeriesInput(
        symbol=symbol,
        timeframe="T",
        period=period,
        series_id=next(iter(series_ids)),
        points=tuple(
            CrossInstrumentPointInput(
                timestamp_utc_ms=_event_time_ns(item) // 1_000_000,
                price=(_event_bid(item) + _event_ask(item)) / 2.0,
                row_id=_event_row_id(item),
                source_row_number=_event_row_id(item),
                event_seq=_event_sequence(item),
            )
            for item in events
        ),
        computed_from="immutable_histdata_source_events",
    )


def _exact_alignment(
    events_by_symbol: Mapping[str, Sequence[Any]],
    policy: CrossSeriesConstraintPolicyV1,
) -> CrossSeriesAlignmentSupportV1:
    by_symbol: dict[str, dict[int, list[Any]]] = {}
    for symbol, events in events_by_symbol.items():
        grouped: dict[int, list[Any]] = {}
        for item in events:
            grouped.setdefault(_event_time_ns(item), []).append(item)
        for rows in grouped.values():
            rows.sort(key=_event_order_key)
        by_symbol[symbol] = grouped
    if not by_symbol:
        return _unavailable_alignment(())
    common_times = sorted(
        set.intersection(*(set(rows) for rows in by_symbol.values()))
    )
    digest = hashlib.sha256()
    sample_ids: list[str] = []
    support = 0
    selected_times: list[int] = []
    for timestamp in common_times:
        cardinality = min(len(rows[timestamp]) for rows in by_symbol.values())
        for ordinal in range(cardinality):
            identities: list[JSONValue] = [
                _event_id(by_symbol[symbol][timestamp][ordinal])
                for symbol in sorted(by_symbol)
            ]
            alignment_id = _stable_id(
                "cross-series-exact-alignment",
                {
                    "timestamp_ns": timestamp,
                    "ordinal": ordinal,
                    "event_ids": identities,
                },
            )
            digest.update(alignment_id.encode("utf-8"))
            if len(sample_ids) < policy.max_alignment_samples:
                sample_ids.append(alignment_id)
            support += 1
            selected_times.append(timestamp)
    unmatched = {
        symbol: max(0, len(events_by_symbol[symbol]) - support)
        for symbol in sorted(events_by_symbol)
    }
    return CrossSeriesAlignmentSupportV1(
        policy=CrossSeriesAlignmentPolicy.EXACT_EVENT_SEQUENCE,
        support_count=support,
        probe_count=max(
            (len(events) for events in events_by_symbol.values()), default=0
        ),
        unmatched_event_count_by_symbol=unmatched,
        stale_support_count=0,
        maximum_observed_age_ns=0,
        p95_observed_age_ns=0,
        configured_tolerance_ns=0,
        configured_max_age_ns=0,
        support_start_ns=min(selected_times) if selected_times else None,
        support_end_ns=max(selected_times) + 1 if selected_times else None,
        recommended_event_time_ns=(
            selected_times[len(selected_times) // 2] if selected_times else None
        ),
        alignment_content_sha256=digest.hexdigest(),
        sample_alignment_ids=tuple(sample_ids),
    )


def _nearest_prior_alignment(
    events_by_symbol: Mapping[str, Sequence[Any]],
    policy: CrossSeriesConstraintPolicyV1,
) -> CrossSeriesAlignmentSupportV1:
    if not events_by_symbol:
        return _unavailable_alignment(())
    ordered = {
        symbol: tuple(sorted(events, key=_event_order_key))
        for symbol, events in events_by_symbol.items()
    }
    probe_symbol = min(
        ordered, key=lambda symbol: (len(ordered[symbol]), symbol)
    )
    probes = ordered[probe_symbol]
    times = {
        symbol: tuple(_event_time_ns(item) for item in events)
        for symbol, events in ordered.items()
    }
    digest = hashlib.sha256()
    sample_ids: list[str] = []
    ages: list[int] = []
    support = 0
    stale = 0
    supported_times: list[int] = []
    matched_event_ids_by_symbol: dict[str, set[str]] = {
        symbol: set() for symbol in ordered
    }
    for probe in probes:
        probe_time = _event_time_ns(probe)
        selected: dict[str, Any] = {}
        selected_ages: dict[str, int] = {}
        for symbol in sorted(ordered):
            rows = ordered[symbol]
            index = bisect_right(times[symbol], probe_time) - 1
            if index < 0:
                break
            item = rows[index]
            age = probe_time - _event_time_ns(item)
            if age > policy.nearest_prior_max_age_ns:
                break
            selected[symbol] = item
            selected_ages[symbol] = age
        if len(selected) != len(ordered):
            continue
        alignment_id = _stable_id(
            "cross-series-nearest-prior-alignment",
            {
                "probe_event_id": _event_id(probe),
                "event_ids": [
                    _event_id(selected[symbol]) for symbol in sorted(selected)
                ],
                "ages_ns": dict(sorted(selected_ages.items())),
                "max_age_ns": policy.nearest_prior_max_age_ns,
            },
        )
        digest.update(alignment_id.encode("utf-8"))
        if len(sample_ids) < policy.max_alignment_samples:
            sample_ids.append(alignment_id)
        support += 1
        supported_times.append(probe_time)
        max_age = max(selected_ages.values(), default=0)
        ages.append(max_age)
        stale += int(max_age > 0)
        for symbol, item in selected.items():
            matched_event_ids_by_symbol[symbol].add(_event_id(item))
    ages.sort()
    p95 = ages[max(0, math.ceil(len(ages) * 0.95) - 1)] if ages else 0
    unmatched = {
        symbol: max(0, len(events) - len(matched_event_ids_by_symbol[symbol]))
        for symbol, events in ordered.items()
    }
    return CrossSeriesAlignmentSupportV1(
        policy=CrossSeriesAlignmentPolicy.NEAREST_PRIOR_BOUNDED,
        support_count=support,
        probe_count=len(probes),
        unmatched_event_count_by_symbol=unmatched,
        stale_support_count=stale,
        maximum_observed_age_ns=max(ages, default=0),
        p95_observed_age_ns=p95,
        configured_tolerance_ns=0,
        configured_max_age_ns=policy.nearest_prior_max_age_ns,
        support_start_ns=min(supported_times) if supported_times else None,
        support_end_ns=max(supported_times) + 1 if supported_times else None,
        recommended_event_time_ns=(
            supported_times[len(supported_times) // 2]
            if supported_times
            else None
        ),
        alignment_content_sha256=digest.hexdigest(),
        sample_alignment_ids=tuple(sample_ids),
    )


def _interval_overlap_alignment(
    events_by_symbol: Mapping[str, Sequence[Any]],
) -> CrossSeriesAlignmentSupportV1:
    symbols = tuple(sorted(events_by_symbol))
    starts = [
        min(_event_time_ns(item) for item in events)
        for events in events_by_symbol.values()
        if events
    ]
    ends = [
        max(_event_time_ns(item) for item in events) + 1
        for events in events_by_symbol.values()
        if events
    ]
    complete = len(starts) == len(events_by_symbol) and bool(starts)
    overlap_start = max(starts) if complete else 0
    overlap_end = min(ends) if complete else 0
    supported = int(overlap_end > overlap_start)
    payload: dict[str, JSONValue] = {
        "symbols": list(symbols),
        "overlap_start_ns": overlap_start if supported else None,
        "overlap_end_ns": overlap_end if supported else None,
    }
    return CrossSeriesAlignmentSupportV1(
        policy=(
            CrossSeriesAlignmentPolicy.INTERVAL_OVERLAP
            if supported
            else CrossSeriesAlignmentPolicy.UNAVAILABLE
        ),
        support_count=supported,
        probe_count=1,
        unmatched_event_count_by_symbol={symbol: 0 for symbol in symbols},
        stale_support_count=0,
        maximum_observed_age_ns=0,
        p95_observed_age_ns=0,
        configured_tolerance_ns=0,
        configured_max_age_ns=0,
        support_start_ns=overlap_start if supported else None,
        support_end_ns=overlap_end if supported else None,
        recommended_event_time_ns=(
            (overlap_start + overlap_end) // 2 if supported else None
        ),
        alignment_content_sha256=_content_sha256(payload),
    )


def _unavailable_alignment(
    symbols: Sequence[str],
) -> CrossSeriesAlignmentSupportV1:
    normalized = tuple(sorted(_symbol(item) for item in symbols))
    return CrossSeriesAlignmentSupportV1(
        policy=CrossSeriesAlignmentPolicy.UNAVAILABLE,
        support_count=0,
        probe_count=0,
        unmatched_event_count_by_symbol={symbol: 0 for symbol in normalized},
        stale_support_count=0,
        maximum_observed_age_ns=0,
        p95_observed_age_ns=0,
        configured_tolerance_ns=0,
        configured_max_age_ns=0,
        support_start_ns=None,
        support_end_ns=None,
        recommended_event_time_ns=None,
        alignment_content_sha256=_content_sha256(
            {"status": "unavailable", "symbols": list(normalized)}
        ),
    )


def _triangle_status(
    summary: Mapping[str, JSONValue],
    *,
    complete: bool,
    exact: CrossSeriesAlignmentSupportV1,
    nearest: CrossSeriesAlignmentSupportV1,
    policy: CrossSeriesConstraintPolicyV1,
) -> tuple[CrossSeriesConstraintStatus, tuple[str, ...]]:
    if not complete:
        return CrossSeriesConstraintStatus.INSUFFICIENT, (
            "partial_cross_series_group",
        )
    candidates = _json_int(summary.get("candidate_count"))
    compared = _json_int(summary.get("compared_timestamp_count"))
    errors = _json_int(summary.get("error_count"))
    if not candidates:
        return CrossSeriesConstraintStatus.UNAVAILABLE, (
            "triangle_relationship_unavailable",
        )
    if errors:
        return CrossSeriesConstraintStatus.CONTRADICTORY, (
            (
                "triangle_error_ratio_exceeds_policy"
                if compared
                and errors / compared > policy.maximum_contradiction_ratio
                else "triangle_error_evidence_excluded"
            ),
        )
    if exact.support_count >= policy.minimum_alignment_support:
        if _json_int(summary.get("warning_count")):
            return CrossSeriesConstraintStatus.LIMITED, (
                "triangle_disagreement_labeled_for_anomaly_use",
            )
        return CrossSeriesConstraintStatus.READY, ()
    if nearest.support_count >= policy.minimum_alignment_support:
        return CrossSeriesConstraintStatus.LIMITED, (
            "exact_triangle_support_sparse",
            "bounded_nearest_prior_support_is_diagnostic_only",
        )
    return CrossSeriesConstraintStatus.INSUFFICIENT, (
        "triangle_alignment_support_below_policy",
    )


def _consistency_status(
    summary: Mapping[str, JSONValue],
) -> CrossSeriesConstraintStatus:
    compared = _json_int(summary.get("compared_timestamp_count"))
    if not compared:
        return CrossSeriesConstraintStatus.INSUFFICIENT
    errors = _json_int(summary.get("error_count"))
    if errors:
        return CrossSeriesConstraintStatus.CONTRADICTORY
    if errors or _json_int(summary.get("warning_count")):
        return CrossSeriesConstraintStatus.LIMITED
    return CrossSeriesConstraintStatus.READY


def _residual_summary(
    summary: Mapping[str, JSONValue],
) -> CrossSeriesResidualSummaryV1 | None:
    distribution = _json_value_mapping(
        summary.get("relative_difference_distribution")
    )
    compared = _json_int(summary.get("compared_timestamp_count"))
    if not compared and distribution.get("status") != "valid":
        return None
    return CrossSeriesResidualSummaryV1(
        compared_count=compared,
        warning_count=_json_int(summary.get("warning_count")),
        error_count=_json_int(summary.get("error_count")),
        minimum=_optional_numeric(distribution.get("minimum")),
        mean=_optional_numeric(distribution.get("mean")),
        p50_upper_bound=_optional_numeric(distribution.get("p50_upper_bound")),
        p95_upper_bound=_optional_numeric(distribution.get("p95_upper_bound")),
        p99_upper_bound=_optional_numeric(distribution.get("p99_upper_bound")),
        maximum=_optional_numeric(distribution.get("maximum")),
        distribution_content_sha256=_content_sha256(distribution),
    )


def _fingerprint_group(
    payload: Mapping[str, JSONValue], period: str
) -> Mapping[str, JSONValue]:
    for item in _sequence(payload.get("groups")):
        row = _json_value_mapping(item)
        axis = _json_value_mapping(row.get("target_axis"))
        if str(axis.get("period")) == period:
            return row
    return {}


def _limiting_symbols(
    coverage: Mapping[str, JSONValue],
    events_by_symbol: Mapping[str, Sequence[Any]],
) -> tuple[str, ...]:
    limiting: set[str] = set()
    for name in ("limiting_start_symbols", "limiting_end_symbols"):
        limiting.update(_symbol(item) for item in _sequence(coverage.get(name)))
    if not limiting:
        minimum = min(len(events) for events in events_by_symbol.values())
        limiting.update(
            symbol
            for symbol, events in events_by_symbol.items()
            if len(events) == minimum
        )
    return tuple(sorted(limiting))


def _scopes_for_status(
    status: CrossSeriesConstraintStatus,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if status in {
        CrossSeriesConstraintStatus.READY,
        CrossSeriesConstraintStatus.LIMITED,
    }:
        return _ALL_USE_SCOPES, ()
    if status in {
        CrossSeriesConstraintStatus.CONTRADICTORY,
        CrossSeriesConstraintStatus.INSUFFICIENT,
        CrossSeriesConstraintStatus.UNAVAILABLE,
    }:
        return ("anomaly_label",), tuple(
            item for item in _ALL_USE_SCOPES if item != "anomaly_label"
        )
    return (), _ALL_USE_SCOPES


def _bundle_status(
    windows: Sequence[CrossSeriesConstraintWindowV1],
    required_symbols: Sequence[str],
    members: Sequence[CrossSeriesMemberEvidenceV1],
) -> CrossSeriesConstraintStatus:
    if not windows or not set(required_symbols).issubset(
        {item.symbol for item in members}
    ):
        return CrossSeriesConstraintStatus.UNAVAILABLE
    triangle = tuple(
        item
        for item in windows
        if item.relation_kind is CrossSeriesRelationKind.TRIANGLE
    )
    if any(
        item.status is CrossSeriesConstraintStatus.CONTRADICTORY
        for item in triangle
    ):
        return CrossSeriesConstraintStatus.CONTRADICTORY
    if any(
        item.status
        in {
            CrossSeriesConstraintStatus.INSUFFICIENT,
            CrossSeriesConstraintStatus.UNAVAILABLE,
        }
        for item in triangle
    ):
        return CrossSeriesConstraintStatus.INSUFFICIENT
    if any(
        item.status is CrossSeriesConstraintStatus.LIMITED for item in windows
    ):
        return CrossSeriesConstraintStatus.LIMITED
    return CrossSeriesConstraintStatus.READY


def _stage_scope(stage: str) -> str:
    normalized = _required_text(stage, "stage").strip().lower()
    return {
        "source_enrichment": "normal_training",
        "proposal": "proposal",
        "carving": "carving",
        "cross_series_reconciliation": "reconciliation",
        "reconciliation": "reconciliation",
        "validation": "validation",
    }.get(normalized, normalized)


def _event_order_key(item: Any) -> tuple[int, int, int, str]:
    return (
        _event_time_ns(item),
        _event_sequence(item),
        _event_row_id(item),
        _event_id(item),
    )


def _event_identity_payload(item: Any) -> dict[str, JSONValue]:
    return {
        "event_id": _event_id(item),
        "symbol": _event_symbol(item),
        "event_time_ns": _event_time_ns(item),
        "event_sequence": _event_sequence(item),
        "source_series_id": _event_series_id(item),
        "source_period": _event_period(item),
        "source_row_id": _event_row_id(item),
    }


def _event_time_ns(item: Any) -> int:
    return _int64(getattr(item, "event_time_ns", None), "event_time_ns")


def _event_symbol(item: Any) -> str:
    return _symbol(getattr(item, "symbol", None))


def _event_origin(item: Any) -> str:
    value = getattr(item, "origin", None)
    selected = getattr(value, "value", value)
    return _required_text(selected, "origin").lower()


def _event_sequence(item: Any) -> int:
    return _nonnegative_int(
        getattr(item, "event_sequence", None), "event_sequence"
    )


def _event_row_id(item: Any) -> int:
    return _positive_int(getattr(item, "source_row_id", None), "source_row_id")


def _event_series_id(item: Any) -> str:
    return _required_text(
        getattr(item, "source_series_id", None), "source_series_id"
    )


def _event_period(item: Any) -> str:
    return _period(getattr(item, "source_period", None))


def _event_id(item: Any) -> str:
    return _required_text(getattr(item, "event_id", None), "event_id")


def _event_bid(item: Any) -> float:
    value = _finite_float(getattr(item, "bid", None), "bid")
    if value <= 0.0:
        raise ValueError("event bid must be positive")
    return value


def _event_ask(item: Any) -> float:
    value = _finite_float(getattr(item, "ask", None), "ask")
    if value <= 0.0 or value < _event_bid(item):
        raise ValueError("event ask must be positive and >= bid")
    return value


def _use_scopes(values: Sequence[str]) -> tuple[str, ...]:
    scopes = _text_tuple(values, "use scopes", maximum=len(_ALL_USE_SCOPES))
    unknown = set(scopes).difference(_ALL_USE_SCOPES)
    if unknown:
        raise ValueError(
            f"unknown cross-series use scopes: {sorted(unknown)!r}"
        )
    return scopes


def _symbols(values: Sequence[str]) -> tuple[str, ...]:
    result = tuple(sorted({_symbol(value) for value in values}))
    if not result:
        raise ValueError("symbols cannot be empty")
    return result


def _symbol(value: object) -> str:
    selected = "".join(
        character
        for character in str(value or "").strip().lower()
        if character.isalnum()
    )
    if not selected:
        raise ValueError("symbol cannot be empty")
    return selected


def _period(value: object) -> str:
    selected = str(value or "").strip()
    if not _PERIOD_RE.fullmatch(selected) or not 1 <= int(selected[4:]) <= 12:
        raise ValueError("period must use YYYYMM")
    return selected


def _stable_id(namespace: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{namespace}:sha256:{digest}"


def _content_sha256(value: object) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_json(value: Mapping[str, JSONValue]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _required_text(value: object, name: str) -> str:
    selected = str(value or "").strip()
    if not selected or len(selected) > MAX_CROSS_SERIES_TEXT:
        raise ValueError(f"{name} must be bounded non-empty text")
    return selected


def _text_tuple(
    values: Sequence[str],
    name: str,
    *,
    maximum: int,
    lowercase: bool = False,
) -> tuple[str, ...]:
    result = tuple(
        sorted(
            {
                (
                    _required_text(value, name).lower()
                    if lowercase
                    else _required_text(value, name)
                )
                for value in values
            }
        )
    )
    if len(result) > maximum:
        raise ValueError(f"{name} exceeds the v1 bound")
    return result


def _sha256(value: object, name: str) -> str:
    selected = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(selected):
        raise ValueError(f"{name} must be a lowercase SHA-256")
    return selected


def _strict_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _int64(value: object, name: str) -> int:
    selected = _strict_int(value, name)
    if not -(2**63) <= selected < 2**63:
        raise ValueError(f"{name} exceeds int64")
    return selected


def _positive_int(value: object, name: str) -> int:
    selected = _strict_int(value, name)
    if selected <= 0:
        raise ValueError(f"{name} must be positive")
    return selected


def _nonnegative_int(value: object, name: str) -> int:
    selected = _strict_int(value, name)
    if selected < 0:
        raise ValueError(f"{name} must be non-negative")
    return selected


def _finite_float(value: object, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    selected = float(value)
    if not math.isfinite(selected):
        raise ValueError(f"{name} must be finite")
    return selected


def _nonnegative_float(value: object, name: str) -> float:
    selected = _finite_float(value, name)
    if selected < 0.0:
        raise ValueError(f"{name} must be non-negative")
    return selected


def _unit_float(value: object, name: str) -> float:
    selected = _finite_float(value, name)
    if not 0.0 <= selected <= 1.0:
        raise ValueError(f"{name} must be inside [0,1]")
    return selected


def _optional_float(value: object, name: str) -> float | None:
    return None if value is None else _finite_float(value, name)


def _optional_numeric(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    selected = float(value)
    return selected if math.isfinite(selected) and selected >= 0.0 else None


def _strict_bool(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be boolean")
    return value


def _json_scalar(value: object) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise TypeError("cross-series effect must be a JSON scalar")


def _json_int(value: object) -> int:
    return (
        value if isinstance(value, int) and not isinstance(value, bool) else 0
    )


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a JSON object")
    return cast(Mapping[str, Any], value)


def _json_value_mapping(value: object) -> Mapping[str, JSONValue]:
    if not isinstance(value, Mapping):
        return {}
    return cast(Mapping[str, JSONValue], value)


def _sequence(value: object) -> Sequence[Any]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        return ()
    return value


def _string_tuple(value: object) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    if not isinstance(value, Mapping):
        raise TypeError("cross-series artifact must contain a JSON object")
    return cast(Mapping[str, Any], value)


def _require_version(actual: str, expected: str, name: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported {name} schema")


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError(f"unsupported schema; expected {expected}")


def _require_derived(
    data: Mapping[str, Any], name: str, expected: object
) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived field {name} differs")


__all__ = [
    "CROSS_SERIES_ALIGNMENT_SUPPORT_SCHEMA_VERSION",
    "CROSS_SERIES_CONSTRAINT_BUNDLE_ARTIFACT_KIND",
    "CROSS_SERIES_CONSTRAINT_BUNDLE_SCHEMA_VERSION",
    "CROSS_SERIES_CONSTRAINT_POLICY_ARTIFACT_KIND",
    "CROSS_SERIES_CONSTRAINT_POLICY_SCHEMA_VERSION",
    "CROSS_SERIES_CONSTRAINT_USE_SCHEMA_VERSION",
    "CROSS_SERIES_CONSTRAINT_WINDOW_SCHEMA_VERSION",
    "CROSS_SERIES_MEMBER_EVIDENCE_SCHEMA_VERSION",
    "CROSS_SERIES_RESIDUAL_SUMMARY_SCHEMA_VERSION",
    "CROSS_SERIES_SOURCE_BINDING_SCHEMA_VERSION",
    "CrossSeriesAlignmentPolicy",
    "CrossSeriesAlignmentSupportV1",
    "CrossSeriesConstraintBundleV1",
    "CrossSeriesConstraintPolicyV1",
    "CrossSeriesConstraintStatus",
    "CrossSeriesConstraintUseStatus",
    "CrossSeriesConstraintUseV1",
    "CrossSeriesConstraintWindowV1",
    "CrossSeriesMemberEvidenceV1",
    "CrossSeriesRelationKind",
    "CrossSeriesResidualSummaryV1",
    "CrossSeriesSourceBindingV1",
    "compile_histdata_cross_series_constraints",
    "cross_series_constraint_use",
    "read_cross_series_constraint_bundle",
    "read_cross_series_constraint_policy",
    "require_constraint_support_for_synchronization_time",
    "select_constraint_synchronization_time",
]
