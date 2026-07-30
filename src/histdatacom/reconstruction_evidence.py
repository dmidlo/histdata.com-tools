"""Provider-neutral point-in-time quality evidence for reconstruction.

The domain contracts in this module deliberately do not model a broker or a
vendor-specific wire format.  The first implementation adapter accepts the
immutable HistData.com ASCII tick cache used by the current research
milestone.  Later providers can compile the same contracts without changing
their meaning.

Evidence artifacts are bounded sidecars.  They may retain scalar findings and
row identities, but never copy complete tick rows or complete source reports.
"""

from __future__ import annotations

import hashlib
import json
import math
import statistics
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, cast

from histdatacom.data_quality.contracts import QualityFinding, QualityReport
from histdatacom.runtime_contracts import JSONScalar, JSONValue

RECONSTRUCTION_EVIDENCE_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-evidence-policy.v1"
)
RECONSTRUCTION_EVIDENCE_RECORD_SCHEMA_VERSION = (
    "histdatacom.reconstruction-evidence-record.v1"
)
RECONSTRUCTION_EVIDENCE_PROJECTION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-evidence-projection.v1"
)
RECONSTRUCTION_EVIDENCE_USE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-evidence-use.v1"
)

RECONSTRUCTION_EVIDENCE_POLICY_ARTIFACT_KIND = (
    "reconstruction_evidence_policy_v1"
)
RECONSTRUCTION_EVIDENCE_PROJECTION_ARTIFACT_KIND = (
    "reconstruction_evidence_projection_v1"
)

CURRENT_EVIDENCE_SOURCE_PROVIDER_ID = "histdata.com"
HISTDATA_LEGACY_CACHE_SCHEMA_VERSION = (
    "histdatacom.histdata-ascii-tick-cache.legacy"
)
HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION = (
    "histdatacom.ascii-tick-training-features.v1"
)
DEFAULT_EVIDENCE_SUSPICIOUS_GAP_MS = 172_800_000
DEFAULT_EVIDENCE_WIDE_SPREAD_MULTIPLIER = 3.0
DEFAULT_EVIDENCE_STALE_QUOTE_RUN_LENGTH = 4
DEFAULT_EVIDENCE_BURST_INTERVAL_NS = 250_000_000
DEFAULT_EVIDENCE_BURST_RUN_LENGTH = 4
MAX_EVIDENCE_RECORDS = 256
MAX_EVIDENCE_ROW_RECORDS = 64
MAX_EVIDENCE_LIMITATIONS = 64
MAX_EVIDENCE_EFFECTS = 32
MAX_EVIDENCE_TEXT = 1024
MIN_EVIDENCE_SIDECAR_CAPACITY = 24

_CACHED_ROW_METRICS = frozenset(
    {
        "duplicate_timestamp",
        "non_monotonic_timestamp",
        "suspicious_gap",
        "weekend_activity",
        "session_closed",
        "negative_spread",
        "zero_spread",
        "wide_spread",
        "invalid_row",
        "partial_row",
        "source_availability",
        "topology_unavailable",
        "distribution_missing",
        "precision_warning",
        "cache_float_precision",
        "fingerprint_unready",
    }
)


class ReconstructionEvidenceKind(str, Enum):
    """Scientific role of one evidence record."""

    ROW_FACT = "row_fact"
    INTERVAL_FINDING = "interval_finding"
    SERIES_FINGERPRINT = "series_fingerprint"
    ADVISORY = "advisory"
    DEFAULT = "default"
    UNAVAILABLE = "unavailable"


class ReconstructionEvidenceInformationMode(str, Enum):
    """Information modes shared by the provider-neutral evidence contract."""

    EX_POST_RECONSTRUCTION = "ex_post_reconstruction"
    EX_ANTE_SIMULATION = "ex_ante_simulation"

    @classmethod
    def from_value(cls, value: object) -> ReconstructionEvidenceInformationMode:
        """Normalize this contract's mode or a compatible domain enum."""
        if isinstance(value, cls):
            return value
        selected = getattr(value, "value", value)
        try:
            return cls(str(selected))
        except ValueError as err:
            raise ValueError(
                "unsupported reconstruction evidence information mode"
            ) from err


class ReconstructionEvidenceGrain(str, Enum):
    """Support grain of a source or target value."""

    ROW = "row"
    INTERVAL = "interval"
    WINDOW = "window"
    PARTITION = "partition"
    PERIOD = "period"
    SERIES = "series"


class ReconstructionEvidenceReadiness(str, Enum):
    """Whether a value is usable under its declared limitations."""

    READY = "ready"
    LIMITED = "limited"
    UNAVAILABLE = "unavailable"


class ReconstructionEvidenceUseStatus(str, Enum):
    """How one stage handled a compatible projection."""

    APPLIED = "applied"
    REFUSED = "refused"
    NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True, slots=True)
class ReconstructionEvidencePolicyV1:
    """Versioned projection, fallback, and boundedness policy."""

    supported_provider_ids: tuple[str, ...] = (
        CURRENT_EVIDENCE_SOURCE_PROVIDER_ID,
    )
    suspicious_gap_fallback_ms: int = DEFAULT_EVIDENCE_SUSPICIOUS_GAP_MS
    wide_spread_multiplier: float = DEFAULT_EVIDENCE_WIDE_SPREAD_MULTIPLIER
    minimum_wide_spread: float = 0.0
    stale_quote_run_length: int = DEFAULT_EVIDENCE_STALE_QUOTE_RUN_LENGTH
    burst_max_interval_ns: int = DEFAULT_EVIDENCE_BURST_INTERVAL_NS
    burst_run_length: int = DEFAULT_EVIDENCE_BURST_RUN_LENGTH
    quality_warning_score_penalty: float = 0.05
    max_records: int = MAX_EVIDENCE_RECORDS
    max_row_records: int = MAX_EVIDENCE_ROW_RECORDS
    fail_closed_on_source_unavailable: bool = True
    policy_id: str = ""
    schema_version: str = RECONSTRUCTION_EVIDENCE_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_EVIDENCE_POLICY_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction evidence policy")
        providers = _normalized_text_tuple(
            self.supported_provider_ids,
            "supported_provider_ids",
            maximum=16,
            lowercase=True,
        )
        if not providers:
            raise ValueError("evidence policy requires a supported provider")
        object.__setattr__(self, "supported_provider_ids", providers)
        for name in (
            "suspicious_gap_fallback_ms",
            "stale_quote_run_length",
            "burst_max_interval_ns",
            "burst_run_length",
            "max_records",
            "max_row_records",
        ):
            value = _positive_int(getattr(self, name), name)
            object.__setattr__(self, name, value)
        if self.max_records > MAX_EVIDENCE_RECORDS:
            raise ValueError("max_records exceeds the v1 evidence limit")
        if self.max_row_records > min(
            self.max_records, MAX_EVIDENCE_ROW_RECORDS
        ):
            raise ValueError("max_row_records exceeds the v1 evidence limit")
        if (
            self.max_records - self.max_row_records
            < MIN_EVIDENCE_SIDECAR_CAPACITY
        ):
            raise ValueError(
                "evidence policy must reserve capacity for mandatory sidecars"
            )
        multiplier = _finite_float(
            self.wide_spread_multiplier, "wide_spread_multiplier"
        )
        if multiplier <= 1.0:
            raise ValueError("wide_spread_multiplier must exceed one")
        object.__setattr__(self, "wide_spread_multiplier", multiplier)
        minimum = _finite_float(self.minimum_wide_spread, "minimum_wide_spread")
        if minimum < 0.0:
            raise ValueError("minimum_wide_spread must be non-negative")
        object.__setattr__(self, "minimum_wide_spread", minimum)
        warning_penalty = _finite_float(
            self.quality_warning_score_penalty,
            "quality_warning_score_penalty",
        )
        if not 0.0 <= warning_penalty <= 1.0:
            raise ValueError(
                "quality_warning_score_penalty must be inside [0,1]"
            )
        object.__setattr__(
            self, "quality_warning_score_penalty", warning_penalty
        )
        if type(self.fail_closed_on_source_unavailable) is not bool:
            raise ValueError(
                "fail_closed_on_source_unavailable must be boolean"
            )
        expected = _stable_id("reconstruction-evidence-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ValueError("reconstruction evidence policy_id differs")
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic policy identity."""
        return {
            "schema_version": self.schema_version,
            "supported_provider_ids": list(self.supported_provider_ids),
            "suspicious_gap_fallback_ms": self.suspicious_gap_fallback_ms,
            "wide_spread_multiplier": self.wide_spread_multiplier,
            "minimum_wide_spread": self.minimum_wide_spread,
            "stale_quote_run_length": self.stale_quote_run_length,
            "burst_max_interval_ns": self.burst_max_interval_ns,
            "burst_run_length": self.burst_run_length,
            "quality_warning_score_penalty": (
                self.quality_warning_score_penalty
            ),
            "max_records": self.max_records,
            "max_row_records": self.max_row_records,
            "fail_closed_on_source_unavailable": (
                self.fail_closed_on_source_unavailable
            ),
            "provider_neutral_contract": True,
            "current_dataset_boundary": "histdata-ascii-tick-only",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible policy data."""
        return {**self.payload(), "policy_id": self.policy_id}

    def to_json(self) -> str:
        """Return canonical JSON."""
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionEvidencePolicyV1:
        """Restore and verify a version-one policy."""
        _require_schema(data, RECONSTRUCTION_EVIDENCE_POLICY_SCHEMA_VERSION)
        _require_derived(data, "provider_neutral_contract", True)
        _require_derived(
            data, "current_dataset_boundary", "histdata-ascii-tick-only"
        )
        return cls(
            supported_provider_ids=_string_tuple(
                data.get("supported_provider_ids")
            ),
            suspicious_gap_fallback_ms=_strict_int(
                data.get("suspicious_gap_fallback_ms"),
                "suspicious_gap_fallback_ms",
            ),
            wide_spread_multiplier=_finite_float(
                data.get("wide_spread_multiplier"), "wide_spread_multiplier"
            ),
            minimum_wide_spread=_finite_float(
                data.get("minimum_wide_spread"), "minimum_wide_spread"
            ),
            stale_quote_run_length=_strict_int(
                data.get("stale_quote_run_length"), "stale_quote_run_length"
            ),
            burst_max_interval_ns=_strict_int(
                data.get("burst_max_interval_ns"), "burst_max_interval_ns"
            ),
            burst_run_length=_strict_int(
                data.get("burst_run_length"), "burst_run_length"
            ),
            quality_warning_score_penalty=_finite_float(
                data.get("quality_warning_score_penalty"),
                "quality_warning_score_penalty",
            ),
            max_records=_strict_int(data.get("max_records"), "max_records"),
            max_row_records=_strict_int(
                data.get("max_row_records"), "max_row_records"
            ),
            fail_closed_on_source_unavailable=_strict_bool(
                data.get("fail_closed_on_source_unavailable"),
                "fail_closed_on_source_unavailable",
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ReconstructionEvidencePolicyV1:
        """Restore a policy from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionEvidenceRecordV1:
    """One scalar constraint with complete point-in-time provenance."""

    kind: ReconstructionEvidenceKind
    metric_id: str
    value: JSONScalar
    source_artifact_id: str
    source_artifact_sha256: str
    source_partition_id: str
    rule_id: str
    calculation_basis: str
    source_grain: ReconstructionEvidenceGrain
    target_grain: ReconstructionEvidenceGrain
    support_start_ns: int
    support_end_ns: int
    available_at_ns: int
    as_of_ns: int
    information_mode: ReconstructionEvidenceInformationMode
    projection_method: str
    readiness: ReconstructionEvidenceReadiness
    confidence: float
    symbol: str
    source_row_id: int | None = None
    limitations: tuple[str, ...] = ()
    record_id: str = ""
    schema_version: str = RECONSTRUCTION_EVIDENCE_RECORD_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_EVIDENCE_RECORD_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction evidence record")
        object.__setattr__(self, "kind", ReconstructionEvidenceKind(self.kind))
        object.__setattr__(
            self, "source_grain", ReconstructionEvidenceGrain(self.source_grain)
        )
        object.__setattr__(
            self, "target_grain", ReconstructionEvidenceGrain(self.target_grain)
        )
        object.__setattr__(
            self, "readiness", ReconstructionEvidenceReadiness(self.readiness)
        )
        object.__setattr__(
            self,
            "information_mode",
            ReconstructionEvidenceInformationMode.from_value(
                self.information_mode
            ),
        )
        for name in (
            "metric_id",
            "source_artifact_id",
            "source_artifact_sha256",
            "source_partition_id",
            "rule_id",
            "calculation_basis",
            "projection_method",
            "symbol",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "source_artifact_sha256",
            _sha256(self.source_artifact_sha256, "source_artifact_sha256"),
        )
        object.__setattr__(self, "symbol", self.symbol.lower())
        for name in (
            "support_start_ns",
            "support_end_ns",
            "available_at_ns",
            "as_of_ns",
        ):
            object.__setattr__(self, name, _int64(getattr(self, name), name))
        if self.support_end_ns < self.support_start_ns:
            raise ValueError("evidence support interval regresses")
        if self.kind is ReconstructionEvidenceKind.UNAVAILABLE:
            if self.value is not None:
                raise ValueError("unavailable evidence cannot retain a value")
            if (
                self.readiness
                is not ReconstructionEvidenceReadiness.UNAVAILABLE
            ):
                raise ValueError(
                    "unavailable evidence requires unavailable readiness"
                )
        elif (
            self.value is None
            and self.readiness is ReconstructionEvidenceReadiness.READY
        ):
            raise ValueError("ready evidence requires a value")
        _json_scalar(self.value)
        confidence = _finite_float(self.confidence, "confidence")
        if not 0.0 <= confidence <= 1.0:
            raise ValueError("evidence confidence must be inside [0,1]")
        object.__setattr__(self, "confidence", confidence)
        if self.source_row_id is not None:
            object.__setattr__(
                self,
                "source_row_id",
                _positive_int(self.source_row_id, "source_row_id"),
            )
        limitations = _normalized_text_tuple(
            self.limitations,
            "limitations",
            maximum=MAX_EVIDENCE_LIMITATIONS,
        )
        object.__setattr__(self, "limitations", limitations)
        if (
            self.information_mode
            is ReconstructionEvidenceInformationMode.EX_ANTE_SIMULATION
            and self.readiness
            is not ReconstructionEvidenceReadiness.UNAVAILABLE
            and self.available_at_ns > self.as_of_ns
        ):
            raise ValueError(
                "ex-ante evidence is not available at its as-of time"
            )
        expected = _stable_id("reconstruction-evidence-record", self.payload())
        if self.record_id and self.record_id != expected:
            raise ValueError("reconstruction evidence record_id differs")
        object.__setattr__(self, "record_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic record identity."""
        return {
            "schema_version": self.schema_version,
            "kind": self.kind.value,
            "metric_id": self.metric_id,
            "value": self.value,
            "source_artifact_id": self.source_artifact_id,
            "source_artifact_sha256": self.source_artifact_sha256,
            "source_partition_id": self.source_partition_id,
            "rule_id": self.rule_id,
            "calculation_basis": self.calculation_basis,
            "source_grain": self.source_grain.value,
            "target_grain": self.target_grain.value,
            "support_start_ns": self.support_start_ns,
            "support_end_ns": self.support_end_ns,
            "available_at_ns": self.available_at_ns,
            "as_of_ns": self.as_of_ns,
            "information_mode": self.information_mode.value,
            "projection_method": self.projection_method,
            "readiness": self.readiness.value,
            "confidence": self.confidence,
            "symbol": self.symbol,
            "source_row_id": self.source_row_id,
            "limitations": list(self.limitations),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible record data."""
        return {**self.payload(), "record_id": self.record_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionEvidenceRecordV1:
        """Restore and verify one evidence record."""
        _require_schema(data, RECONSTRUCTION_EVIDENCE_RECORD_SCHEMA_VERSION)
        return cls(
            kind=ReconstructionEvidenceKind(str(data.get("kind", ""))),
            metric_id=str(data.get("metric_id", "")),
            value=_json_scalar(data.get("value")),
            source_artifact_id=str(data.get("source_artifact_id", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            source_partition_id=str(data.get("source_partition_id", "")),
            rule_id=str(data.get("rule_id", "")),
            calculation_basis=str(data.get("calculation_basis", "")),
            source_grain=ReconstructionEvidenceGrain(
                str(data.get("source_grain", ""))
            ),
            target_grain=ReconstructionEvidenceGrain(
                str(data.get("target_grain", ""))
            ),
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
            projection_method=str(data.get("projection_method", "")),
            readiness=ReconstructionEvidenceReadiness(
                str(data.get("readiness", ""))
            ),
            confidence=_finite_float(data.get("confidence"), "confidence"),
            symbol=str(data.get("symbol", "")),
            source_row_id=(
                None
                if data.get("source_row_id") is None
                else _strict_int(data.get("source_row_id"), "source_row_id")
            ),
            limitations=_string_tuple(data.get("limitations")),
            record_id=str(data.get("record_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class PointInTimeEvidenceProjectionV1:
    """Bounded row facts and sidecar constraints for one source partition."""

    evidence_window_id: str
    source_provider_id: str
    source_partition_id: str
    source_artifact_id: str
    source_artifact_sha256: str
    symbol: str
    period: str
    support_start_ns: int
    support_end_ns: int
    available_at_ns: int
    as_of_ns: int
    information_mode: ReconstructionEvidenceInformationMode
    policy_id: str
    row_records: tuple[ReconstructionEvidenceRecordV1, ...] = ()
    sidecar_records: tuple[ReconstructionEvidenceRecordV1, ...] = ()
    status: ReconstructionEvidenceReadiness = (
        ReconstructionEvidenceReadiness.READY
    )
    limitations: tuple[str, ...] = ()
    omitted_record_count: int = 0
    projection_id: str = ""
    schema_version: str = RECONSTRUCTION_EVIDENCE_PROJECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_EVIDENCE_PROJECTION_SCHEMA_VERSION
        ):
            raise ValueError("unsupported point-in-time evidence projection")
        for name in (
            "evidence_window_id",
            "source_provider_id",
            "source_partition_id",
            "source_artifact_id",
            "source_artifact_sha256",
            "symbol",
            "period",
            "policy_id",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self, "source_provider_id", self.source_provider_id.lower()
        )
        object.__setattr__(
            self,
            "source_artifact_sha256",
            _sha256(self.source_artifact_sha256, "source_artifact_sha256"),
        )
        object.__setattr__(self, "symbol", self.symbol.lower())
        object.__setattr__(self, "period", _period(self.period))
        for name in (
            "support_start_ns",
            "support_end_ns",
            "available_at_ns",
            "as_of_ns",
        ):
            object.__setattr__(self, name, _int64(getattr(self, name), name))
        if self.support_end_ns <= self.support_start_ns:
            raise ValueError("evidence projection support is empty")
        object.__setattr__(
            self,
            "information_mode",
            ReconstructionEvidenceInformationMode.from_value(
                self.information_mode
            ),
        )
        object.__setattr__(
            self, "status", ReconstructionEvidenceReadiness(self.status)
        )
        rows = tuple(self.row_records)
        sidecars = tuple(self.sidecar_records)
        if any(
            not isinstance(item, ReconstructionEvidenceRecordV1)
            for item in rows
        ):
            raise TypeError("row_records require evidence record contracts")
        if any(
            item.target_grain is not ReconstructionEvidenceGrain.ROW
            for item in rows
        ):
            raise ValueError("row_records must have row target grain")
        if any(
            not isinstance(item, ReconstructionEvidenceRecordV1)
            for item in sidecars
        ):
            raise TypeError("sidecar_records require evidence record contracts")
        if any(
            item.target_grain is ReconstructionEvidenceGrain.ROW
            for item in sidecars
        ):
            raise ValueError("aggregate evidence cannot be flattened onto rows")
        if len(rows) > MAX_EVIDENCE_ROW_RECORDS:
            raise ValueError("row evidence exceeds the v1 bound")
        if len(rows) + len(sidecars) > MAX_EVIDENCE_RECORDS:
            raise ValueError("evidence projection exceeds the v1 bound")
        for item in (*rows, *sidecars):
            if item.source_partition_id != self.source_partition_id:
                raise ValueError("evidence record partition differs")
            if item.symbol != self.symbol:
                raise ValueError("evidence record symbol differs")
            if item.information_mode is not self.information_mode:
                raise ValueError("evidence record information mode differs")
            if (
                item.support_start_ns < self.support_start_ns
                or item.support_end_ns > self.support_end_ns
            ):
                raise ValueError("evidence record exceeds projection support")
        object.__setattr__(self, "row_records", rows)
        object.__setattr__(self, "sidecar_records", sidecars)
        limitations = _normalized_text_tuple(
            self.limitations,
            "limitations",
            maximum=MAX_EVIDENCE_LIMITATIONS,
        )
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(
            self,
            "omitted_record_count",
            _nonnegative_int(self.omitted_record_count, "omitted_record_count"),
        )
        expected = _stable_id(
            "reconstruction-evidence-projection", self.payload()
        )
        if self.projection_id and self.projection_id != expected:
            raise ValueError("point-in-time evidence projection_id differs")
        object.__setattr__(self, "projection_id", expected)

    @property
    def records(self) -> tuple[ReconstructionEvidenceRecordV1, ...]:
        """Return every retained record without changing its grain."""
        return (*self.row_records, *self.sidecar_records)

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic projection identity."""
        return {
            "schema_version": self.schema_version,
            "evidence_window_id": self.evidence_window_id,
            "source_provider_id": self.source_provider_id,
            "source_partition_id": self.source_partition_id,
            "source_artifact_id": self.source_artifact_id,
            "source_artifact_sha256": self.source_artifact_sha256,
            "symbol": self.symbol,
            "period": self.period,
            "support_start_ns": self.support_start_ns,
            "support_end_ns": self.support_end_ns,
            "available_at_ns": self.available_at_ns,
            "as_of_ns": self.as_of_ns,
            "information_mode": self.information_mode.value,
            "policy_id": self.policy_id,
            "row_records": [item.to_dict() for item in self.row_records],
            "sidecar_records": [
                item.to_dict() for item in self.sidecar_records
            ],
            "status": self.status.value,
            "limitations": list(self.limitations),
            "omitted_record_count": self.omitted_record_count,
            "bounded_sidecar": True,
            "full_tick_rows_embedded": False,
            "full_reports_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible projection data."""
        return {**self.payload(), "projection_id": self.projection_id}

    def to_json(self) -> str:
        """Return canonical JSON."""
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> PointInTimeEvidenceProjectionV1:
        """Restore and verify one projection."""
        _require_schema(data, RECONSTRUCTION_EVIDENCE_PROJECTION_SCHEMA_VERSION)
        _require_derived(data, "bounded_sidecar", True)
        _require_derived(data, "full_tick_rows_embedded", False)
        _require_derived(data, "full_reports_embedded", False)
        return cls(
            evidence_window_id=str(data.get("evidence_window_id", "")),
            source_provider_id=str(data.get("source_provider_id", "")),
            source_partition_id=str(data.get("source_partition_id", "")),
            source_artifact_id=str(data.get("source_artifact_id", "")),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
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
            row_records=tuple(
                ReconstructionEvidenceRecordV1.from_dict(_mapping(item))
                for item in _sequence(data.get("row_records"))
            ),
            sidecar_records=tuple(
                ReconstructionEvidenceRecordV1.from_dict(_mapping(item))
                for item in _sequence(data.get("sidecar_records"))
            ),
            status=ReconstructionEvidenceReadiness(str(data.get("status", ""))),
            limitations=_string_tuple(data.get("limitations")),
            omitted_record_count=_strict_int(
                data.get("omitted_record_count", 0), "omitted_record_count"
            ),
            projection_id=str(data.get("projection_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> PointInTimeEvidenceProjectionV1:
        """Restore a projection from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionEvidenceUseV1:
    """One explicit stage application or refusal with bounded effects."""

    stage: str
    status: ReconstructionEvidenceUseStatus
    projection_ids: tuple[str, ...]
    used_at_ns: int
    consumed_record_ids: tuple[str, ...]
    reason: str
    effects: Mapping[str, JSONScalar] = field(default_factory=dict)
    decision_id: str = ""
    schema_version: str = RECONSTRUCTION_EVIDENCE_USE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_EVIDENCE_USE_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction evidence use")
        object.__setattr__(self, "stage", _required_text(self.stage, "stage"))
        object.__setattr__(
            self, "status", ReconstructionEvidenceUseStatus(self.status)
        )
        projections = _normalized_text_tuple(
            self.projection_ids, "projection_ids", maximum=64
        )
        if not projections:
            raise ValueError("evidence use requires a projection")
        object.__setattr__(self, "projection_ids", projections)
        object.__setattr__(
            self,
            "consumed_record_ids",
            _normalized_text_tuple(
                self.consumed_record_ids,
                "consumed_record_ids",
                maximum=MAX_EVIDENCE_RECORDS,
            ),
        )
        object.__setattr__(
            self, "used_at_ns", _int64(self.used_at_ns, "used_at_ns")
        )
        object.__setattr__(
            self, "reason", _required_text(self.reason, "reason")
        )
        if len(self.effects) > MAX_EVIDENCE_EFFECTS:
            raise ValueError("evidence effects exceed the v1 bound")
        effects = {
            _required_text(str(name), "effect name"): _json_scalar(value)
            for name, value in sorted(self.effects.items())
        }
        object.__setattr__(self, "effects", effects)
        expected = _stable_id("reconstruction-evidence-use", self.payload())
        if self.decision_id and self.decision_id != expected:
            raise ValueError("reconstruction evidence decision_id differs")
        object.__setattr__(self, "decision_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return semantic decision identity."""
        return {
            "schema_version": self.schema_version,
            "stage": self.stage,
            "status": self.status.value,
            "projection_ids": list(self.projection_ids),
            "used_at_ns": self.used_at_ns,
            "consumed_record_ids": list(self.consumed_record_ids),
            "reason": self.reason,
            "effects": dict(self.effects),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic JSON-compatible decision data."""
        return {**self.payload(), "decision_id": self.decision_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ReconstructionEvidenceUseV1:
        """Restore and verify one evidence-use decision."""
        _require_schema(data, RECONSTRUCTION_EVIDENCE_USE_SCHEMA_VERSION)
        return cls(
            stage=str(data.get("stage", "")),
            status=ReconstructionEvidenceUseStatus(str(data.get("status", ""))),
            projection_ids=_string_tuple(data.get("projection_ids")),
            used_at_ns=_strict_int(data.get("used_at_ns"), "used_at_ns"),
            consumed_record_ids=_string_tuple(data.get("consumed_record_ids")),
            reason=str(data.get("reason", "")),
            effects={
                str(name): _json_scalar(value)
                for name, value in _mapping(data.get("effects")).items()
            },
            decision_id=str(data.get("decision_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class EvidenceThresholdResolutionV1:
    """Resolved gap and spread thresholds plus their calculation basis."""

    suspicious_gap_ms: int
    suspicious_gap_basis: str
    wide_spread_threshold: float | None
    wide_spread_basis: str


def resolve_reconstruction_evidence_thresholds(
    *,
    spreads: Iterable[float] = (),
    quality_report: QualityReport | None = None,
    quality_payload: Mapping[str, JSONValue] | None = None,
    fingerprint_payload: Mapping[str, JSONValue] | None = None,
    classification_profile: Mapping[str, JSONValue] | None = None,
    policy: ReconstructionEvidencePolicyV1 | None = None,
) -> EvidenceThresholdResolutionV1:
    """Resolve versioned profile/fingerprint thresholds with explicit fallback."""
    selected = policy or ReconstructionEvidencePolicyV1()
    report_payload = (
        quality_report.to_dict() if quality_report is not None else None
    )
    sources = (
        ("classification_profile", classification_profile),
        ("fingerprint", fingerprint_payload),
        ("quality_payload", quality_payload),
        ("quality_report", report_payload),
    )
    gap: int | None = None
    gap_basis = ""
    for basis, payload in sources:
        value = _first_numeric(payload, "suspicious_gap_ms")
        if value is not None and value > 0 and float(value).is_integer():
            gap = int(value)
            gap_basis = f"{basis}:suspicious_gap_ms"
            break
    if gap is None:
        gap = selected.suspicious_gap_fallback_ms
        gap_basis = "evidence_policy:explicit_fallback"

    wide: float | None = None
    wide_basis = ""
    for basis, payload in sources:
        value = _first_numeric(payload, "wide_spread_threshold")
        if value is not None and value >= 0.0:
            wide = float(value)
            wide_basis = f"{basis}:wide_spread_threshold"
            break
    usable_spreads = tuple(
        float(value)
        for value in spreads
        if isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
        and float(value) >= 0.0
    )
    if wide is None and usable_spreads:
        median = statistics.median(usable_spreads)
        minimum = selected.minimum_wide_spread
        for basis, payload in sources:
            configured = _first_numeric(payload, "minimum_wide_spread")
            if configured is not None and configured >= 0.0:
                minimum = float(configured)
                wide_basis = f"{basis}:minimum_and_multiplier"
                multiplier = _first_numeric(payload, "wide_spread_multiplier")
                if multiplier is None or multiplier <= 1.0:
                    multiplier = selected.wide_spread_multiplier
                wide = max(median * float(multiplier), minimum)
                break
        if wide is None:
            wide = max(median * selected.wide_spread_multiplier, minimum)
            wide_basis = "observed_partition_median:evidence_policy_multiplier"
    if wide is None and selected.minimum_wide_spread > 0.0:
        wide = selected.minimum_wide_spread
        wide_basis = "evidence_policy:explicit_minimum_fallback"
    if wide is None:
        wide_basis = "unavailable:no_positive_spread_baseline"
    return EvidenceThresholdResolutionV1(
        suspicious_gap_ms=gap,
        suspicious_gap_basis=gap_basis,
        wide_spread_threshold=wide,
        wide_spread_basis=wide_basis,
    )


def compile_histdata_point_in_time_evidence(
    events: Sequence[Any],
    *,
    evidence_window_id: str,
    source_partition_id: str,
    source_artifact_id: str,
    source_artifact_sha256: str,
    symbol: str,
    period: str,
    support_start_ns: int,
    support_end_ns: int,
    available_at_ns: int,
    as_of_ns: int,
    information_mode: object,
    policy: ReconstructionEvidencePolicyV1 | None = None,
    quality_report: QualityReport | None = None,
    quality_payload: Mapping[str, JSONValue] | None = None,
    fingerprint_payload: Mapping[str, JSONValue] | None = None,
    classification_profile: Mapping[str, JSONValue] | None = None,
    source_cache_schema_version: str = HISTDATA_LEGACY_CACHE_SCHEMA_VERSION,
    cached_row_evidence: Mapping[int, Mapping[str, JSONScalar]] | None = None,
    cached_row_evidence_complete: bool = False,
) -> PointInTimeEvidenceProjectionV1:
    """Compile one bounded HistData ASCII tick partition/window projection.

    ``events`` may contain any immutable event object exposing
    ``event_time_ns``, ``bid``, ``ask``, and ``source_row_id``.  This adapter is
    provider-specific; all returned contracts are provider-neutral.
    """
    selected = policy or ReconstructionEvidencePolicyV1()
    provider = CURRENT_EVIDENCE_SOURCE_PROVIDER_ID
    if provider not in selected.supported_provider_ids:
        raise ValueError("evidence policy does not support HistData.com")
    mode = ReconstructionEvidenceInformationMode.from_value(information_mode)
    start = _int64(support_start_ns, "support_start_ns")
    end = _int64(support_end_ns, "support_end_ns")
    if end <= start:
        raise ValueError("evidence support interval is empty")
    available = _int64(available_at_ns, "available_at_ns")
    as_of = _int64(as_of_ns, "as_of_ns")
    rows = tuple(_event_values(item) for item in events)
    if any(timestamp < start or timestamp >= end for timestamp, *_ in rows):
        raise ValueError("source event lies outside evidence support")
    row_ids = {row_id for _, row_id, _, _ in rows}
    if len(row_ids) != len(rows):
        raise ValueError("source event row identities are duplicated")
    cache_schema = _required_text(
        source_cache_schema_version, "source_cache_schema_version"
    )
    if cache_schema not in {
        HISTDATA_LEGACY_CACHE_SCHEMA_VERSION,
        HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION,
    }:
        raise ValueError("unsupported HistData source cache evidence schema")
    cached = _normalize_cached_row_evidence(cached_row_evidence)
    cache_evidence_complete = _strict_bool(
        cached_row_evidence_complete, "cached_row_evidence_complete"
    )
    if set(cached) - row_ids:
        raise ValueError("cached row evidence refers outside supplied events")
    if cached and cache_schema != HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION:
        raise ValueError("legacy cache cannot supply enriched row evidence")
    if (
        cache_evidence_complete
        and cache_schema != HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION
    ):
        raise ValueError("legacy cache cannot declare complete row evidence")
    visible_rows = tuple(
        item
        for item in rows
        if mode is ReconstructionEvidenceInformationMode.EX_POST_RECONSTRUCTION
        or item[0] <= as_of
    )
    future_withheld = len(visible_rows) != len(rows)
    external_evidence_visible = (
        mode is ReconstructionEvidenceInformationMode.EX_POST_RECONSTRUCTION
        or available <= as_of
    )
    spreads = tuple(ask - bid for _, _, bid, ask in visible_rows)
    thresholds = resolve_reconstruction_evidence_thresholds(
        spreads=spreads,
        quality_report=(quality_report if external_evidence_visible else None),
        quality_payload=(
            quality_payload if external_evidence_visible else None
        ),
        fingerprint_payload=(
            fingerprint_payload if external_evidence_visible else None
        ),
        classification_profile=(
            classification_profile if external_evidence_visible else None
        ),
        policy=selected,
    )
    common = {
        "source_artifact_id": source_artifact_id,
        "source_artifact_sha256": source_artifact_sha256,
        "source_partition_id": source_partition_id,
        "as_of_ns": as_of,
        "information_mode": mode,
        "symbol": symbol,
    }
    report_payload = _quality_report_payload(
        quality_report if external_evidence_visible else None
    )
    report_common = (
        _external_evidence_common(common, "quality_report", report_payload)
        if report_payload is not None
        else common
    )
    row_records: list[ReconstructionEvidenceRecordV1] = []
    sidecars: list[ReconstructionEvidenceRecordV1] = []
    limitations: list[str] = [
        "lexical_price_precision_unavailable_in_arrow_cache",
        "histdata_tick_volume_is_activity_proxy_not_centralized_volume",
    ]
    if cache_schema == HISTDATA_LEGACY_CACHE_SCHEMA_VERSION:
        limitations.append("legacy_cache_row_quality_columns_unavailable")
    omitted = 0

    def append_row(record: ReconstructionEvidenceRecordV1) -> None:
        nonlocal omitted
        if len(row_records) >= selected.max_row_records:
            omitted += 1
            return
        row_records.append(record)

    def append_sidecar(record: ReconstructionEvidenceRecordV1) -> None:
        nonlocal omitted
        if len(row_records) + len(sidecars) >= selected.max_records:
            omitted += 1
            return
        sidecars.append(record)

    if not rows:
        append_sidecar(
            _record(
                kind=ReconstructionEvidenceKind.UNAVAILABLE,
                metric_id="source_availability",
                value=None,
                rule_id="reconstruction_evidence.source_availability.v1",
                calculation_basis="no_rows_in_partition_window",
                source_grain=ReconstructionEvidenceGrain.PARTITION,
                target_grain=ReconstructionEvidenceGrain.WINDOW,
                support_start_ns=start,
                support_end_ns=end,
                available_at_ns=min(available, as_of),
                projection_method="unavailable_sidecar",
                readiness=ReconstructionEvidenceReadiness.UNAVAILABLE,
                confidence=1.0,
                limitations=("source_partition_window_has_no_rows",),
                **common,
            )
        )
        return PointInTimeEvidenceProjectionV1(
            evidence_window_id=evidence_window_id,
            source_provider_id=provider,
            source_partition_id=source_partition_id,
            source_artifact_id=source_artifact_id,
            source_artifact_sha256=source_artifact_sha256,
            symbol=symbol,
            period=period,
            support_start_ns=start,
            support_end_ns=end,
            available_at_ns=available,
            as_of_ns=as_of,
            information_mode=mode,
            policy_id=selected.policy_id,
            sidecar_records=tuple(sidecars),
            status=ReconstructionEvidenceReadiness.UNAVAILABLE,
            limitations=("source_partition_window_has_no_rows",),
        )

    if future_withheld:
        limitations.append(
            "future_row_and_window_evidence_withheld_in_ex_ante_mode"
        )
    reported_row_counts: Counter[str] = Counter()
    row_timestamps = {row_id: timestamp for timestamp, row_id, *_ in rows}
    if quality_report is not None and external_evidence_visible:
        for finding in quality_report.findings:
            row_record = _quality_finding_row_record(
                finding,
                symbol=symbol,
                period=period,
                support_start_ns=start,
                support_end_ns=end,
                available_at_ns=available,
                row_timestamps=row_timestamps,
                common=report_common,
            )
            if row_record is not None:
                reported_row_counts[row_record.metric_id] += 1
                append_row(row_record)
    duplicate_counts = Counter(item[0] for item in visible_rows)
    previous: tuple[int, int, float, float] | None = None
    stale_run = 1
    burst_run = 1
    cached_match_count = 0
    cached_additional_count = 0
    counts: Counter[str] = Counter()
    for timestamp, row_id, bid, ask in visible_rows:
        spread = ask - bid
        exact_metrics: list[tuple[str, JSONScalar]] = []
        if duplicate_counts[timestamp] > 1:
            exact_metrics.append(("duplicate_timestamp", True))
            counts["duplicate_timestamp"] += 1
        if spread < 0.0:
            exact_metrics.append(("negative_spread", True))
            counts["negative_spread"] += 1
        if spread == 0.0:
            exact_metrics.append(("zero_spread", True))
            counts["zero_spread"] += 1
        if (
            thresholds.wide_spread_threshold is not None
            and spread > thresholds.wide_spread_threshold
        ):
            exact_metrics.append(("wide_spread", True))
            counts["wide_spread"] += 1
        emitted_metric_ids = {metric_id for metric_id, _ in exact_metrics}
        for metric_id, value in exact_metrics:
            append_row(
                _record(
                    kind=ReconstructionEvidenceKind.ROW_FACT,
                    metric_id=metric_id,
                    value=value,
                    rule_id=f"reconstruction_evidence.{metric_id}.v1",
                    calculation_basis="immutable_histdata_arrow_row",
                    source_grain=ReconstructionEvidenceGrain.ROW,
                    target_grain=ReconstructionEvidenceGrain.ROW,
                    support_start_ns=timestamp,
                    support_end_ns=timestamp,
                    available_at_ns=timestamp,
                    projection_method="exact_row_identity",
                    readiness=ReconstructionEvidenceReadiness.READY,
                    confidence=1.0,
                    source_row_id=row_id,
                    **common,
                )
            )
        if previous is not None:
            previous_time, _, previous_bid, previous_ask = previous
            gap = timestamp - previous_time
            if gap < 0:
                counts["non_monotonic_timestamp"] += 1
                emitted_metric_ids.add("non_monotonic_timestamp")
                append_row(
                    _record(
                        kind=ReconstructionEvidenceKind.ROW_FACT,
                        metric_id="non_monotonic_timestamp",
                        value=True,
                        rule_id="reconstruction_evidence.non_monotonic.v1",
                        calculation_basis="immutable_source_row_order",
                        source_grain=ReconstructionEvidenceGrain.ROW,
                        target_grain=ReconstructionEvidenceGrain.ROW,
                        support_start_ns=timestamp,
                        support_end_ns=timestamp,
                        available_at_ns=timestamp,
                        projection_method="exact_row_identity",
                        readiness=ReconstructionEvidenceReadiness.READY,
                        confidence=1.0,
                        source_row_id=row_id,
                        **common,
                    )
                )
            if gap > thresholds.suspicious_gap_ms * 1_000_000:
                counts["suspicious_gap"] += 1
                emitted_metric_ids.add("suspicious_gap")
                append_sidecar(
                    _record(
                        kind=ReconstructionEvidenceKind.INTERVAL_FINDING,
                        metric_id="suspicious_gap_ns",
                        value=gap,
                        rule_id="reconstruction_evidence.suspicious_gap.v1",
                        calculation_basis=thresholds.suspicious_gap_basis,
                        source_grain=ReconstructionEvidenceGrain.INTERVAL,
                        target_grain=ReconstructionEvidenceGrain.INTERVAL,
                        support_start_ns=previous_time,
                        support_end_ns=timestamp,
                        available_at_ns=timestamp,
                        projection_method="interval_sidecar_no_row_flattening",
                        readiness=ReconstructionEvidenceReadiness.READY,
                        confidence=1.0,
                        limitations=(
                            "session_or_expected_closure_requires_calendar_context",
                        ),
                        **common,
                    )
                )
            stale_run = (
                stale_run + 1
                if bid == previous_bid and ask == previous_ask
                else 1
            )
            burst_run = (
                burst_run + 1
                if 0 <= gap <= selected.burst_max_interval_ns
                else 1
            )
            if stale_run == selected.stale_quote_run_length:
                counts["stale_quote_run"] += 1
            if burst_run == selected.burst_run_length:
                counts["burst_run"] += 1
        cached_metrics = cached.get(row_id, {})
        if cache_evidence_complete:
            for metric_id in (
                "duplicate_timestamp",
                "negative_spread",
                "non_monotonic_timestamp",
                "zero_spread",
            ):
                if bool(cached_metrics.get(metric_id, False)) != (
                    metric_id in emitted_metric_ids
                ):
                    raise ValueError(
                        "cached objective row evidence differs from source rows"
                    )
        for metric_id, value in sorted(cached_metrics.items()):
            if value is not True:
                continue
            if metric_id in emitted_metric_ids:
                cached_match_count += 1
                continue
            cached_additional_count += 1
            if metric_id == "suspicious_gap":
                if previous is None:
                    raise ValueError(
                        "cached suspicious-gap evidence lacks a previous row"
                    )
                gap = timestamp - previous[0]
                counts["suspicious_gap"] += 1
                append_sidecar(
                    _record(
                        kind=ReconstructionEvidenceKind.INTERVAL_FINDING,
                        metric_id="suspicious_gap_ns",
                        value=gap,
                        rule_id=(
                            "reconstruction_evidence.cached_suspicious_gap.v1"
                        ),
                        calculation_basis="histdata_enriched_cache_row_flag",
                        source_grain=ReconstructionEvidenceGrain.ROW,
                        target_grain=ReconstructionEvidenceGrain.INTERVAL,
                        support_start_ns=previous[0],
                        support_end_ns=timestamp,
                        available_at_ns=timestamp,
                        projection_method="cached_row_to_interval_sidecar",
                        readiness=ReconstructionEvidenceReadiness.READY,
                        confidence=1.0,
                        source_row_id=row_id,
                        limitations=(
                            "threshold_basis_retained_in_enriched_cache",
                        ),
                        **common,
                    )
                )
                continue
            if metric_id in {
                "duplicate_timestamp",
                "negative_spread",
                "non_monotonic_timestamp",
                "wide_spread",
                "zero_spread",
            }:
                counts[metric_id] += 1
            append_row(
                _record(
                    kind=ReconstructionEvidenceKind.ROW_FACT,
                    metric_id=metric_id,
                    value=True,
                    rule_id=f"reconstruction_evidence.cached_{metric_id}.v1",
                    calculation_basis="histdata_enriched_cache_row_flag",
                    source_grain=ReconstructionEvidenceGrain.ROW,
                    target_grain=ReconstructionEvidenceGrain.ROW,
                    support_start_ns=timestamp,
                    support_end_ns=timestamp,
                    available_at_ns=timestamp,
                    projection_method="exact_cached_row_identity",
                    readiness=ReconstructionEvidenceReadiness.READY,
                    confidence=1.0,
                    source_row_id=row_id,
                    **common,
                )
            )
        previous = (timestamp, row_id, bid, ask)

    visible_start = min((item[0] for item in visible_rows), default=start)
    visible_end = max(
        (item[0] for item in visible_rows), default=min(end, as_of)
    )
    aggregate_available = max(visible_end, available)
    aggregate_allowed = (
        mode is ReconstructionEvidenceInformationMode.EX_POST_RECONSTRUCTION
        or aggregate_available <= as_of
    ) and not future_withheld
    aggregate_specs: list[tuple[str, JSONScalar, str]] = [
        ("row_count", len(visible_rows), "immutable_histdata_arrow_rows"),
        (
            "source_cache_schema_version",
            cache_schema,
            "source_artifact_schema",
        ),
        (
            "cached_row_evidence_complete",
            cache_evidence_complete,
            "source_artifact_schema",
        ),
        (
            "cached_row_evidence_match_count",
            cached_match_count,
            "source_row_reconciliation",
        ),
        (
            "cached_row_evidence_additional_count",
            cached_additional_count,
            "source_row_reconciliation",
        ),
        (
            "suspicious_gap_threshold_ms",
            thresholds.suspicious_gap_ms,
            thresholds.suspicious_gap_basis,
        ),
        (
            "wide_spread_threshold",
            thresholds.wide_spread_threshold,
            thresholds.wide_spread_basis,
        ),
        (
            "duplicate_timestamp_count",
            counts["duplicate_timestamp"],
            "row_facts",
        ),
        ("negative_spread_count", counts["negative_spread"], "row_facts"),
        (
            "non_monotonic_timestamp_count",
            counts["non_monotonic_timestamp"],
            "row_facts",
        ),
        ("zero_spread_count", counts["zero_spread"], "row_facts"),
        ("wide_spread_count", counts["wide_spread"], "row_facts"),
        ("suspicious_gap_count", counts["suspicious_gap"], "interval_findings"),
        (
            "stale_quote_run_count",
            counts["stale_quote_run"],
            "observed_sequence",
        ),
        ("burst_run_count", counts["burst_run"], "observed_sequence"),
    ]
    if aggregate_allowed:
        for metric_id, value, basis in aggregate_specs:
            readiness = (
                ReconstructionEvidenceReadiness.LIMITED
                if value is None
                else ReconstructionEvidenceReadiness.READY
            )
            append_sidecar(
                _record(
                    kind=(
                        ReconstructionEvidenceKind.DEFAULT
                        if "fallback" in basis
                        else ReconstructionEvidenceKind.SERIES_FINGERPRINT
                    ),
                    metric_id=metric_id,
                    value=value,
                    rule_id=f"reconstruction_evidence.{metric_id}.v1",
                    calculation_basis=basis,
                    source_grain=ReconstructionEvidenceGrain.PARTITION,
                    target_grain=ReconstructionEvidenceGrain.WINDOW,
                    support_start_ns=visible_start,
                    support_end_ns=visible_end,
                    available_at_ns=aggregate_available,
                    projection_method="bounded_window_sidecar",
                    readiness=readiness,
                    confidence=(
                        1.0
                        if readiness is ReconstructionEvidenceReadiness.READY
                        else 0.0
                    ),
                    limitations=(
                        ("explicit_policy_fallback",)
                        if "fallback" in basis
                        else ()
                    ),
                    **common,
                )
            )
        for metric_id, count in sorted(reported_row_counts.items()):
            append_sidecar(
                _record(
                    kind=ReconstructionEvidenceKind.ADVISORY,
                    metric_id=f"quality_report.reported_{metric_id}_count",
                    value=count,
                    rule_id="reconstruction_evidence.quality_report.rows.v1",
                    calculation_basis="exact_report_row_locations",
                    source_grain=ReconstructionEvidenceGrain.ROW,
                    target_grain=ReconstructionEvidenceGrain.WINDOW,
                    support_start_ns=visible_start,
                    support_end_ns=visible_end,
                    available_at_ns=aggregate_available,
                    projection_method="bounded_exact_report_count_sidecar",
                    readiness=ReconstructionEvidenceReadiness.READY,
                    confidence=1.0,
                    limitations=(
                        "aggregate_count_not_flattened_onto_other_rows",
                    ),
                    **report_common,
                )
            )
        _append_external_evidence(
            append_sidecar,
            quality_report=quality_report,
            quality_payload=quality_payload,
            fingerprint_payload=fingerprint_payload,
            classification_profile=classification_profile,
            support_start_ns=visible_start,
            support_end_ns=visible_end,
            available_at_ns=aggregate_available,
            common=common,
        )
    else:
        append_sidecar(
            _record(
                kind=ReconstructionEvidenceKind.UNAVAILABLE,
                metric_id="future_aggregate_evidence",
                value=None,
                rule_id="reconstruction_evidence.point_in_time_guard.v1",
                calculation_basis="available_at_exceeds_ex_ante_as_of",
                source_grain=ReconstructionEvidenceGrain.WINDOW,
                target_grain=ReconstructionEvidenceGrain.WINDOW,
                support_start_ns=start,
                support_end_ns=min(end, as_of),
                available_at_ns=as_of,
                projection_method="redacted_unavailable_sidecar",
                readiness=ReconstructionEvidenceReadiness.UNAVAILABLE,
                confidence=1.0,
                limitations=(
                    "future_values_and_future_finding_counts_not_retained",
                ),
                **common,
            )
        )

    status = ReconstructionEvidenceReadiness.READY
    if (
        future_withheld
        or omitted
        or any(
            item.readiness is not ReconstructionEvidenceReadiness.READY
            for item in sidecars
        )
    ):
        status = ReconstructionEvidenceReadiness.LIMITED
    if omitted:
        limitations.append("evidence_records_truncated_by_policy")
    if not any(
        value is not None
        for value in (
            quality_report,
            quality_payload,
            fingerprint_payload,
            classification_profile,
        )
    ):
        limitations.append(
            "external_quality_and_fingerprint_artifacts_not_supplied"
        )
        status = ReconstructionEvidenceReadiness.LIMITED
    return PointInTimeEvidenceProjectionV1(
        evidence_window_id=evidence_window_id,
        source_provider_id=provider,
        source_partition_id=source_partition_id,
        source_artifact_id=source_artifact_id,
        source_artifact_sha256=source_artifact_sha256,
        symbol=symbol,
        period=period,
        support_start_ns=start,
        support_end_ns=end,
        available_at_ns=available,
        as_of_ns=as_of,
        information_mode=mode,
        policy_id=selected.policy_id,
        row_records=tuple(row_records),
        sidecar_records=tuple(sidecars),
        status=status,
        limitations=tuple(limitations),
        omitted_record_count=omitted,
    )


def reconstruction_evidence_use(
    projections: Sequence[PointInTimeEvidenceProjectionV1],
    *,
    stage: str,
    used_at_ns: int,
    policy: ReconstructionEvidencePolicyV1 | None = None,
) -> ReconstructionEvidenceUseV1:
    """Resolve the safe quality score and carving thresholds for one stage."""
    if not projections:
        raise ValueError("evidence use requires projections")
    selected_policy = policy or ReconstructionEvidencePolicyV1()
    if any(
        projection.policy_id != selected_policy.policy_id
        for projection in projections
    ):
        raise ValueError("evidence projections differ from the supplied policy")
    used_at = _int64(used_at_ns, "used_at_ns")
    observed = tuple(
        record
        for projection in projections
        for record in projection.records
        if record.available_at_ns <= used_at and record.as_of_ns <= used_at
    )
    available = tuple(
        record
        for record in observed
        if record.readiness is not ReconstructionEvidenceReadiness.UNAVAILABLE
    )
    hard_metrics = {
        "negative_spread",
        "non_monotonic_timestamp",
        "invalid_row",
        "partial_row",
        "negative_spread_count",
        "non_monotonic_timestamp_count",
        "quality_report.reported_negative_spread_count",
        "quality_report.reported_non_monotonic_timestamp_count",
    }
    hard = tuple(
        record
        for record in available
        if record.metric_id in hard_metrics
        and record.value not in (False, 0, None)
    )
    quality_status_records = tuple(
        record
        for record in available
        if record.metric_id
        in {"quality_report.status", "quality_payload.status"}
    )
    failed_quality = tuple(
        record
        for record in quality_status_records
        if str(record.value).lower() in {"error", "failed", "invalid"}
    )
    warning_quality = tuple(
        record
        for record in quality_status_records
        if str(record.value).lower() in {"warning", "warn", "limited"}
    )
    hard = (*hard, *failed_quality)
    unavailable_source = tuple(
        record
        for record in observed
        if (
            record.metric_id == "source_availability"
            or record.metric_id
            == "quality_report.reported_source_availability_count"
        )
        and (
            record.readiness is ReconstructionEvidenceReadiness.UNAVAILABLE
            or record.value not in (False, 0, None)
        )
    )
    fail_closed_on_unavailable = (
        selected_policy.fail_closed_on_source_unavailable
    )
    consumed: list[ReconstructionEvidenceRecordV1] = []
    effects: dict[str, JSONScalar] = {}
    for metric_id, effect_name in (
        ("suspicious_gap_threshold_ms", "max_anchor_gap_ns"),
        ("wide_spread_threshold", "wide_spread_threshold"),
    ):
        matches = tuple(
            record for record in available if record.metric_id == metric_id
        )
        if matches:
            selected = max(matches, key=lambda item: item.available_at_ns)
            consumed.append(selected)
            if selected.value is not None:
                value = cast(int | float, selected.value)
                effects[effect_name] = (
                    int(value) * 1_000_000
                    if effect_name == "max_anchor_gap_ns"
                    else float(value)
                )
    count_metrics = {
        "negative_spread_count": 4.0,
        "non_monotonic_timestamp_count": 4.0,
        "suspicious_gap_count": 1.0,
        "wide_spread_count": 0.5,
        "duplicate_timestamp_count": 0.25,
        "stale_quote_run_count": 0.25,
    }
    penalty = 0.0
    row_count = 0
    for record in available:
        if record.metric_id == "row_count" and isinstance(record.value, int):
            row_count += record.value
        weight = count_metrics.get(record.metric_id)
        if weight is not None and isinstance(record.value, (int, float)):
            penalty += weight * float(record.value)
            consumed.append(record)
    consumed.extend(quality_status_records)
    effects["quality_warning_count"] = len(warning_quality)
    effects["source_quality_score"] = max(
        0.0,
        1.0
        - penalty / max(1.0, float(row_count))
        - len(warning_quality) * selected_policy.quality_warning_score_penalty,
    )
    if hard or (unavailable_source and fail_closed_on_unavailable):
        consumed.extend(hard)
        consumed.extend(unavailable_source)
        return ReconstructionEvidenceUseV1(
            stage=stage,
            status=ReconstructionEvidenceUseStatus.REFUSED,
            projection_ids=tuple(item.projection_id for item in projections),
            used_at_ns=used_at,
            consumed_record_ids=tuple(item.record_id for item in consumed),
            reason=(
                "point-in-time source evidence is unavailable"
                if unavailable_source and not hard
                else "hard point-in-time source evidence refused the stage"
            ),
            effects=effects,
        )
    if not available:
        return ReconstructionEvidenceUseV1(
            stage=stage,
            status=ReconstructionEvidenceUseStatus.NOT_APPLICABLE,
            projection_ids=tuple(item.projection_id for item in projections),
            used_at_ns=used_at,
            consumed_record_ids=(),
            reason="no evidence record was available at the stage use time",
            effects={"source_quality_score": 1.0},
        )
    return ReconstructionEvidenceUseV1(
        stage=stage,
        status=ReconstructionEvidenceUseStatus.APPLIED,
        projection_ids=tuple(item.projection_id for item in projections),
        used_at_ns=used_at,
        consumed_record_ids=tuple(item.record_id for item in consumed),
        reason="available point-in-time evidence conditioned the stage",
        effects=effects,
    )


def read_reconstruction_evidence_policy(
    path: str | Path,
) -> ReconstructionEvidencePolicyV1:
    """Read a deterministic evidence policy artifact."""
    return ReconstructionEvidencePolicyV1.from_json(
        Path(path).read_text(encoding="utf-8")
    )


def read_point_in_time_evidence_projection(
    path: str | Path,
) -> PointInTimeEvidenceProjectionV1:
    """Read a deterministic point-in-time evidence projection."""
    return PointInTimeEvidenceProjectionV1.from_json(
        Path(path).read_text(encoding="utf-8")
    )


def _append_external_evidence(
    append: Any,
    *,
    quality_report: QualityReport | None,
    quality_payload: Mapping[str, JSONValue] | None,
    fingerprint_payload: Mapping[str, JSONValue] | None,
    classification_profile: Mapping[str, JSONValue] | None,
    support_start_ns: int,
    support_end_ns: int,
    available_at_ns: int,
    common: Mapping[str, Any],
) -> None:
    sources: tuple[tuple[str, Mapping[str, Any] | None], ...] = (
        (
            "quality_report",
            _quality_report_payload(quality_report),
        ),
        ("quality_payload", quality_payload),
        ("fingerprint", fingerprint_payload),
        (
            "classification_profile",
            classification_profile,
        ),
    )
    metric_names = (
        "status",
        "fingerprint_id",
        "expected_session_closure_count",
        "weekend_activity_count",
        "invalid_row_count",
        "partial_row_count",
        "precision_source",
        "calendar_profile_complete",
        "applicable_dynamics_status",
    )
    for source_name, payload in sources:
        if payload is None:
            continue
        source_common = _external_evidence_common(common, source_name, payload)
        emitted = False
        for metric_id in metric_names:
            value = _first_scalar(payload, metric_id)
            if value is None:
                continue
            emitted = True
            append(
                _record(
                    kind=(
                        ReconstructionEvidenceKind.ADVISORY
                        if metric_id in {"status", "applicable_dynamics_status"}
                        else ReconstructionEvidenceKind.SERIES_FINGERPRINT
                    ),
                    metric_id=f"{source_name}.{metric_id}",
                    value=value,
                    rule_id=f"reconstruction_evidence.{source_name}.v1",
                    calculation_basis=f"bounded_projection:{source_name}:{metric_id}",
                    source_grain=ReconstructionEvidenceGrain.PERIOD,
                    target_grain=ReconstructionEvidenceGrain.WINDOW,
                    support_start_ns=support_start_ns,
                    support_end_ns=support_end_ns,
                    available_at_ns=available_at_ns,
                    projection_method="bounded_external_sidecar",
                    readiness=ReconstructionEvidenceReadiness.READY,
                    confidence=1.0,
                    limitations=("aggregate_value_not_flattened_onto_rows",),
                    **source_common,
                )
            )
        if not emitted:
            append(
                _record(
                    kind=ReconstructionEvidenceKind.ADVISORY,
                    metric_id=f"{source_name}.supplied",
                    value=True,
                    rule_id=f"reconstruction_evidence.{source_name}.v1",
                    calculation_basis="supplied_but_no_supported_scalar_metric",
                    source_grain=ReconstructionEvidenceGrain.PERIOD,
                    target_grain=ReconstructionEvidenceGrain.WINDOW,
                    support_start_ns=support_start_ns,
                    support_end_ns=support_end_ns,
                    available_at_ns=available_at_ns,
                    projection_method="bounded_external_sidecar",
                    readiness=ReconstructionEvidenceReadiness.LIMITED,
                    confidence=0.0,
                    limitations=(
                        "unsupported_fields_retained_only_by_artifact_hash",
                    ),
                    **source_common,
                )
            )


def _quality_finding_row_record(
    finding: QualityFinding,
    *,
    symbol: str,
    period: str,
    support_start_ns: int,
    support_end_ns: int,
    available_at_ns: int,
    row_timestamps: Mapping[int, int],
    common: Mapping[str, Any],
) -> ReconstructionEvidenceRecordV1 | None:
    """Project only a report finding with an exact matching row location."""
    if (
        finding.location.row_number is None
        or finding.location.timestamp_utc_ms is None
        or finding.target.symbol.upper() != symbol.upper()
        or finding.target.period != period
    ):
        return None
    normalized = f"{finding.code} {finding.rule_id}".lower()
    metric_id = next(
        (
            metric
            for token, metric in (
                ("duplicate", "duplicate_timestamp"),
                ("non_monotonic", "non_monotonic_timestamp"),
                ("negative_spread", "negative_spread"),
                ("zero_spread", "zero_spread"),
                ("wide_spread", "wide_spread"),
                ("partial", "partial_row"),
                ("invalid", "invalid_row"),
                ("weekend", "weekend_activity"),
                ("session_closed", "session_closed"),
                ("source_unavailable", "source_availability"),
            )
            if token in normalized
        ),
        None,
    )
    if metric_id is None:
        return None
    timestamp_ns = finding.location.timestamp_utc_ms * 1_000_000
    if (
        not support_start_ns <= timestamp_ns < support_end_ns
        or row_timestamps.get(finding.location.row_number) != timestamp_ns
    ):
        return None
    record_available = max(timestamp_ns, available_at_ns)
    if (
        common["information_mode"]
        is ReconstructionEvidenceInformationMode.EX_ANTE_SIMULATION
        and record_available > common["as_of_ns"]
    ):
        return None
    return _record(
        kind=ReconstructionEvidenceKind.ROW_FACT,
        metric_id=metric_id,
        value=True,
        rule_id=finding.rule_id or "quality_report.exact_location.v1",
        calculation_basis="quality_report_exact_row_location",
        source_grain=ReconstructionEvidenceGrain.ROW,
        target_grain=ReconstructionEvidenceGrain.ROW,
        support_start_ns=timestamp_ns,
        support_end_ns=timestamp_ns,
        available_at_ns=record_available,
        projection_method="exact_report_location",
        readiness=ReconstructionEvidenceReadiness.READY,
        confidence=1.0,
        source_row_id=finding.location.row_number,
        limitations=("row_identity_resolved_against_target_source_partition",),
        **common,
    )


def _quality_report_payload(
    quality_report: QualityReport | None,
) -> Mapping[str, Any] | None:
    if quality_report is None:
        return None
    return {
        **quality_report.to_dict(),
        "status": quality_report.status.value,
    }


def _external_evidence_common(
    common: Mapping[str, Any],
    source_name: str,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    encoded = _canonical_json(payload).encode("utf-8")
    digest = hashlib.sha256(encoded).hexdigest()
    return {
        **common,
        "source_artifact_id": (
            f"reconstruction-evidence-input:{source_name}:sha256:{digest}"
        ),
        "source_artifact_sha256": digest,
    }


def _record(**values: Any) -> ReconstructionEvidenceRecordV1:
    return ReconstructionEvidenceRecordV1(**values)


def _event_values(item: Any) -> tuple[int, int, float, float]:
    timestamp = _int64(item.event_time_ns, "event_time_ns")
    row_id = _positive_int(item.source_row_id, "source_row_id")
    bid = _finite_float(item.bid, "bid")
    ask = _finite_float(item.ask, "ask")
    return timestamp, row_id, bid, ask


def _normalize_cached_row_evidence(
    value: Mapping[int, Mapping[str, JSONScalar]] | None,
) -> dict[int, dict[str, bool]]:
    if value is None:
        return {}
    result: dict[int, dict[str, bool]] = {}
    for raw_row_id, raw_metrics in value.items():
        row_id = _positive_int(raw_row_id, "cached evidence row_id")
        metrics: dict[str, bool] = {}
        for raw_metric_id, raw_value in raw_metrics.items():
            metric_id = _required_text(raw_metric_id, "cached metric_id")
            if metric_id not in _CACHED_ROW_METRICS:
                raise ValueError("unsupported cached row evidence metric")
            if type(raw_value) is not bool:
                raise TypeError("cached row evidence values must be boolean")
            if raw_value:
                metrics[metric_id] = True
        if metrics:
            result[row_id] = metrics
    return result


def _first_numeric(value: Mapping[str, Any] | None, key: str) -> float | None:
    selected = _first_scalar(value, key)
    if isinstance(selected, bool) or not isinstance(selected, (int, float)):
        return None
    result = float(selected)
    return result if math.isfinite(result) else None


def _first_scalar(value: Mapping[str, Any] | None, key: str) -> JSONScalar:
    if value is None:
        return None
    pending: list[tuple[Mapping[str, Any], int]] = [(value, 0)]
    visited = 0
    while pending and visited < 4096:
        current, depth = pending.pop(0)
        visited += 1
        if key in current:
            candidate = current[key]
            if (
                isinstance(candidate, (str, int, float, bool))
                or candidate is None
            ):
                return candidate
        if depth >= 8:
            continue
        for nested in current.values():
            if isinstance(nested, Mapping):
                pending.append((nested, depth + 1))
            elif isinstance(nested, Sequence) and not isinstance(
                nested, (str, bytes, bytearray)
            ):
                for item in nested[:256]:
                    if isinstance(item, Mapping):
                        pending.append((item, depth + 1))
    return None


def _stable_id(namespace: str, payload: Mapping[str, JSONValue]) -> str:
    encoded = _canonical_json(payload).encode("utf-8")
    return f"{namespace}:sha256:{hashlib.sha256(encoded).hexdigest()}"


def _canonical_json(value: Mapping[str, JSONValue]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _required_text(value: object, name: str) -> str:
    selected = str(value).strip()
    if not selected:
        raise ValueError(f"{name} is required")
    if len(selected) > MAX_EVIDENCE_TEXT:
        raise ValueError(f"{name} exceeds the v1 text limit")
    return selected


def _sha256(value: object, name: str) -> str:
    selected = _required_text(value, name)
    if len(selected) != 64 or any(
        character not in "0123456789abcdef" for character in selected
    ):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return selected


def _period(value: object) -> str:
    selected = _required_text(value, "period")
    if (
        len(selected) != 6
        or not selected.isdigit()
        or not 1 <= int(selected[4:]) <= 12
    ):
        raise ValueError("period must use YYYYMM")
    return selected


def _normalized_text_tuple(
    values: Iterable[object],
    name: str,
    *,
    maximum: int,
    lowercase: bool = False,
) -> tuple[str, ...]:
    result = tuple(
        sorted(
            {
                (
                    _required_text(item, name).lower()
                    if lowercase
                    else _required_text(item, name)
                )
                for item in values
            }
        )
    )
    if len(result) > maximum:
        raise ValueError(f"{name} exceeds the v1 bound")
    return result


def _strict_int(value: object, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _int64(value: object, name: str) -> int:
    selected = _strict_int(value, name)
    if not -(2**63) <= selected <= 2**63 - 1:
        raise ValueError(f"{name} exceeds signed int64")
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


def _strict_bool(value: object, name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{name} must be boolean")
    return bool(value)


def _json_scalar(value: object) -> JSONScalar:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value):
        return value
    raise ValueError("evidence values must be finite JSON scalars")


def _string_tuple(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError("expected a sequence of strings")
    return tuple(str(item) for item in value)


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a mapping")
    return cast(Mapping[str, Any], value)


def _sequence(value: object) -> Sequence[Any]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError("expected a sequence")
    return value


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        return _mapping(json.loads(text))
    except json.JSONDecodeError as err:
        raise ValueError("invalid reconstruction evidence JSON") from err


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError("unsupported reconstruction evidence schema")


def _require_derived(
    data: Mapping[str, Any], name: str, expected: object
) -> None:
    if data.get(name) != expected:
        raise ValueError(
            f"derived reconstruction evidence field differs: {name}"
        )


__all__ = [
    "CURRENT_EVIDENCE_SOURCE_PROVIDER_ID",
    "DEFAULT_EVIDENCE_SUSPICIOUS_GAP_MS",
    "HISTDATA_ENRICHED_CACHE_SCHEMA_VERSION",
    "HISTDATA_LEGACY_CACHE_SCHEMA_VERSION",
    "RECONSTRUCTION_EVIDENCE_POLICY_ARTIFACT_KIND",
    "RECONSTRUCTION_EVIDENCE_POLICY_SCHEMA_VERSION",
    "RECONSTRUCTION_EVIDENCE_PROJECTION_ARTIFACT_KIND",
    "RECONSTRUCTION_EVIDENCE_PROJECTION_SCHEMA_VERSION",
    "RECONSTRUCTION_EVIDENCE_RECORD_SCHEMA_VERSION",
    "RECONSTRUCTION_EVIDENCE_USE_SCHEMA_VERSION",
    "EvidenceThresholdResolutionV1",
    "PointInTimeEvidenceProjectionV1",
    "ReconstructionEvidenceGrain",
    "ReconstructionEvidenceInformationMode",
    "ReconstructionEvidenceKind",
    "ReconstructionEvidencePolicyV1",
    "ReconstructionEvidenceReadiness",
    "ReconstructionEvidenceRecordV1",
    "ReconstructionEvidenceUseStatus",
    "ReconstructionEvidenceUseV1",
    "compile_histdata_point_in_time_evidence",
    "read_point_in_time_evidence_projection",
    "read_reconstruction_evidence_policy",
    "reconstruction_evidence_use",
    "resolve_reconstruction_evidence_thresholds",
]
