"""Measured campaign resource envelopes and mounted-storage qualification.

Planning estimates are admission hints, not evidence about physical storage or
runtime behavior.  This module independently verifies committed reconstruction
products, measures their physical representation, fits conservative high-tail
envelopes, and binds the resulting campaign forecast to the final adaptive
support map and frozen release candidate.

The contracts deliberately keep destructive storage-fault operations outside
the library.  A disconnect/remount drill is supplied as a strong evidence
artifact and is checked against the measured filesystem identity.  This keeps
the audit reproducible without allowing a package command to unmount operator
storage.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import time
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, cast

from histdatacom.orchestration.reconstruction import (
    artifact_ref_for_file,
    verify_artifact_ref,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.persistence import (
    ReconstructionProductManifestV3,
    verify_reconstruction_publication,
)
from histdatacom.synthetic.release_candidate import (
    ReconstructionReleaseCandidateV1,
    read_reconstruction_release_candidate,
)
from histdatacom.synthetic.support_verification import (
    FinalAdaptiveSupportMapIndexV1,
    read_final_adaptive_support_map_index,
    read_final_support_verification_shard,
)

RESOURCE_RUNTIME_TELEMETRY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-resource-runtime-telemetry.v1"
)
RESOURCE_PROBE_SCHEMA_VERSION = "histdatacom.reconstruction-resource-probe.v1"
RESOURCE_MEASUREMENT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-resource-measurement.v1"
)
RESOURCE_MEASUREMENT_CORPUS_SCHEMA_VERSION = (
    "histdatacom.reconstruction-resource-measurement-corpus.v1"
)
RESOURCE_ENVELOPE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-resource-envelope.v1"
)
CAMPAIGN_RESOURCE_FORECAST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-resource-forecast.v1"
)
CAMPAIGN_RESOURCE_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-resource-policy.v1"
)
PACKING_REVIEW_SCHEMA_VERSION = (
    "histdatacom.reconstruction-physical-packing-review.v1"
)
STORAGE_QUALIFICATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-storage-qualification.v1"
)
CAMPAIGN_RESOURCE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-resource-audit.v1"
)
CAMPAIGN_RESOURCE_AUDIT_SPEC_SCHEMA_VERSION = (
    "histdatacom.reconstruction-campaign-resource-audit-spec.v1"
)

RESOURCE_MEASUREMENT_ARTIFACT_KIND = "reconstruction_resource_measurement_v1"
RESOURCE_MEASUREMENT_CORPUS_ARTIFACT_KIND = (
    "reconstruction_resource_measurement_corpus_v1"
)
STORAGE_QUALIFICATION_ARTIFACT_KIND = "reconstruction_storage_qualification_v1"
CAMPAIGN_RESOURCE_AUDIT_ARTIFACT_KIND = (
    "reconstruction_campaign_resource_audit_v1"
)

MAX_RESOURCE_MEASUREMENTS = 4_096
MAX_RESOURCE_ARTIFACT_BYTES = 8 * 1024**2
DEFAULT_ENVELOPE_QUANTILE = 0.95
_SHA256_RE_LENGTH = 64

REQUIRED_MEASUREMENT_STRATA: Mapping[str, frozenset[str]] = {
    "era": frozenset(
        {
            "early_sparse",
            "feed_transition",
            "crisis_high_activity",
            "modern_dense",
        }
    ),
    "missingness": frozenset({"low", "median", "high"}),
    "alignment": frozenset({"exact", "bounded_nearest"}),
    "deficit": frozenset({"zero", "positive"}),
    "split": frozenset({"deep_recursive", "unsplit_or_shallow"}),
    "member_scope": frozenset({"all_retained"}),
}

TERMINAL_OUTCOMES = frozenset({"success", "refusal", "cancellation", "failure"})
SUCCESS_CLEANUP_STATUS = "committed_scratch_removed"
NON_SUCCESS_CLEANUP_STATUSES = frozenset(
    {
        "refused_scratch_removed",
        "cancelled_scratch_removed",
        "failed_scratch_removed",
    }
)


class ReconstructionResourceAuditError(ValueError):
    """Measured evidence is incomplete, inconsistent, or outside policy."""


@dataclass(frozen=True, slots=True)
class ReconstructionResourceRuntimeTelemetryV1:
    """Measured runtime counters for one representative terminal case."""

    wall_seconds: float
    cpu_seconds: float
    peak_rss_bytes: int
    peak_scratch_bytes: int
    stage_output_bytes: int
    candidate_event_count: int
    poisson_work_units: int
    temporal_history_bytes: int
    checkpoint_bytes: int
    cleanup_status: str
    uncommitted_bytes_after_cleanup: int = 0
    schema_version: str = RESOURCE_RUNTIME_TELEMETRY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RESOURCE_RUNTIME_TELEMETRY_SCHEMA_VERSION
        )
        object.__setattr__(
            self,
            "wall_seconds",
            _positive_float(self.wall_seconds, "wall_seconds"),
        )
        object.__setattr__(
            self,
            "cpu_seconds",
            _nonnegative_float(self.cpu_seconds, "cpu_seconds"),
        )
        for name in (
            "peak_rss_bytes",
            "peak_scratch_bytes",
            "stage_output_bytes",
            "candidate_event_count",
            "poisson_work_units",
            "temporal_history_bytes",
            "checkpoint_bytes",
            "uncommitted_bytes_after_cleanup",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "cleanup_status",
            _required_text(self.cleanup_status, "cleanup_status"),
        )
        if self.uncommitted_bytes_after_cleanup:
            raise ReconstructionResourceAuditError(
                "terminal telemetry retains uncommitted scratch bytes"
            )

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "wall_seconds": self.wall_seconds,
            "cpu_seconds": self.cpu_seconds,
            "peak_rss_bytes": self.peak_rss_bytes,
            "peak_scratch_bytes": self.peak_scratch_bytes,
            "stage_output_bytes": self.stage_output_bytes,
            "candidate_event_count": self.candidate_event_count,
            "poisson_work_units": self.poisson_work_units,
            "temporal_history_bytes": self.temporal_history_bytes,
            "checkpoint_bytes": self.checkpoint_bytes,
            "cleanup_status": self.cleanup_status,
            "uncommitted_bytes_after_cleanup": 0,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionResourceRuntimeTelemetryV1:
        return cls(
            wall_seconds=_strict_float(
                data.get("wall_seconds"), "wall_seconds"
            ),
            cpu_seconds=_strict_float(data.get("cpu_seconds"), "cpu_seconds"),
            peak_rss_bytes=_strict_int(
                data.get("peak_rss_bytes"), "peak_rss_bytes"
            ),
            peak_scratch_bytes=_strict_int(
                data.get("peak_scratch_bytes"), "peak_scratch_bytes"
            ),
            stage_output_bytes=_strict_int(
                data.get("stage_output_bytes"), "stage_output_bytes"
            ),
            candidate_event_count=_strict_int(
                data.get("candidate_event_count"), "candidate_event_count"
            ),
            poisson_work_units=_strict_int(
                data.get("poisson_work_units"), "poisson_work_units"
            ),
            temporal_history_bytes=_strict_int(
                data.get("temporal_history_bytes"), "temporal_history_bytes"
            ),
            checkpoint_bytes=_strict_int(
                data.get("checkpoint_bytes"), "checkpoint_bytes"
            ),
            cleanup_status=str(data.get("cleanup_status", "")),
            uncommitted_bytes_after_cleanup=_strict_int(
                data.get("uncommitted_bytes_after_cleanup", 0),
                "uncommitted_bytes_after_cleanup",
            ),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionResourceProbeV1:
    """One independently measurable product or non-product terminal case."""

    case_id: str
    terminal_outcome: str
    strata: Mapping[str, str]
    telemetry: ReconstructionResourceRuntimeTelemetryV1
    product_manifest_ref: ArtifactRef | None = None
    probe_id: str = ""
    schema_version: str = RESOURCE_PROBE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, RESOURCE_PROBE_SCHEMA_VERSION)
        object.__setattr__(
            self, "case_id", _required_text(self.case_id, "case_id")
        )
        outcome = _required_text(self.terminal_outcome, "terminal_outcome")
        if outcome not in TERMINAL_OUTCOMES:
            raise ReconstructionResourceAuditError(
                "unsupported terminal outcome"
            )
        object.__setattr__(self, "terminal_outcome", outcome)
        strata = _strata(self.strata)
        object.__setattr__(self, "strata", strata)
        if not isinstance(
            self.telemetry, ReconstructionResourceRuntimeTelemetryV1
        ):
            raise TypeError("resource probe telemetry is invalid")
        if outcome == "success":
            if self.product_manifest_ref is None:
                raise ReconstructionResourceAuditError(
                    "successful resource probe requires a product manifest"
                )
            _require_strong_ref(self.product_manifest_ref)
            if self.telemetry.cleanup_status != SUCCESS_CLEANUP_STATUS:
                raise ReconstructionResourceAuditError(
                    "successful resource probe cleanup is not qualified"
                )
        else:
            if self.product_manifest_ref is not None:
                raise ReconstructionResourceAuditError(
                    "non-success resource probe cannot claim a product"
                )
            expected_cleanup = {
                "refusal": "refused_scratch_removed",
                "cancellation": "cancelled_scratch_removed",
                "failure": "failed_scratch_removed",
            }[outcome]
            if self.telemetry.cleanup_status != expected_cleanup:
                raise ReconstructionResourceAuditError(
                    "non-success resource probe cleanup differs"
                )
        expected = _stable_id("reconstruction-resource-probe", self.payload())
        if self.probe_id and self.probe_id != expected:
            raise ReconstructionResourceAuditError(
                "resource probe identity differs"
            )
        object.__setattr__(self, "probe_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "case_id": self.case_id,
            "terminal_outcome": self.terminal_outcome,
            "strata": dict(self.strata),
            "telemetry": self.telemetry.to_dict(),
            "product_manifest_ref": (
                self.product_manifest_ref.to_dict()
                if self.product_manifest_ref is not None
                else None
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "probe_id": self.probe_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionResourceProbeV1:
        ref_data = data.get("product_manifest_ref")
        return cls(
            case_id=str(data.get("case_id", "")),
            terminal_outcome=str(data.get("terminal_outcome", "")),
            strata={
                str(key): str(value)
                for key, value in _mapping(data.get("strata"), "strata").items()
            },
            telemetry=ReconstructionResourceRuntimeTelemetryV1.from_dict(
                _mapping(data.get("telemetry"), "telemetry")
            ),
            product_manifest_ref=(
                None
                if ref_data is None
                else ArtifactRef.from_dict(
                    _mapping(ref_data, "product_manifest_ref")
                )
            ),
            probe_id=str(data.get("probe_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionResourceMeasurementV1:
    """Physical and runtime facts independently measured for one probe."""

    probe_id: str
    case_id: str
    terminal_outcome: str
    strata: Mapping[str, str]
    product_manifest_ref: ArtifactRef | None
    publication_id: str | None
    logical_event_count: int
    observed_event_count: int
    synthetic_event_count: int
    physical_row_count: int
    parquet_bytes: int
    parquet_uncompressed_bytes: int
    manifest_bytes: int
    directory_bytes: int
    inode_count: int
    row_group_count: int
    row_group_occupancy: float
    bytes_per_synthetic_event: float
    bytes_per_logical_event: float
    compression_ratio: float
    verification_read_bytes: int
    verify_wall_seconds: float
    verify_throughput_bytes_per_second: float
    wall_seconds: float
    cpu_seconds: float
    peak_rss_bytes: int
    peak_scratch_bytes: int
    stage_output_bytes: int
    write_amplification: float
    candidate_event_count: int
    candidate_amplification: float
    poisson_work_units: int
    temporal_history_bytes: int
    checkpoint_bytes: int
    cleanup_status: str
    measurement_id: str = ""
    schema_version: str = RESOURCE_MEASUREMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RESOURCE_MEASUREMENT_SCHEMA_VERSION
        )
        for name in (
            "probe_id",
            "case_id",
            "terminal_outcome",
            "cleanup_status",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        if self.terminal_outcome not in TERMINAL_OUTCOMES:
            raise ReconstructionResourceAuditError(
                "measurement outcome is invalid"
            )
        object.__setattr__(self, "strata", _strata(self.strata))
        for name in (
            "logical_event_count",
            "observed_event_count",
            "synthetic_event_count",
            "physical_row_count",
            "parquet_bytes",
            "parquet_uncompressed_bytes",
            "manifest_bytes",
            "directory_bytes",
            "inode_count",
            "row_group_count",
            "verification_read_bytes",
            "peak_rss_bytes",
            "peak_scratch_bytes",
            "stage_output_bytes",
            "candidate_event_count",
            "poisson_work_units",
            "temporal_history_bytes",
            "checkpoint_bytes",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        for name in (
            "row_group_occupancy",
            "bytes_per_synthetic_event",
            "bytes_per_logical_event",
            "compression_ratio",
            "verify_wall_seconds",
            "verify_throughput_bytes_per_second",
            "wall_seconds",
            "cpu_seconds",
            "write_amplification",
            "candidate_amplification",
        ):
            value = _nonnegative_float(getattr(self, name), name)
            object.__setattr__(self, name, round(value, 12))
        if not 0.0 <= self.row_group_occupancy <= 1.0:
            raise ReconstructionResourceAuditError(
                "row-group occupancy is invalid"
            )
        if self.logical_event_count != (
            self.observed_event_count + self.synthetic_event_count
        ):
            raise ReconstructionResourceAuditError(
                "measurement event counts differ"
            )
        if self.terminal_outcome == "success":
            if self.product_manifest_ref is None or self.publication_id is None:
                raise ReconstructionResourceAuditError(
                    "successful measurement lacks product identity"
                )
            _require_strong_ref(self.product_manifest_ref)
            if self.physical_row_count != self.synthetic_event_count:
                raise ReconstructionResourceAuditError(
                    "v3 measurement physical rows differ from synthetic rows"
                )
            if not self.logical_event_count or not self.manifest_bytes:
                raise ReconstructionResourceAuditError(
                    "successful measurement is physically incomplete"
                )
        else:
            if (
                self.product_manifest_ref is not None
                or self.publication_id is not None
            ):
                raise ReconstructionResourceAuditError(
                    "non-success measurement claims a product"
                )
            if any(
                (
                    self.logical_event_count,
                    self.observed_event_count,
                    self.synthetic_event_count,
                    self.parquet_bytes,
                    self.parquet_uncompressed_bytes,
                    self.manifest_bytes,
                    self.directory_bytes,
                    self.physical_row_count,
                    self.inode_count,
                    self.row_group_count,
                    self.verification_read_bytes,
                )
            ):
                raise ReconstructionResourceAuditError(
                    "non-success measurement exposes committed product bytes"
                )
        expected = _stable_id(
            "reconstruction-resource-measurement", self.payload()
        )
        if self.measurement_id and self.measurement_id != expected:
            raise ReconstructionResourceAuditError(
                "measurement identity differs"
            )
        object.__setattr__(self, "measurement_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "probe_id": self.probe_id,
            "case_id": self.case_id,
            "terminal_outcome": self.terminal_outcome,
            "strata": dict(self.strata),
            "product_manifest_ref": (
                self.product_manifest_ref.to_dict()
                if self.product_manifest_ref is not None
                else None
            ),
            "publication_id": self.publication_id,
            "logical_event_count": self.logical_event_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "physical_row_count": self.physical_row_count,
            "parquet_bytes": self.parquet_bytes,
            "parquet_uncompressed_bytes": self.parquet_uncompressed_bytes,
            "manifest_bytes": self.manifest_bytes,
            "directory_bytes": self.directory_bytes,
            "inode_count": self.inode_count,
            "row_group_count": self.row_group_count,
            "row_group_occupancy": self.row_group_occupancy,
            "bytes_per_synthetic_event": self.bytes_per_synthetic_event,
            "bytes_per_logical_event": self.bytes_per_logical_event,
            "compression_ratio": self.compression_ratio,
            "verification_read_bytes": self.verification_read_bytes,
            "verify_wall_seconds": self.verify_wall_seconds,
            "verify_throughput_bytes_per_second": (
                self.verify_throughput_bytes_per_second
            ),
            "wall_seconds": self.wall_seconds,
            "cpu_seconds": self.cpu_seconds,
            "peak_rss_bytes": self.peak_rss_bytes,
            "peak_scratch_bytes": self.peak_scratch_bytes,
            "stage_output_bytes": self.stage_output_bytes,
            "write_amplification": self.write_amplification,
            "candidate_event_count": self.candidate_event_count,
            "candidate_amplification": self.candidate_amplification,
            "poisson_work_units": self.poisson_work_units,
            "temporal_history_bytes": self.temporal_history_bytes,
            "checkpoint_bytes": self.checkpoint_bytes,
            "cleanup_status": self.cleanup_status,
            "measurement_basis": "verified-v3-physical-bytes-and-runtime-receipt-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "measurement_id": self.measurement_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionResourceMeasurementV1:
        _require_derived(
            data,
            "measurement_basis",
            "verified-v3-physical-bytes-and-runtime-receipt-v1",
        )
        ref_data = data.get("product_manifest_ref")
        return cls(
            probe_id=str(data.get("probe_id", "")),
            case_id=str(data.get("case_id", "")),
            terminal_outcome=str(data.get("terminal_outcome", "")),
            strata={
                str(key): str(value)
                for key, value in _mapping(data.get("strata"), "strata").items()
            },
            product_manifest_ref=(
                None
                if ref_data is None
                else ArtifactRef.from_dict(
                    _mapping(ref_data, "product_manifest_ref")
                )
            ),
            publication_id=_optional_text(data.get("publication_id")),
            logical_event_count=_strict_int(
                data.get("logical_event_count"), "logical_event_count"
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count"), "synthetic_event_count"
            ),
            physical_row_count=_strict_int(
                data.get("physical_row_count"), "physical_row_count"
            ),
            parquet_bytes=_strict_int(
                data.get("parquet_bytes"), "parquet_bytes"
            ),
            parquet_uncompressed_bytes=_strict_int(
                data.get("parquet_uncompressed_bytes"),
                "parquet_uncompressed_bytes",
            ),
            manifest_bytes=_strict_int(
                data.get("manifest_bytes"), "manifest_bytes"
            ),
            directory_bytes=_strict_int(
                data.get("directory_bytes"), "directory_bytes"
            ),
            inode_count=_strict_int(data.get("inode_count"), "inode_count"),
            row_group_count=_strict_int(
                data.get("row_group_count"), "row_group_count"
            ),
            row_group_occupancy=_strict_float(
                data.get("row_group_occupancy"), "row_group_occupancy"
            ),
            bytes_per_synthetic_event=_strict_float(
                data.get("bytes_per_synthetic_event"),
                "bytes_per_synthetic_event",
            ),
            bytes_per_logical_event=_strict_float(
                data.get("bytes_per_logical_event"), "bytes_per_logical_event"
            ),
            compression_ratio=_strict_float(
                data.get("compression_ratio"), "compression_ratio"
            ),
            verification_read_bytes=_strict_int(
                data.get("verification_read_bytes"), "verification_read_bytes"
            ),
            verify_wall_seconds=_strict_float(
                data.get("verify_wall_seconds"), "verify_wall_seconds"
            ),
            verify_throughput_bytes_per_second=_strict_float(
                data.get("verify_throughput_bytes_per_second"),
                "verify_throughput_bytes_per_second",
            ),
            wall_seconds=_strict_float(
                data.get("wall_seconds"), "wall_seconds"
            ),
            cpu_seconds=_strict_float(data.get("cpu_seconds"), "cpu_seconds"),
            peak_rss_bytes=_strict_int(
                data.get("peak_rss_bytes"), "peak_rss_bytes"
            ),
            peak_scratch_bytes=_strict_int(
                data.get("peak_scratch_bytes"), "peak_scratch_bytes"
            ),
            stage_output_bytes=_strict_int(
                data.get("stage_output_bytes"), "stage_output_bytes"
            ),
            write_amplification=_strict_float(
                data.get("write_amplification"), "write_amplification"
            ),
            candidate_event_count=_strict_int(
                data.get("candidate_event_count"), "candidate_event_count"
            ),
            candidate_amplification=_strict_float(
                data.get("candidate_amplification"), "candidate_amplification"
            ),
            poisson_work_units=_strict_int(
                data.get("poisson_work_units"), "poisson_work_units"
            ),
            temporal_history_bytes=_strict_int(
                data.get("temporal_history_bytes"), "temporal_history_bytes"
            ),
            checkpoint_bytes=_strict_int(
                data.get("checkpoint_bytes"), "checkpoint_bytes"
            ),
            cleanup_status=str(data.get("cleanup_status", "")),
            measurement_id=str(data.get("measurement_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionResourceMeasurementCorpusV1:
    """Full stratified measurement census used for envelope fitting."""

    measurements: tuple[ReconstructionResourceMeasurementV1, ...]
    stratum_counts: Mapping[str, int] = field(default_factory=dict)
    terminal_counts: Mapping[str, int] = field(default_factory=dict)
    corpus_id: str = ""
    schema_version: str = RESOURCE_MEASUREMENT_CORPUS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, RESOURCE_MEASUREMENT_CORPUS_SCHEMA_VERSION
        )
        measurements = tuple(
            sorted(self.measurements, key=lambda item: item.case_id)
        )
        if not measurements or len(measurements) > MAX_RESOURCE_MEASUREMENTS:
            raise ReconstructionResourceAuditError(
                "resource measurement corpus size is invalid"
            )
        if len({item.measurement_id for item in measurements}) != len(
            measurements
        ):
            raise ReconstructionResourceAuditError(
                "resource measurement corpus contains duplicates"
            )
        object.__setattr__(self, "measurements", measurements)
        strata_counter = Counter(
            f"{axis}:{value}"
            for item in measurements
            for axis, value in item.strata.items()
        )
        terminal_counter = Counter(
            item.terminal_outcome for item in measurements
        )
        if set(terminal_counter) != set(TERMINAL_OUTCOMES):
            raise ReconstructionResourceAuditError(
                "resource corpus lacks success/refusal/cancellation/failure cleanup"
            )
        for axis, required_values in REQUIRED_MEASUREMENT_STRATA.items():
            observed = {
                item.strata[axis]
                for item in measurements
                if axis in item.strata
            }
            missing = required_values.difference(observed)
            if missing:
                raise ReconstructionResourceAuditError(
                    f"resource corpus lacks {axis} strata: {', '.join(sorted(missing))}"
                )
        supplied_strata = dict(self.stratum_counts)
        if supplied_strata and supplied_strata != dict(
            sorted(strata_counter.items())
        ):
            raise ReconstructionResourceAuditError(
                "resource corpus strata differ"
            )
        supplied_terminal = dict(self.terminal_counts)
        if supplied_terminal and supplied_terminal != dict(
            sorted(terminal_counter.items())
        ):
            raise ReconstructionResourceAuditError(
                "resource terminal counts differ"
            )
        object.__setattr__(
            self, "stratum_counts", dict(sorted(strata_counter.items()))
        )
        object.__setattr__(
            self, "terminal_counts", dict(sorted(terminal_counter.items()))
        )
        expected = _stable_id("reconstruction-resource-corpus", self.payload())
        if self.corpus_id and self.corpus_id != expected:
            raise ReconstructionResourceAuditError(
                "resource corpus identity differs"
            )
        object.__setattr__(self, "corpus_id", expected)
        _ensure_size(self.to_dict(), "resource measurement corpus")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "measurements": [item.to_dict() for item in self.measurements],
            "stratum_counts": dict(self.stratum_counts),
            "terminal_counts": dict(self.terminal_counts),
            "aggregate_workload": _aggregate_workload(self.measurements),
            "event_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "corpus_id": self.corpus_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionResourceMeasurementCorpusV1:
        _require_derived(data, "event_rows_inline", False)
        corpus = cls(
            measurements=tuple(
                ReconstructionResourceMeasurementV1.from_dict(
                    _mapping(item, "measurement")
                )
                for item in _sequence(data.get("measurements"), "measurements")
            ),
            stratum_counts=_int_mapping(
                data.get("stratum_counts"), "stratum_counts"
            ),
            terminal_counts=_int_mapping(
                data.get("terminal_counts"), "terminal_counts"
            ),
            corpus_id=str(data.get("corpus_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(
            data,
            "aggregate_workload",
            _aggregate_workload(corpus.measurements),
        )
        return corpus


@dataclass(frozen=True, slots=True)
class ReconstructionResourceEnvelopeV1:
    """One conservative nearest-rank high-tail planning envelope."""

    metric: str
    quantile: float
    high_quantile_value: float
    maximum_value: float
    minimum_value: float
    high_quantile_absolute_residual: float
    maximum_absolute_residual: float
    maximum_positive_residual: float
    sample_count: int
    extrapolation_limit_factor: float
    basis: str
    envelope_id: str = ""
    schema_version: str = RESOURCE_ENVELOPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, RESOURCE_ENVELOPE_SCHEMA_VERSION)
        object.__setattr__(
            self, "metric", _required_text(self.metric, "metric")
        )
        quantile = _strict_float(self.quantile, "quantile")
        if not 0.5 <= quantile < 1.0:
            raise ReconstructionResourceAuditError(
                "envelope quantile is invalid"
            )
        object.__setattr__(self, "quantile", quantile)
        for name in (
            "high_quantile_value",
            "maximum_value",
            "minimum_value",
            "high_quantile_absolute_residual",
            "maximum_absolute_residual",
            "maximum_positive_residual",
        ):
            object.__setattr__(
                self, name, _nonnegative_float(getattr(self, name), name)
            )
        if (
            not self.minimum_value
            <= self.high_quantile_value
            <= self.maximum_value
        ):
            raise ReconstructionResourceAuditError(
                "resource envelope ordering differs"
            )
        expected_maximum_residual = max(
            self.high_quantile_value - self.minimum_value,
            self.maximum_value - self.high_quantile_value,
        )
        expected_positive_residual = (
            self.maximum_value - self.high_quantile_value
        )
        if not math.isclose(
            self.maximum_absolute_residual, expected_maximum_residual
        ) or not math.isclose(
            self.maximum_positive_residual, expected_positive_residual
        ):
            raise ReconstructionResourceAuditError(
                "resource envelope residual bounds differ"
            )
        if (
            self.high_quantile_absolute_residual
            > self.maximum_absolute_residual
        ):
            raise ReconstructionResourceAuditError(
                "resource envelope residual quantile exceeds maximum"
            )
        object.__setattr__(
            self,
            "sample_count",
            _positive_int(self.sample_count, "sample_count"),
        )
        factor = _strict_float(
            self.extrapolation_limit_factor, "extrapolation_limit_factor"
        )
        if factor < 1.0:
            raise ReconstructionResourceAuditError(
                "resource envelope extrapolation factor is invalid"
            )
        object.__setattr__(self, "extrapolation_limit_factor", factor)
        object.__setattr__(self, "basis", _required_text(self.basis, "basis"))
        expected = _stable_id(
            "reconstruction-resource-envelope", self.payload()
        )
        if self.envelope_id and self.envelope_id != expected:
            raise ReconstructionResourceAuditError(
                "resource envelope identity differs"
            )
        object.__setattr__(self, "envelope_id", expected)

    @property
    def conservative_upper_bound(self) -> float:
        """Return the fitted high tail plus its worst observed positive residual."""
        return self.high_quantile_value + self.maximum_positive_residual

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "metric": self.metric,
            "quantile": self.quantile,
            "high_quantile_value": self.high_quantile_value,
            "maximum_value": self.maximum_value,
            "minimum_value": self.minimum_value,
            "high_quantile_absolute_residual": (
                self.high_quantile_absolute_residual
            ),
            "maximum_absolute_residual": self.maximum_absolute_residual,
            "maximum_positive_residual": self.maximum_positive_residual,
            "sample_count": self.sample_count,
            "extrapolation_limit_factor": self.extrapolation_limit_factor,
            "basis": self.basis,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "envelope_id": self.envelope_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionResourceEnvelopeV1:
        return cls(
            metric=str(data.get("metric", "")),
            quantile=_strict_float(data.get("quantile"), "quantile"),
            high_quantile_value=_strict_float(
                data.get("high_quantile_value"), "high_quantile_value"
            ),
            maximum_value=_strict_float(
                data.get("maximum_value"), "maximum_value"
            ),
            minimum_value=_strict_float(
                data.get("minimum_value"), "minimum_value"
            ),
            high_quantile_absolute_residual=_strict_float(
                data.get("high_quantile_absolute_residual"),
                "high_quantile_absolute_residual",
            ),
            maximum_absolute_residual=_strict_float(
                data.get("maximum_absolute_residual"),
                "maximum_absolute_residual",
            ),
            maximum_positive_residual=_strict_float(
                data.get("maximum_positive_residual"),
                "maximum_positive_residual",
            ),
            sample_count=_strict_int(data.get("sample_count"), "sample_count"),
            extrapolation_limit_factor=_strict_float(
                data.get("extrapolation_limit_factor"),
                "extrapolation_limit_factor",
            ),
            basis=str(data.get("basis", "")),
            envelope_id=str(data.get("envelope_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CampaignResourceForecastV1:
    """Measured high-tail forecast for the exact final support rectangle."""

    final_support_map_id: str
    product_count: int
    logical_event_count: int
    observed_event_count: int
    synthetic_event_count: int
    candidate_event_count: int
    output_bytes_lower: int
    output_bytes_upper: int
    peak_scratch_bytes_per_worker: int
    peak_rss_bytes_per_worker: int
    inode_count_upper: int
    temporal_history_bytes_upper: int
    checkpoint_bytes_upper: int
    poisson_work_units_upper: int
    verification_read_bytes_lower: int
    verification_read_bytes_upper: int
    verify_seconds_lower: float
    verify_seconds_upper: float
    campaign_seconds_lower: float
    campaign_seconds_upper: float
    campaign_cpu_seconds_lower: float
    campaign_cpu_seconds_upper: float
    write_amplification_upper: float
    candidate_amplification_upper: float
    extrapolation_factor: float
    forecast_id: str = ""
    schema_version: str = CAMPAIGN_RESOURCE_FORECAST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, CAMPAIGN_RESOURCE_FORECAST_SCHEMA_VERSION
        )
        object.__setattr__(
            self,
            "final_support_map_id",
            _required_text(self.final_support_map_id, "final_support_map_id"),
        )
        for name in (
            "product_count",
            "logical_event_count",
            "observed_event_count",
            "synthetic_event_count",
            "candidate_event_count",
            "output_bytes_lower",
            "output_bytes_upper",
            "peak_scratch_bytes_per_worker",
            "peak_rss_bytes_per_worker",
            "inode_count_upper",
            "temporal_history_bytes_upper",
            "checkpoint_bytes_upper",
            "poisson_work_units_upper",
            "verification_read_bytes_lower",
            "verification_read_bytes_upper",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        if self.logical_event_count != (
            self.observed_event_count + self.synthetic_event_count
        ):
            raise ReconstructionResourceAuditError(
                "forecast event counts differ"
            )
        if self.output_bytes_lower > self.output_bytes_upper:
            raise ReconstructionResourceAuditError(
                "forecast output bounds differ"
            )
        if (
            self.verification_read_bytes_lower
            > self.verification_read_bytes_upper
        ):
            raise ReconstructionResourceAuditError(
                "forecast verification-read bounds differ"
            )
        for lower, upper, name in (
            (
                self.verify_seconds_lower,
                self.verify_seconds_upper,
                "verify_seconds",
            ),
            (
                self.campaign_seconds_lower,
                self.campaign_seconds_upper,
                "campaign_seconds",
            ),
            (
                self.campaign_cpu_seconds_lower,
                self.campaign_cpu_seconds_upper,
                "campaign_cpu_seconds",
            ),
        ):
            lower_value = _nonnegative_float(lower, f"{name}_lower")
            upper_value = _nonnegative_float(upper, f"{name}_upper")
            if lower_value > upper_value:
                raise ReconstructionResourceAuditError(f"{name} bounds differ")
        for name in (
            "write_amplification_upper",
            "candidate_amplification_upper",
        ):
            value = _nonnegative_float(getattr(self, name), name)
            object.__setattr__(self, name, round(value, 12))
        factor = _nonnegative_float(
            self.extrapolation_factor, "extrapolation_factor"
        )
        if factor < 1.0:
            raise ReconstructionResourceAuditError(
                "forecast extrapolation is invalid"
            )
        object.__setattr__(self, "extrapolation_factor", round(factor, 12))
        expected = _stable_id(
            "reconstruction-resource-forecast", self.payload()
        )
        if self.forecast_id and self.forecast_id != expected:
            raise ReconstructionResourceAuditError(
                "resource forecast identity differs"
            )
        object.__setattr__(self, "forecast_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "final_support_map_id": self.final_support_map_id,
            "product_count": self.product_count,
            "logical_event_count": self.logical_event_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "candidate_event_count": self.candidate_event_count,
            "output_bytes_lower": self.output_bytes_lower,
            "output_bytes_upper": self.output_bytes_upper,
            "peak_scratch_bytes_per_worker": self.peak_scratch_bytes_per_worker,
            "peak_rss_bytes_per_worker": self.peak_rss_bytes_per_worker,
            "inode_count_upper": self.inode_count_upper,
            "temporal_history_bytes_upper": self.temporal_history_bytes_upper,
            "checkpoint_bytes_upper": self.checkpoint_bytes_upper,
            "poisson_work_units_upper": self.poisson_work_units_upper,
            "verification_read_bytes_lower": self.verification_read_bytes_lower,
            "verification_read_bytes_upper": self.verification_read_bytes_upper,
            "verify_seconds_lower": round(self.verify_seconds_lower, 6),
            "verify_seconds_upper": round(self.verify_seconds_upper, 6),
            "campaign_seconds_lower": round(self.campaign_seconds_lower, 6),
            "campaign_seconds_upper": round(self.campaign_seconds_upper, 6),
            "campaign_cpu_seconds_lower": round(
                self.campaign_cpu_seconds_lower, 6
            ),
            "campaign_cpu_seconds_upper": round(
                self.campaign_cpu_seconds_upper, 6
            ),
            "write_amplification_upper": self.write_amplification_upper,
            "candidate_amplification_upper": self.candidate_amplification_upper,
            "extrapolation_factor": self.extrapolation_factor,
            "forecast_basis": "nearest-rank-high-quantile-plus-observed-max-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "forecast_id": self.forecast_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CampaignResourceForecastV1:
        _require_derived(
            data,
            "forecast_basis",
            "nearest-rank-high-quantile-plus-observed-max-v1",
        )
        return cls(
            final_support_map_id=str(data.get("final_support_map_id", "")),
            product_count=_strict_int(
                data.get("product_count"), "product_count"
            ),
            logical_event_count=_strict_int(
                data.get("logical_event_count"), "logical_event_count"
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count"), "synthetic_event_count"
            ),
            candidate_event_count=_strict_int(
                data.get("candidate_event_count"), "candidate_event_count"
            ),
            output_bytes_lower=_strict_int(
                data.get("output_bytes_lower"), "output_bytes_lower"
            ),
            output_bytes_upper=_strict_int(
                data.get("output_bytes_upper"), "output_bytes_upper"
            ),
            peak_scratch_bytes_per_worker=_strict_int(
                data.get("peak_scratch_bytes_per_worker"),
                "peak_scratch_bytes_per_worker",
            ),
            peak_rss_bytes_per_worker=_strict_int(
                data.get("peak_rss_bytes_per_worker"),
                "peak_rss_bytes_per_worker",
            ),
            inode_count_upper=_strict_int(
                data.get("inode_count_upper"), "inode_count_upper"
            ),
            temporal_history_bytes_upper=_strict_int(
                data.get("temporal_history_bytes_upper"),
                "temporal_history_bytes_upper",
            ),
            checkpoint_bytes_upper=_strict_int(
                data.get("checkpoint_bytes_upper"), "checkpoint_bytes_upper"
            ),
            poisson_work_units_upper=_strict_int(
                data.get("poisson_work_units_upper"), "poisson_work_units_upper"
            ),
            verification_read_bytes_lower=_strict_int(
                data.get("verification_read_bytes_lower"),
                "verification_read_bytes_lower",
            ),
            verification_read_bytes_upper=_strict_int(
                data.get("verification_read_bytes_upper"),
                "verification_read_bytes_upper",
            ),
            verify_seconds_lower=_strict_float(
                data.get("verify_seconds_lower"), "verify_seconds_lower"
            ),
            verify_seconds_upper=_strict_float(
                data.get("verify_seconds_upper"), "verify_seconds_upper"
            ),
            campaign_seconds_lower=_strict_float(
                data.get("campaign_seconds_lower"), "campaign_seconds_lower"
            ),
            campaign_seconds_upper=_strict_float(
                data.get("campaign_seconds_upper"), "campaign_seconds_upper"
            ),
            campaign_cpu_seconds_lower=_strict_float(
                data.get("campaign_cpu_seconds_lower"),
                "campaign_cpu_seconds_lower",
            ),
            campaign_cpu_seconds_upper=_strict_float(
                data.get("campaign_cpu_seconds_upper"),
                "campaign_cpu_seconds_upper",
            ),
            write_amplification_upper=_strict_float(
                data.get("write_amplification_upper"),
                "write_amplification_upper",
            ),
            candidate_amplification_upper=_strict_float(
                data.get("candidate_amplification_upper"),
                "candidate_amplification_upper",
            ),
            extrapolation_factor=_strict_float(
                data.get("extrapolation_factor"), "extrapolation_factor"
            ),
            forecast_id=str(data.get("forecast_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CampaignResourcePolicyV1:
    """Frozen capacity reserve, concurrency, and immutable shard policy."""

    available_memory_bytes: int
    available_scratch_bytes: int
    available_output_bytes: int
    available_inodes: int
    maximum_campaign_seconds: float
    reserve_fraction: float
    maximum_container_bytes: int
    maximum_products_per_container: int
    concurrency: int
    products_per_shard: int
    maximum_write_amplification: float
    maximum_candidate_amplification: float
    status: str
    refusal_reasons: tuple[str, ...] = ()
    policy_id: str = ""
    schema_version: str = CAMPAIGN_RESOURCE_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, CAMPAIGN_RESOURCE_POLICY_SCHEMA_VERSION
        )
        for name in (
            "available_memory_bytes",
            "available_scratch_bytes",
            "available_output_bytes",
            "available_inodes",
            "maximum_container_bytes",
            "maximum_products_per_container",
            "concurrency",
            "products_per_shard",
        ):
            object.__setattr__(
                self, name, _positive_int(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "maximum_campaign_seconds",
            _positive_float(
                self.maximum_campaign_seconds, "maximum_campaign_seconds"
            ),
        )
        reserve = _strict_float(self.reserve_fraction, "reserve_fraction")
        if not 0.05 <= reserve <= 0.75:
            raise ReconstructionResourceAuditError(
                "capacity reserve is invalid"
            )
        object.__setattr__(self, "reserve_fraction", reserve)
        for name in (
            "maximum_write_amplification",
            "maximum_candidate_amplification",
        ):
            value = _nonnegative_float(getattr(self, name), name)
            object.__setattr__(self, name, round(value, 12))
        status = _required_text(self.status, "status")
        if status not in {"admitted", "refused"}:
            raise ReconstructionResourceAuditError(
                "resource policy status is invalid"
            )
        reasons = tuple(
            sorted(
                {
                    _required_text(item, "refusal_reason")
                    for item in self.refusal_reasons
                }
            )
        )
        if (status == "refused") != bool(reasons):
            raise ReconstructionResourceAuditError(
                "resource policy reasons differ"
            )
        object.__setattr__(self, "status", status)
        object.__setattr__(self, "refusal_reasons", reasons)
        expected = _stable_id("reconstruction-resource-policy", self.payload())
        if self.policy_id and self.policy_id != expected:
            raise ReconstructionResourceAuditError(
                "resource policy identity differs"
            )
        object.__setattr__(self, "policy_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "available_memory_bytes": self.available_memory_bytes,
            "available_scratch_bytes": self.available_scratch_bytes,
            "available_output_bytes": self.available_output_bytes,
            "available_inodes": self.available_inodes,
            "maximum_campaign_seconds": self.maximum_campaign_seconds,
            "reserve_fraction": self.reserve_fraction,
            "maximum_container_bytes": self.maximum_container_bytes,
            "maximum_products_per_container": self.maximum_products_per_container,
            "concurrency": self.concurrency,
            "products_per_shard": self.products_per_shard,
            "maximum_write_amplification": self.maximum_write_amplification,
            "maximum_candidate_amplification": (
                self.maximum_candidate_amplification
            ),
            "status": self.status,
            "refusal_reasons": list(self.refusal_reasons),
            "reserve_basis": "capacity-minus-recovery-and-verification-reserve-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CampaignResourcePolicyV1:
        _require_derived(
            data,
            "reserve_basis",
            "capacity-minus-recovery-and-verification-reserve-v1",
        )
        return cls(
            available_memory_bytes=_strict_int(
                data.get("available_memory_bytes"), "available_memory_bytes"
            ),
            available_scratch_bytes=_strict_int(
                data.get("available_scratch_bytes"), "available_scratch_bytes"
            ),
            available_output_bytes=_strict_int(
                data.get("available_output_bytes"), "available_output_bytes"
            ),
            available_inodes=_strict_int(
                data.get("available_inodes"), "available_inodes"
            ),
            maximum_campaign_seconds=_strict_float(
                data.get("maximum_campaign_seconds"), "maximum_campaign_seconds"
            ),
            reserve_fraction=_strict_float(
                data.get("reserve_fraction"), "reserve_fraction"
            ),
            maximum_container_bytes=_strict_int(
                data.get("maximum_container_bytes"), "maximum_container_bytes"
            ),
            maximum_products_per_container=_strict_int(
                data.get("maximum_products_per_container"),
                "maximum_products_per_container",
            ),
            concurrency=_strict_int(data.get("concurrency"), "concurrency"),
            products_per_shard=_strict_int(
                data.get("products_per_shard"), "products_per_shard"
            ),
            maximum_write_amplification=_strict_float(
                data.get("maximum_write_amplification"),
                "maximum_write_amplification",
            ),
            maximum_candidate_amplification=_strict_float(
                data.get("maximum_candidate_amplification"),
                "maximum_candidate_amplification",
            ),
            status=str(data.get("status", "")),
            refusal_reasons=tuple(
                str(item)
                for item in _sequence(
                    data.get("refusal_reasons"), "refusal_reasons"
                )
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionPhysicalPackingReviewV1:
    """Bounded immutable-container decision without changing product identity."""

    decision: str
    median_product_bytes: int
    small_product_fraction: float
    maximum_container_bytes: int
    maximum_products_per_container: int
    per_window_identity_preserved: bool = True
    independent_replay_preserved: bool = True
    atomic_publication_preserved: bool = True
    bounded_corruption_blast_radius: bool = True
    product_index_lookup_preserved: bool = True
    observed_anchors_not_duplicated: bool = True
    partition_sensitivity_evidence_available: bool = False
    review_id: str = ""
    schema_version: str = PACKING_REVIEW_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, PACKING_REVIEW_SCHEMA_VERSION)
        decision = _required_text(self.decision, "decision")
        if decision not in {
            "retain_per_product",
            "bounded_immutable_containers",
        }:
            raise ReconstructionResourceAuditError(
                "packing decision is invalid"
            )
        object.__setattr__(self, "decision", decision)
        for name in (
            "median_product_bytes",
            "maximum_container_bytes",
            "maximum_products_per_container",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        fraction = _strict_float(
            self.small_product_fraction, "small_product_fraction"
        )
        if not 0.0 <= fraction <= 1.0:
            raise ReconstructionResourceAuditError(
                "small-product fraction is invalid"
            )
        object.__setattr__(self, "small_product_fraction", fraction)
        required = (
            self.per_window_identity_preserved,
            self.independent_replay_preserved,
            self.atomic_publication_preserved,
            self.bounded_corruption_blast_radius,
            self.product_index_lookup_preserved,
            self.observed_anchors_not_duplicated,
        )
        if not all(item is True for item in required):
            raise ReconstructionResourceAuditError(
                "packing review weakens invariants"
            )
        if decision == "bounded_immutable_containers" and not (
            self.partition_sensitivity_evidence_available
        ):
            raise ReconstructionResourceAuditError(
                "container packing lacks partition-sensitivity evidence"
            )
        expected = _stable_id("reconstruction-packing-review", self.payload())
        if self.review_id and self.review_id != expected:
            raise ReconstructionResourceAuditError(
                "packing review identity differs"
            )
        object.__setattr__(self, "review_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "decision": self.decision,
            "median_product_bytes": self.median_product_bytes,
            "small_product_fraction": self.small_product_fraction,
            "maximum_container_bytes": self.maximum_container_bytes,
            "maximum_products_per_container": self.maximum_products_per_container,
            "per_window_identity_preserved": True,
            "independent_replay_preserved": True,
            "atomic_publication_preserved": True,
            "bounded_corruption_blast_radius": True,
            "product_index_lookup_preserved": True,
            "observed_anchors_not_duplicated": True,
            "partition_sensitivity_evidence_available": (
                self.partition_sensitivity_evidence_available
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "review_id": self.review_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionPhysicalPackingReviewV1:
        return cls(
            decision=str(data.get("decision", "")),
            median_product_bytes=_strict_int(
                data.get("median_product_bytes"), "median_product_bytes"
            ),
            small_product_fraction=_strict_float(
                data.get("small_product_fraction"), "small_product_fraction"
            ),
            maximum_container_bytes=_strict_int(
                data.get("maximum_container_bytes"), "maximum_container_bytes"
            ),
            maximum_products_per_container=_strict_int(
                data.get("maximum_products_per_container"),
                "maximum_products_per_container",
            ),
            per_window_identity_preserved=data.get(
                "per_window_identity_preserved"
            )
            is True,
            independent_replay_preserved=data.get(
                "independent_replay_preserved"
            )
            is True,
            atomic_publication_preserved=data.get(
                "atomic_publication_preserved"
            )
            is True,
            bounded_corruption_blast_radius=data.get(
                "bounded_corruption_blast_radius"
            )
            is True,
            product_index_lookup_preserved=data.get(
                "product_index_lookup_preserved"
            )
            is True,
            observed_anchors_not_duplicated=data.get(
                "observed_anchors_not_duplicated"
            )
            is True,
            partition_sensitivity_evidence_available=data.get(
                "partition_sensitivity_evidence_available"
            )
            is True,
            review_id=str(data.get("review_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionStorageQualificationV1:
    """Mounted-storage integrity, remount, disconnect, and no-fallback evidence."""

    output_root: str
    scratch_root: str
    filesystem_id: str
    device_id: str
    remounted_filesystem_id: str
    remounted_device_id: str
    sustained_test_bytes: int
    write_throughput_bytes_per_second: float
    read_throughput_bytes_per_second: float
    sentinel_sha256_before: str
    sentinel_sha256_after: str
    qualification_evidence_ref: ArtifactRef
    disconnect_evidence_ref: ArtifactRef
    same_filesystem: bool
    non_sparse_write_verified: bool
    disconnect_failed_closed: bool
    local_fallback_absent: bool
    remount_hash_verified: bool
    success_cleanup_verified: bool
    refusal_cleanup_verified: bool
    cancellation_cleanup_verified: bool
    failure_cleanup_verified: bool
    qualified_at_utc: str
    qualification_id: str = ""
    schema_version: str = STORAGE_QUALIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, STORAGE_QUALIFICATION_SCHEMA_VERSION
        )
        output = _absolute_path(self.output_root, "output_root")
        scratch = _absolute_path(self.scratch_root, "scratch_root")
        if (
            output == scratch
            or output.is_relative_to(scratch)
            or scratch.is_relative_to(output)
        ):
            raise ReconstructionResourceAuditError("storage roots overlap")
        object.__setattr__(self, "output_root", str(output))
        object.__setattr__(self, "scratch_root", str(scratch))
        for name in (
            "filesystem_id",
            "device_id",
            "remounted_filesystem_id",
            "remounted_device_id",
            "qualified_at_utc",
        ):
            object.__setattr__(
                self, name, _required_text(getattr(self, name), name)
            )
        object.__setattr__(
            self,
            "sustained_test_bytes",
            _positive_int(self.sustained_test_bytes, "sustained_test_bytes"),
        )
        for name in (
            "write_throughput_bytes_per_second",
            "read_throughput_bytes_per_second",
        ):
            object.__setattr__(
                self, name, _positive_float(getattr(self, name), name)
            )
        for name in ("sentinel_sha256_before", "sentinel_sha256_after"):
            object.__setattr__(self, name, _sha256(getattr(self, name), name))
        if self.sentinel_sha256_before != self.sentinel_sha256_after:
            raise ReconstructionResourceAuditError(
                "storage remount hash differs"
            )
        _require_strong_ref(self.qualification_evidence_ref)
        _require_strong_ref(self.disconnect_evidence_ref)
        expected_evidence: Mapping[str, JSONValue] = {
            "filesystem_id": self.filesystem_id,
            "device_id": self.device_id,
            "remounted_filesystem_id": self.remounted_filesystem_id,
            "remounted_device_id": self.remounted_device_id,
            "sustained_test_bytes": self.sustained_test_bytes,
            "write_throughput_bytes_per_second": (
                self.write_throughput_bytes_per_second
            ),
            "read_throughput_bytes_per_second": (
                self.read_throughput_bytes_per_second
            ),
            "sentinel_sha256_before": self.sentinel_sha256_before,
            "sentinel_sha256_after": self.sentinel_sha256_after,
            "same_filesystem": True,
            "non_sparse_write_verified": True,
            "remount_hash_verified": True,
            "all_terminal_cleanup_verified": True,
        }
        if any(
            self.qualification_evidence_ref.metadata.get(key) != value
            for key, value in expected_evidence.items()
        ):
            raise ReconstructionResourceAuditError(
                "storage measurement evidence binding differs"
            )
        if (
            self.disconnect_evidence_ref.metadata.get("filesystem_id")
            != self.filesystem_id
            or self.disconnect_evidence_ref.metadata.get("device_id")
            != self.device_id
            or self.disconnect_evidence_ref.metadata.get("failed_closed")
            is not True
            or self.disconnect_evidence_ref.metadata.get(
                "local_fallback_absent"
            )
            is not True
        ):
            raise ReconstructionResourceAuditError(
                "storage disconnect evidence binding differs"
            )
        checks = (
            self.same_filesystem,
            self.non_sparse_write_verified,
            self.disconnect_failed_closed,
            self.local_fallback_absent,
            self.remount_hash_verified,
            self.success_cleanup_verified,
            self.refusal_cleanup_verified,
            self.cancellation_cleanup_verified,
            self.failure_cleanup_verified,
        )
        if not all(item is True for item in checks):
            raise ReconstructionResourceAuditError(
                "storage qualification is incomplete"
            )
        if (
            self.filesystem_id != self.remounted_filesystem_id
            or self.device_id != self.remounted_device_id
        ):
            raise ReconstructionResourceAuditError(
                "remounted storage identity differs"
            )
        expected = _stable_id(
            "reconstruction-storage-qualification", self.payload()
        )
        if self.qualification_id and self.qualification_id != expected:
            raise ReconstructionResourceAuditError(
                "storage qualification identity differs"
            )
        object.__setattr__(self, "qualification_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "output_root": self.output_root,
            "scratch_root": self.scratch_root,
            "filesystem_id": self.filesystem_id,
            "device_id": self.device_id,
            "remounted_filesystem_id": self.remounted_filesystem_id,
            "remounted_device_id": self.remounted_device_id,
            "sustained_test_bytes": self.sustained_test_bytes,
            "write_throughput_bytes_per_second": (
                self.write_throughput_bytes_per_second
            ),
            "read_throughput_bytes_per_second": (
                self.read_throughput_bytes_per_second
            ),
            "sentinel_sha256_before": self.sentinel_sha256_before,
            "sentinel_sha256_after": self.sentinel_sha256_after,
            "qualification_evidence_ref": self.qualification_evidence_ref.to_dict(),
            "disconnect_evidence_ref": self.disconnect_evidence_ref.to_dict(),
            "same_filesystem": True,
            "non_sparse_write_verified": True,
            "disconnect_failed_closed": True,
            "local_fallback_absent": True,
            "remount_hash_verified": True,
            "success_cleanup_verified": True,
            "refusal_cleanup_verified": True,
            "cancellation_cleanup_verified": True,
            "failure_cleanup_verified": True,
            "qualified_at_utc": self.qualified_at_utc,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "qualification_id": self.qualification_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionStorageQualificationV1:
        return cls(
            output_root=str(data.get("output_root", "")),
            scratch_root=str(data.get("scratch_root", "")),
            filesystem_id=str(data.get("filesystem_id", "")),
            device_id=str(data.get("device_id", "")),
            remounted_filesystem_id=str(
                data.get("remounted_filesystem_id", "")
            ),
            remounted_device_id=str(data.get("remounted_device_id", "")),
            sustained_test_bytes=_strict_int(
                data.get("sustained_test_bytes"), "sustained_test_bytes"
            ),
            write_throughput_bytes_per_second=_strict_float(
                data.get("write_throughput_bytes_per_second"),
                "write_throughput_bytes_per_second",
            ),
            read_throughput_bytes_per_second=_strict_float(
                data.get("read_throughput_bytes_per_second"),
                "read_throughput_bytes_per_second",
            ),
            sentinel_sha256_before=str(data.get("sentinel_sha256_before", "")),
            sentinel_sha256_after=str(data.get("sentinel_sha256_after", "")),
            qualification_evidence_ref=ArtifactRef.from_dict(
                _mapping(
                    data.get("qualification_evidence_ref"),
                    "qualification_evidence_ref",
                )
            ),
            disconnect_evidence_ref=ArtifactRef.from_dict(
                _mapping(
                    data.get("disconnect_evidence_ref"),
                    "disconnect_evidence_ref",
                )
            ),
            same_filesystem=data.get("same_filesystem") is True,
            non_sparse_write_verified=data.get("non_sparse_write_verified")
            is True,
            disconnect_failed_closed=data.get("disconnect_failed_closed")
            is True,
            local_fallback_absent=data.get("local_fallback_absent") is True,
            remount_hash_verified=data.get("remount_hash_verified") is True,
            success_cleanup_verified=data.get("success_cleanup_verified")
            is True,
            refusal_cleanup_verified=data.get("refusal_cleanup_verified")
            is True,
            cancellation_cleanup_verified=data.get(
                "cancellation_cleanup_verified"
            )
            is True,
            failure_cleanup_verified=data.get("failure_cleanup_verified")
            is True,
            qualified_at_utc=str(data.get("qualified_at_utc", "")),
            qualification_id=str(data.get("qualification_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionCampaignResourceAuditV1:
    """Final measured resource admission bound to frozen campaign identity."""

    final_support_map_ref: ArtifactRef
    release_candidate_ref: ArtifactRef
    corpus_ref: ArtifactRef
    storage_qualification_ref: ArtifactRef
    envelopes: tuple[ReconstructionResourceEnvelopeV1, ...]
    forecast: CampaignResourceForecastV1
    policy: CampaignResourcePolicyV1
    packing_review: ReconstructionPhysicalPackingReviewV1
    residual_limitations: tuple[str, ...]
    status: str = "qualified"
    audit_id: str = ""
    schema_version: str = CAMPAIGN_RESOURCE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, CAMPAIGN_RESOURCE_AUDIT_SCHEMA_VERSION
        )
        for ref in (
            self.final_support_map_ref,
            self.release_candidate_ref,
            self.corpus_ref,
            self.storage_qualification_ref,
        ):
            _require_strong_ref(ref)
        support_id = _required_text(
            self.final_support_map_ref.metadata.get("final_support_map_id"),
            "final_support_map_id",
        )
        candidate_id = _required_text(
            self.release_candidate_ref.metadata.get("candidate_id"),
            "candidate_id",
        )
        if (
            self.final_support_map_ref.metadata.get("release_candidate_id")
            != candidate_id
            or self.forecast.final_support_map_id != support_id
        ):
            raise ReconstructionResourceAuditError(
                "resource audit frozen identity binding differs"
            )
        envelopes = tuple(sorted(self.envelopes, key=lambda item: item.metric))
        required_metrics = set(_RESOURCE_METRICS)
        if {item.metric for item in envelopes} != required_metrics:
            raise ReconstructionResourceAuditError(
                "resource audit envelope metrics are incomplete"
            )
        object.__setattr__(self, "envelopes", envelopes)
        if self.policy.status != "admitted" or self.status != "qualified":
            raise ReconstructionResourceAuditError(
                "campaign resources are not admitted"
            )
        limitations = tuple(
            sorted(
                {
                    _required_text(item, "limitation")
                    for item in self.residual_limitations
                }
            )
        )
        if not limitations:
            raise ReconstructionResourceAuditError(
                "resource audit must state extrapolation limitations"
            )
        object.__setattr__(self, "residual_limitations", limitations)
        expected = _stable_id(
            "reconstruction-campaign-resource-audit", self.payload()
        )
        if self.audit_id and self.audit_id != expected:
            raise ReconstructionResourceAuditError(
                "resource audit identity differs"
            )
        object.__setattr__(self, "audit_id", expected)
        _ensure_size(self.to_dict(), "campaign resource audit")

    def payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "final_support_map_ref": self.final_support_map_ref.to_dict(),
            "release_candidate_ref": self.release_candidate_ref.to_dict(),
            "corpus_ref": self.corpus_ref.to_dict(),
            "storage_qualification_ref": self.storage_qualification_ref.to_dict(),
            "envelopes": [item.to_dict() for item in self.envelopes],
            "forecast": self.forecast.to_dict(),
            "policy": self.policy.to_dict(),
            "packing_review": self.packing_review.to_dict(),
            "residual_limitations": list(self.residual_limitations),
            "status": self.status,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionCampaignResourceAuditV1:
        return cls(
            final_support_map_ref=ArtifactRef.from_dict(
                _mapping(
                    data.get("final_support_map_ref"), "final_support_map_ref"
                )
            ),
            release_candidate_ref=ArtifactRef.from_dict(
                _mapping(
                    data.get("release_candidate_ref"), "release_candidate_ref"
                )
            ),
            corpus_ref=ArtifactRef.from_dict(
                _mapping(data.get("corpus_ref"), "corpus_ref")
            ),
            storage_qualification_ref=ArtifactRef.from_dict(
                _mapping(
                    data.get("storage_qualification_ref"),
                    "storage_qualification_ref",
                )
            ),
            envelopes=tuple(
                ReconstructionResourceEnvelopeV1.from_dict(
                    _mapping(item, "envelope")
                )
                for item in _sequence(data.get("envelopes"), "envelopes")
            ),
            forecast=CampaignResourceForecastV1.from_dict(
                _mapping(data.get("forecast"), "forecast")
            ),
            policy=CampaignResourcePolicyV1.from_dict(
                _mapping(data.get("policy"), "policy")
            ),
            packing_review=ReconstructionPhysicalPackingReviewV1.from_dict(
                _mapping(data.get("packing_review"), "packing_review")
            ),
            residual_limitations=tuple(
                str(item)
                for item in _sequence(
                    data.get("residual_limitations"), "residual_limitations"
                )
            ),
            status=str(data.get("status", "")),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


_RESOURCE_METRICS: Mapping[str, tuple[str, str]] = {
    "bytes_per_logical_event": (
        "bytes_per_logical_event",
        "successful-products",
    ),
    "bytes_per_synthetic_event": (
        "bytes_per_synthetic_event",
        "positive-deficit-products",
    ),
    "manifest_bytes_per_product": ("manifest_bytes", "successful-products"),
    "inodes_per_product": ("inode_count", "successful-products"),
    "peak_rss_bytes_per_worker": ("peak_rss_bytes", "all-terminal-cases"),
    "peak_scratch_bytes_per_worker": (
        "peak_scratch_bytes",
        "all-terminal-cases",
    ),
    "wall_seconds_per_candidate": (
        "wall_per_candidate",
        "candidate-work-cases",
    ),
    "cpu_seconds_per_candidate": ("cpu_per_candidate", "candidate-work-cases"),
    "write_amplification": ("write_amplification", "successful-products"),
    "candidate_amplification": (
        "candidate_amplification",
        "successful-products",
    ),
    "verify_throughput_bytes_per_second": (
        "verify_throughput_bytes_per_second",
        "successful-products",
    ),
    "verification_read_amplification": (
        "verification_read_amplification",
        "successful-products",
    ),
    "poisson_work_units_per_candidate": (
        "poisson_work_units_per_candidate",
        "candidate-work-cases",
    ),
    "temporal_history_bytes_per_product": (
        "temporal_history_bytes",
        "all-terminal-cases",
    ),
    "checkpoint_bytes_per_product": ("checkpoint_bytes", "all-terminal-cases"),
}


def measure_reconstruction_resource_probe(
    probe: ReconstructionResourceProbeV1,
) -> ReconstructionResourceMeasurementV1:
    """Independently verify and measure one probe's committed physical bytes."""
    telemetry = probe.telemetry
    if probe.product_manifest_ref is None:
        return ReconstructionResourceMeasurementV1(
            probe_id=probe.probe_id,
            case_id=probe.case_id,
            terminal_outcome=probe.terminal_outcome,
            strata=probe.strata,
            product_manifest_ref=None,
            publication_id=None,
            logical_event_count=0,
            observed_event_count=0,
            synthetic_event_count=0,
            physical_row_count=0,
            parquet_bytes=0,
            parquet_uncompressed_bytes=0,
            manifest_bytes=0,
            directory_bytes=0,
            inode_count=0,
            row_group_count=0,
            row_group_occupancy=0.0,
            bytes_per_synthetic_event=0.0,
            bytes_per_logical_event=0.0,
            compression_ratio=0.0,
            verification_read_bytes=0,
            verify_wall_seconds=0.0,
            verify_throughput_bytes_per_second=0.0,
            wall_seconds=telemetry.wall_seconds,
            cpu_seconds=telemetry.cpu_seconds,
            peak_rss_bytes=telemetry.peak_rss_bytes,
            peak_scratch_bytes=telemetry.peak_scratch_bytes,
            stage_output_bytes=telemetry.stage_output_bytes,
            write_amplification=0.0,
            candidate_event_count=telemetry.candidate_event_count,
            candidate_amplification=0.0,
            poisson_work_units=telemetry.poisson_work_units,
            temporal_history_bytes=telemetry.temporal_history_bytes,
            checkpoint_bytes=telemetry.checkpoint_bytes,
            cleanup_status=telemetry.cleanup_status,
        )
    manifest_path = verify_artifact_ref(probe.product_manifest_ref)
    started = time.perf_counter()
    manifest = verify_reconstruction_publication(manifest_path)
    elapsed = max(time.perf_counter() - started, 1e-9)
    if not isinstance(manifest, ReconstructionProductManifestV3):
        raise ReconstructionResourceAuditError(
            "resource envelopes require synthetic-delta v3 products"
        )
    product_root = manifest_path.parent
    directory_bytes, inode_count = _tree_physical_usage(product_root)
    parquet_paths = tuple(
        product_root / item.relative_path for item in manifest.partitions
    )
    parquet_bytes = sum(path.stat().st_size for path in parquet_paths)
    uncompressed, row_groups = _parquet_physical_metadata(parquet_paths)
    row_group_capacity = row_groups * manifest.replay.row_group_size
    physical_rows = sum(item.row_count for item in manifest.partitions)
    occupancy = (
        physical_rows / row_group_capacity if row_group_capacity else 0.0
    )
    manifest_bytes = manifest_path.stat().st_size
    source_bytes = sum(
        segment.source_artifact.size_bytes or 0
        for segment in manifest.observed_anchor_segments
    )
    read_bytes = manifest_bytes + parquet_bytes + source_bytes
    logical = manifest.event_count
    observed = manifest.source.observed_event_count
    synthetic = manifest.constraints.synthetic_event_count
    final_bytes = manifest_bytes + parquet_bytes
    stage_write = max(telemetry.stage_output_bytes, final_bytes)
    write_amplification = (
        (telemetry.peak_scratch_bytes + stage_write) / final_bytes
        if final_bytes
        else 0.0
    )
    candidate_amplification = (
        telemetry.candidate_event_count / observed if observed else 0.0
    )
    return ReconstructionResourceMeasurementV1(
        probe_id=probe.probe_id,
        case_id=probe.case_id,
        terminal_outcome=probe.terminal_outcome,
        strata=probe.strata,
        product_manifest_ref=probe.product_manifest_ref,
        publication_id=manifest.publication_id,
        logical_event_count=logical,
        observed_event_count=observed,
        synthetic_event_count=synthetic,
        physical_row_count=physical_rows,
        parquet_bytes=parquet_bytes,
        parquet_uncompressed_bytes=uncompressed,
        manifest_bytes=manifest_bytes,
        directory_bytes=directory_bytes,
        inode_count=inode_count,
        row_group_count=row_groups,
        row_group_occupancy=occupancy,
        bytes_per_synthetic_event=(
            parquet_bytes / synthetic if synthetic else 0.0
        ),
        bytes_per_logical_event=(parquet_bytes / logical if logical else 0.0),
        compression_ratio=(
            parquet_bytes / uncompressed if uncompressed else 0.0
        ),
        verification_read_bytes=read_bytes,
        verify_wall_seconds=elapsed,
        verify_throughput_bytes_per_second=read_bytes / elapsed,
        wall_seconds=telemetry.wall_seconds,
        cpu_seconds=telemetry.cpu_seconds,
        peak_rss_bytes=telemetry.peak_rss_bytes,
        peak_scratch_bytes=telemetry.peak_scratch_bytes,
        stage_output_bytes=telemetry.stage_output_bytes,
        write_amplification=write_amplification,
        candidate_event_count=telemetry.candidate_event_count,
        candidate_amplification=candidate_amplification,
        poisson_work_units=telemetry.poisson_work_units,
        temporal_history_bytes=telemetry.temporal_history_bytes,
        checkpoint_bytes=telemetry.checkpoint_bytes,
        cleanup_status=telemetry.cleanup_status,
    )


def build_resource_measurement_corpus(
    probes: Iterable[ReconstructionResourceProbeV1],
) -> ReconstructionResourceMeasurementCorpusV1:
    """Measure every probe and enforce the complete representative census."""
    return ReconstructionResourceMeasurementCorpusV1(
        measurements=tuple(
            measure_reconstruction_resource_probe(item) for item in probes
        )
    )


def fit_resource_envelopes(
    corpus: ReconstructionResourceMeasurementCorpusV1,
    *,
    quantile: float = DEFAULT_ENVELOPE_QUANTILE,
    extrapolation_limit_factor: float = 8.0,
) -> tuple[ReconstructionResourceEnvelopeV1, ...]:
    """Fit deterministic nearest-rank high-tail envelopes, never means."""
    successful = tuple(
        item
        for item in corpus.measurements
        if item.terminal_outcome == "success"
    )
    positive = tuple(item for item in successful if item.synthetic_event_count)
    candidate = tuple(
        item for item in corpus.measurements if item.candidate_event_count
    )
    values_by_metric: dict[str, tuple[float, ...]] = {
        "bytes_per_logical_event": tuple(
            item.bytes_per_logical_event for item in successful
        ),
        "bytes_per_synthetic_event": tuple(
            item.bytes_per_synthetic_event for item in positive
        ),
        "manifest_bytes_per_product": tuple(
            float(item.manifest_bytes) for item in successful
        ),
        "inodes_per_product": tuple(
            float(item.inode_count) for item in successful
        ),
        "peak_rss_bytes_per_worker": tuple(
            float(item.peak_rss_bytes) for item in corpus.measurements
        ),
        "peak_scratch_bytes_per_worker": tuple(
            float(item.peak_scratch_bytes) for item in corpus.measurements
        ),
        "wall_seconds_per_candidate": tuple(
            item.wall_seconds / item.candidate_event_count for item in candidate
        ),
        "cpu_seconds_per_candidate": tuple(
            item.cpu_seconds / item.candidate_event_count for item in candidate
        ),
        "write_amplification": tuple(
            item.write_amplification for item in successful
        ),
        "candidate_amplification": tuple(
            item.candidate_amplification for item in successful
        ),
        "verify_throughput_bytes_per_second": tuple(
            item.verify_throughput_bytes_per_second for item in successful
        ),
        "verification_read_amplification": tuple(
            item.verification_read_bytes
            / max(1, item.parquet_bytes + item.manifest_bytes)
            for item in successful
        ),
        "poisson_work_units_per_candidate": tuple(
            item.poisson_work_units / item.candidate_event_count
            for item in candidate
        ),
        "temporal_history_bytes_per_product": tuple(
            float(item.temporal_history_bytes) for item in corpus.measurements
        ),
        "checkpoint_bytes_per_product": tuple(
            float(item.checkpoint_bytes) for item in corpus.measurements
        ),
    }
    envelopes: list[ReconstructionResourceEnvelopeV1] = []
    for metric, values in values_by_metric.items():
        if not values:
            raise ReconstructionResourceAuditError(
                f"resource envelope {metric} lacks measurements"
            )
        high_quantile = _nearest_rank(values, quantile)
        absolute_residuals = tuple(
            abs(value - high_quantile) for value in values
        )
        envelopes.append(
            ReconstructionResourceEnvelopeV1(
                metric=metric,
                quantile=quantile,
                high_quantile_value=high_quantile,
                maximum_value=max(values),
                minimum_value=min(values),
                high_quantile_absolute_residual=_nearest_rank(
                    absolute_residuals, quantile
                ),
                maximum_absolute_residual=max(absolute_residuals),
                maximum_positive_residual=max(values) - high_quantile,
                sample_count=len(values),
                extrapolation_limit_factor=extrapolation_limit_factor,
                basis=_RESOURCE_METRICS[metric][1],
            )
        )
    return tuple(sorted(envelopes, key=lambda item: item.metric))


def forecast_campaign_resources(
    support: FinalAdaptiveSupportMapIndexV1,
    corpus: ReconstructionResourceMeasurementCorpusV1,
    envelopes: Sequence[ReconstructionResourceEnvelopeV1],
) -> CampaignResourceForecastV1:
    """Apply measured envelopes to every executable final-support coordinate."""
    metric = {item.metric: item for item in envelopes}
    if set(metric) != set(_RESOURCE_METRICS):
        raise ReconstructionResourceAuditError(
            "resource envelope set is incomplete"
        )
    product_count = 0
    observed_events = 0
    synthetic_events = 0
    candidate_events = 0
    for ref in support.verification_shard_refs:
        shard = read_final_support_verification_shard(ref.path)
        for window in shard.windows:
            if window.status != "executable":
                continue
            product_count += window.member_count
            observed = (
                sum(window.core_event_counts.values()) * window.member_count
            )
            synthetic = window.modeled_missing_event_count * window.member_count
            observed_events += observed
            synthetic_events += synthetic
            candidate_events += math.ceil(
                sum(window.core_event_counts.values())
                * window.candidate_amplification
                * window.member_count
            )
    logical_events = observed_events + synthetic_events
    if not product_count or not logical_events:
        raise ReconstructionResourceAuditError(
            "final support map has no measurable product rectangle"
        )
    measured_max_logical = max(
        item.logical_event_count
        for item in corpus.measurements
        if item.terminal_outcome == "success"
    )
    average_product_events = logical_events / product_count
    extrapolation = max(1.0, average_product_events / measured_max_logical)
    limit = min(item.extrapolation_limit_factor for item in envelopes)
    if extrapolation > limit:
        raise ReconstructionResourceAuditError(
            "campaign forecast exceeds measured extrapolation limit"
        )
    manifest_high = metric[
        "manifest_bytes_per_product"
    ].conservative_upper_bound
    output_high = metric["bytes_per_logical_event"].conservative_upper_bound
    output_low = metric["bytes_per_logical_event"].minimum_value
    manifest_low = metric["manifest_bytes_per_product"].minimum_value
    output_lower = math.ceil(
        logical_events * output_low + product_count * manifest_low
    )
    output_upper = math.ceil(
        logical_events * output_high + product_count * manifest_high
    )
    read_amplification = metric["verification_read_amplification"]
    verification_read_lower = math.ceil(
        output_lower * read_amplification.minimum_value
    )
    verification_read_upper = math.ceil(
        output_upper * read_amplification.conservative_upper_bound
    )
    verify_fast = metric["verify_throughput_bytes_per_second"].maximum_value
    verify_slow = metric["verify_throughput_bytes_per_second"].minimum_value
    if verify_fast <= 0.0 or verify_slow <= 0.0:
        raise ReconstructionResourceAuditError(
            "verification throughput is zero"
        )
    wall_per_candidate = metric["wall_seconds_per_candidate"]
    campaign_lower = candidate_events * wall_per_candidate.minimum_value
    campaign_upper = (
        candidate_events * wall_per_candidate.conservative_upper_bound
    )
    cpu_per_candidate = metric["cpu_seconds_per_candidate"]
    return CampaignResourceForecastV1(
        final_support_map_id=support.final_support_map_id,
        product_count=product_count,
        logical_event_count=logical_events,
        observed_event_count=observed_events,
        synthetic_event_count=synthetic_events,
        candidate_event_count=candidate_events,
        output_bytes_lower=output_lower,
        output_bytes_upper=output_upper,
        peak_scratch_bytes_per_worker=math.ceil(
            metric["peak_scratch_bytes_per_worker"].conservative_upper_bound
        ),
        peak_rss_bytes_per_worker=math.ceil(
            metric["peak_rss_bytes_per_worker"].conservative_upper_bound
        ),
        inode_count_upper=math.ceil(
            product_count
            * metric["inodes_per_product"].conservative_upper_bound
        ),
        temporal_history_bytes_upper=math.ceil(
            product_count
            * metric[
                "temporal_history_bytes_per_product"
            ].conservative_upper_bound
        ),
        checkpoint_bytes_upper=math.ceil(
            product_count
            * metric["checkpoint_bytes_per_product"].conservative_upper_bound
        ),
        poisson_work_units_upper=math.ceil(
            candidate_events
            * metric[
                "poisson_work_units_per_candidate"
            ].conservative_upper_bound
        ),
        verification_read_bytes_lower=verification_read_lower,
        verification_read_bytes_upper=verification_read_upper,
        verify_seconds_lower=verification_read_lower / verify_fast,
        verify_seconds_upper=verification_read_upper / verify_slow,
        campaign_seconds_lower=campaign_lower,
        campaign_seconds_upper=campaign_upper,
        campaign_cpu_seconds_lower=(
            candidate_events * cpu_per_candidate.minimum_value
        ),
        campaign_cpu_seconds_upper=(
            candidate_events * cpu_per_candidate.conservative_upper_bound
        ),
        write_amplification_upper=metric[
            "write_amplification"
        ].conservative_upper_bound,
        candidate_amplification_upper=metric[
            "candidate_amplification"
        ].conservative_upper_bound,
        extrapolation_factor=extrapolation,
    )


def admit_campaign_resources(
    forecast: CampaignResourceForecastV1,
    *,
    available_memory_bytes: int,
    available_scratch_bytes: int,
    available_output_bytes: int,
    available_inodes: int,
    maximum_campaign_seconds: float,
    reserve_fraction: float = 0.25,
    maximum_container_bytes: int = 4 * 1024**3,
    maximum_products_per_container: int = 256,
) -> CampaignResourcePolicyV1:
    """Derive bounded concurrency/shards and fail closed outside capacity."""
    memory = _positive_int(available_memory_bytes, "available_memory_bytes")
    scratch = _positive_int(available_scratch_bytes, "available_scratch_bytes")
    output = _positive_int(available_output_bytes, "available_output_bytes")
    inodes = _positive_int(available_inodes, "available_inodes")
    reserve = _strict_float(reserve_fraction, "reserve_fraction")
    if not 0.05 <= reserve <= 0.75:
        raise ReconstructionResourceAuditError("capacity reserve is invalid")
    maximum_seconds = _positive_float(
        maximum_campaign_seconds, "maximum_campaign_seconds"
    )
    container_bytes = _positive_int(
        maximum_container_bytes, "maximum_container_bytes"
    )
    products_per_container = _positive_int(
        maximum_products_per_container, "maximum_products_per_container"
    )
    usable = 1.0 - reserve
    per_worker_memory = max(1, forecast.peak_rss_bytes_per_worker)
    per_worker_scratch = max(1, forecast.peak_scratch_bytes_per_worker)
    concurrency = max(
        1,
        min(
            math.floor(memory * usable / per_worker_memory),
            math.floor(scratch * usable / per_worker_scratch),
        ),
    )
    product_bytes = max(
        1, math.ceil(forecast.output_bytes_upper / forecast.product_count)
    )
    product_inodes = max(
        1, math.ceil(forecast.inode_count_upper / forecast.product_count)
    )
    products_per_shard = max(
        1,
        min(
            products_per_container,
            math.floor(container_bytes / product_bytes),
            math.floor(max(1, inodes * usable) / product_inodes),
        ),
    )
    reasons: list[str] = []
    if per_worker_memory > memory * usable:
        reasons.append("peak_memory_exceeds_reserved_capacity")
    if per_worker_scratch > scratch * usable:
        reasons.append("peak_scratch_exceeds_reserved_capacity")
    if forecast.output_bytes_upper > output * usable:
        reasons.append("output_forecast_exceeds_reserved_capacity")
    if forecast.inode_count_upper > inodes * usable:
        reasons.append("inode_forecast_exceeds_reserved_capacity")
    effective_upper = forecast.campaign_seconds_upper / concurrency
    if effective_upper > maximum_seconds:
        reasons.append("campaign_duration_exceeds_budget")
    return CampaignResourcePolicyV1(
        available_memory_bytes=memory,
        available_scratch_bytes=scratch,
        available_output_bytes=output,
        available_inodes=inodes,
        maximum_campaign_seconds=maximum_seconds,
        reserve_fraction=reserve,
        maximum_container_bytes=container_bytes,
        maximum_products_per_container=products_per_container,
        concurrency=concurrency,
        products_per_shard=products_per_shard,
        maximum_write_amplification=forecast.write_amplification_upper,
        maximum_candidate_amplification=forecast.candidate_amplification_upper,
        status="refused" if reasons else "admitted",
        refusal_reasons=tuple(reasons),
    )


def review_physical_packing(
    corpus: ReconstructionResourceMeasurementCorpusV1,
    policy: CampaignResourcePolicyV1,
    *,
    small_product_threshold_bytes: int = 1 * 1024**2,
    partition_sensitivity_evidence_available: bool = False,
) -> ReconstructionPhysicalPackingReviewV1:
    """Evaluate packing while conservatively preserving per-product layout."""
    product_bytes = sorted(
        item.directory_bytes
        for item in corpus.measurements
        if item.terminal_outcome == "success"
    )
    median = int(_nearest_rank(product_bytes, 0.5))
    fraction = sum(
        item < small_product_threshold_bytes for item in product_bytes
    ) / len(product_bytes)
    decision = (
        "bounded_immutable_containers"
        if fraction >= 0.75 and partition_sensitivity_evidence_available
        else "retain_per_product"
    )
    return ReconstructionPhysicalPackingReviewV1(
        decision=decision,
        median_product_bytes=median,
        small_product_fraction=fraction,
        maximum_container_bytes=policy.maximum_container_bytes,
        maximum_products_per_container=policy.maximum_products_per_container,
        partition_sensitivity_evidence_available=(
            partition_sensitivity_evidence_available
        ),
    )


def build_campaign_resource_audit(
    *,
    final_support_map_path: str | Path,
    release_candidate_path: str | Path,
    corpus: ReconstructionResourceMeasurementCorpusV1,
    storage: ReconstructionStorageQualificationV1,
    available_memory_bytes: int,
    available_scratch_bytes: int,
    available_output_bytes: int,
    available_inodes: int,
    maximum_campaign_seconds: float,
    output_directory: str | Path,
    quantile: float = DEFAULT_ENVELOPE_QUANTILE,
    reserve_fraction: float = 0.25,
    maximum_container_bytes: int = 4 * 1024**3,
    maximum_products_per_container: int = 256,
    partition_sensitivity_evidence_available: bool = False,
) -> ArtifactRef:
    """Build, write, reread, and independently bind the final resource audit."""
    support = read_final_adaptive_support_map_index(final_support_map_path)
    candidate = read_reconstruction_release_candidate(release_candidate_path)
    support_ref = artifact_ref_for_file(
        final_support_map_path,
        kind="final_adaptive_support_map_index_v1",
        metadata={
            "final_support_map_id": support.final_support_map_id,
            "release_candidate_id": support.release_candidate_id,
        },
    )
    candidate_ref = artifact_ref_for_file(
        release_candidate_path,
        kind="reconstruction_release_candidate_v1",
        metadata={"candidate_id": candidate.candidate_id},
    )
    if support.release_candidate_id != candidate.candidate_id:
        raise ReconstructionResourceAuditError(
            "resource audit candidate differs from final support"
        )
    _verify_storage_candidate_binding(storage, candidate)
    root = Path(output_directory).expanduser().resolve()
    corpus_ref = write_resource_measurement_corpus(
        corpus, root / "measurements"
    )
    storage_ref = write_storage_qualification(storage, root / "storage")
    envelopes = fit_resource_envelopes(corpus, quantile=quantile)
    forecast = forecast_campaign_resources(support, corpus, envelopes)
    if storage.sustained_test_bytes <= forecast.peak_scratch_bytes_per_worker:
        raise ReconstructionResourceAuditError(
            "storage sustained write does not exceed measured scratch peak"
        )
    policy = admit_campaign_resources(
        forecast,
        available_memory_bytes=available_memory_bytes,
        available_scratch_bytes=available_scratch_bytes,
        available_output_bytes=available_output_bytes,
        available_inodes=available_inodes,
        maximum_campaign_seconds=maximum_campaign_seconds,
        reserve_fraction=reserve_fraction,
        maximum_container_bytes=maximum_container_bytes,
        maximum_products_per_container=maximum_products_per_container,
    )
    if policy.status != "admitted":
        raise ReconstructionResourceAuditError(
            "campaign resource admission refused: "
            + ", ".join(policy.refusal_reasons)
        )
    packing = review_physical_packing(
        corpus,
        policy,
        partition_sensitivity_evidence_available=(
            partition_sensitivity_evidence_available
        ),
    )
    audit = ReconstructionCampaignResourceAuditV1(
        final_support_map_ref=support_ref,
        release_candidate_ref=candidate_ref,
        corpus_ref=corpus_ref,
        storage_qualification_ref=storage_ref,
        envelopes=envelopes,
        forecast=forecast,
        policy=policy,
        packing_review=packing,
        residual_limitations=(
            "Envelope validity is bounded to the recorded source, runtime, compression, and measured extrapolation range.",
            "A changed support map, release candidate, filesystem identity, writer, or compression implementation requires a new audit.",
            "Packing remains per-product unless independent partition-sensitivity evidence qualifies immutable container shards.",
        ),
    )
    ref = write_campaign_resource_audit(audit, root)
    if read_campaign_resource_audit(ref.path) != audit:
        raise ReconstructionResourceAuditError(
            "resource audit write/read differs"
        )
    return ref


def build_campaign_resource_audit_from_spec(
    spec_path: str | Path,
    *,
    output_directory: str | Path,
) -> ArtifactRef:
    """Build the audit from one bounded operator-authored JSON specification."""
    data = _read_json(spec_path)
    _require_schema(
        str(data.get("schema_version", "")),
        CAMPAIGN_RESOURCE_AUDIT_SPEC_SCHEMA_VERSION,
    )
    probes = tuple(
        _probe_from_spec(_mapping(item, "probe"))
        for item in _sequence(data.get("probes"), "probes")
    )
    corpus = build_resource_measurement_corpus(probes)
    storage = read_storage_qualification(
        str(data.get("storage_qualification", ""))
    )
    capacity = _mapping(data.get("capacity"), "capacity")
    return build_campaign_resource_audit(
        final_support_map_path=str(data.get("final_support_map", "")),
        release_candidate_path=str(data.get("release_candidate", "")),
        corpus=corpus,
        storage=storage,
        available_memory_bytes=_strict_int(
            capacity.get("available_memory_bytes"), "available_memory_bytes"
        ),
        available_scratch_bytes=_strict_int(
            capacity.get("available_scratch_bytes"), "available_scratch_bytes"
        ),
        available_output_bytes=_strict_int(
            capacity.get("available_output_bytes"), "available_output_bytes"
        ),
        available_inodes=_strict_int(
            capacity.get("available_inodes"), "available_inodes"
        ),
        maximum_campaign_seconds=_strict_float(
            capacity.get("maximum_campaign_seconds"), "maximum_campaign_seconds"
        ),
        output_directory=output_directory,
        quantile=_strict_float(data.get("quantile", 0.95), "quantile"),
        reserve_fraction=_strict_float(
            data.get("reserve_fraction", 0.25), "reserve_fraction"
        ),
        maximum_container_bytes=_strict_int(
            data.get("maximum_container_bytes", 4 * 1024**3),
            "maximum_container_bytes",
        ),
        maximum_products_per_container=_strict_int(
            data.get("maximum_products_per_container", 256),
            "maximum_products_per_container",
        ),
        partition_sensitivity_evidence_available=(
            data.get("partition_sensitivity_evidence_available") is True
        ),
    )


def write_resource_measurement_corpus(
    corpus: ReconstructionResourceMeasurementCorpusV1,
    output_directory: str | Path,
) -> ArtifactRef:
    path = _write_json_artifact(
        corpus.to_dict(),
        output_directory,
        "reconstruction-resource-measurement-corpus",
        corpus.corpus_id,
    )
    return artifact_ref_for_file(
        path,
        kind=RESOURCE_MEASUREMENT_CORPUS_ARTIFACT_KIND,
        metadata={
            "corpus_id": corpus.corpus_id,
            "measurement_count": len(corpus.measurements),
        },
    )


def read_resource_measurement_corpus(
    path: str | Path,
) -> ReconstructionResourceMeasurementCorpusV1:
    return ReconstructionResourceMeasurementCorpusV1.from_dict(_read_json(path))


def write_storage_qualification(
    qualification: ReconstructionStorageQualificationV1,
    output_directory: str | Path,
) -> ArtifactRef:
    path = _write_json_artifact(
        qualification.to_dict(),
        output_directory,
        "reconstruction-storage-qualification",
        qualification.qualification_id,
    )
    return artifact_ref_for_file(
        path,
        kind=STORAGE_QUALIFICATION_ARTIFACT_KIND,
        metadata={
            "qualification_id": qualification.qualification_id,
            "filesystem_id": qualification.filesystem_id,
            "device_id": qualification.device_id,
            "passed": True,
        },
    )


def read_storage_qualification(
    path: str | Path,
) -> ReconstructionStorageQualificationV1:
    qualification = ReconstructionStorageQualificationV1.from_dict(
        _read_json(path)
    )
    verify_artifact_ref(qualification.qualification_evidence_ref)
    verify_artifact_ref(qualification.disconnect_evidence_ref)
    return qualification


def write_campaign_resource_audit(
    audit: ReconstructionCampaignResourceAuditV1,
    output_directory: str | Path,
) -> ArtifactRef:
    path = _write_json_artifact(
        audit.to_dict(),
        output_directory,
        "reconstruction-campaign-resource-audit",
        audit.audit_id,
    )
    return artifact_ref_for_file(
        path,
        kind=CAMPAIGN_RESOURCE_AUDIT_ARTIFACT_KIND,
        metadata={
            "audit_id": audit.audit_id,
            "final_support_map_id": audit.forecast.final_support_map_id,
            "policy_id": audit.policy.policy_id,
            "status": audit.status,
        },
    )


def read_campaign_resource_audit(
    path: str | Path,
) -> ReconstructionCampaignResourceAuditV1:
    audit = ReconstructionCampaignResourceAuditV1.from_dict(_read_json(path))
    for ref in (
        audit.final_support_map_ref,
        audit.release_candidate_ref,
        audit.corpus_ref,
        audit.storage_qualification_ref,
    ):
        verify_artifact_ref(ref)
    support = read_final_adaptive_support_map_index(
        audit.final_support_map_ref.path
    )
    candidate = read_reconstruction_release_candidate(
        audit.release_candidate_ref.path
    )
    corpus = read_resource_measurement_corpus(audit.corpus_ref.path)
    storage = read_storage_qualification(audit.storage_qualification_ref.path)
    if (
        support.final_support_map_id != audit.forecast.final_support_map_id
        or candidate.candidate_id != support.release_candidate_id
        or corpus.corpus_id != audit.corpus_ref.metadata.get("corpus_id")
        or storage.qualification_id
        != audit.storage_qualification_ref.metadata.get("qualification_id")
    ):
        raise ReconstructionResourceAuditError(
            "resource audit referenced evidence differs"
        )
    rebuilt_envelopes = fit_resource_envelopes(
        corpus,
        quantile=audit.envelopes[0].quantile,
        extrapolation_limit_factor=min(
            item.extrapolation_limit_factor for item in audit.envelopes
        ),
    )
    if rebuilt_envelopes != audit.envelopes:
        raise ReconstructionResourceAuditError(
            "resource audit envelopes differ from the measurement corpus"
        )
    rebuilt_forecast = forecast_campaign_resources(
        support, corpus, rebuilt_envelopes
    )
    if rebuilt_forecast != audit.forecast:
        raise ReconstructionResourceAuditError(
            "resource audit forecast differs from final support"
        )
    rebuilt_policy = admit_campaign_resources(
        rebuilt_forecast,
        available_memory_bytes=audit.policy.available_memory_bytes,
        available_scratch_bytes=audit.policy.available_scratch_bytes,
        available_output_bytes=audit.policy.available_output_bytes,
        available_inodes=audit.policy.available_inodes,
        maximum_campaign_seconds=audit.policy.maximum_campaign_seconds,
        reserve_fraction=audit.policy.reserve_fraction,
        maximum_container_bytes=audit.policy.maximum_container_bytes,
        maximum_products_per_container=(
            audit.policy.maximum_products_per_container
        ),
    )
    if rebuilt_policy != audit.policy:
        raise ReconstructionResourceAuditError(
            "resource audit policy differs from forecast admission"
        )
    rebuilt_packing = review_physical_packing(
        corpus,
        rebuilt_policy,
        partition_sensitivity_evidence_available=(
            audit.packing_review.partition_sensitivity_evidence_available
        ),
    )
    if rebuilt_packing != audit.packing_review:
        raise ReconstructionResourceAuditError(
            "resource audit packing review differs"
        )
    _verify_storage_candidate_binding(storage, candidate)
    if (
        storage.sustained_test_bytes
        <= rebuilt_forecast.peak_scratch_bytes_per_worker
    ):
        raise ReconstructionResourceAuditError(
            "storage sustained write does not exceed measured scratch peak"
        )
    return audit


def _probe_from_spec(data: Mapping[str, Any]) -> ReconstructionResourceProbeV1:
    manifest = _optional_text(data.get("product_manifest"))
    telemetry_data = dict(_mapping(data.get("telemetry"), "telemetry"))
    receipt = _optional_text(data.get("operation_receipt"))
    manifest_ref: ArtifactRef | None = None
    if manifest is not None:
        manifest_path = Path(manifest).expanduser().resolve()
        manifest_value = verify_reconstruction_publication(manifest_path)
        if not isinstance(manifest_value, ReconstructionProductManifestV3):
            raise ReconstructionResourceAuditError("probe product is not v3")
        manifest_ref = artifact_ref_for_file(
            manifest_path,
            kind="reconstruction-product-manifest",
            metadata={
                "manifest_id": manifest_value.manifest_id,
                "publication_id": manifest_value.publication_id,
                "schema_version": manifest_value.schema_version,
            },
        )
        if receipt is not None:
            telemetry_data = {
                **_receipt_telemetry(receipt, manifest_path),
                **telemetry_data,
            }
    return ReconstructionResourceProbeV1(
        case_id=str(data.get("case_id", "")),
        terminal_outcome=str(data.get("terminal_outcome", "")),
        strata={
            str(key): str(value)
            for key, value in _mapping(data.get("strata"), "strata").items()
        },
        telemetry=ReconstructionResourceRuntimeTelemetryV1.from_dict(
            telemetry_data
        ),
        product_manifest_ref=manifest_ref,
    )


def _receipt_telemetry(
    receipt_path: str | Path, manifest_path: Path
) -> dict[str, JSONValue]:
    data = _read_json(receipt_path)
    matches: list[Mapping[str, Any]] = []
    for report in _sequence(data.get("reports", ()), "reports"):
        report_data = _mapping(report, "report")
        for ref in _sequence(
            report_data.get("committed_manifest_refs", ()),
            "committed_manifest_refs",
        ):
            ref_data = _mapping(ref, "manifest_ref")
            if (
                Path(str(ref_data.get("path", ""))).expanduser().resolve()
                == manifest_path
            ):
                strong = ArtifactRef.from_dict(ref_data)
                verify_artifact_ref(strong)
                matches.append(strong.metadata)
    if len(matches) != 1:
        raise ReconstructionResourceAuditError(
            "operation receipt does not uniquely bind the product manifest"
        )
    metadata = matches[0]
    return {
        "schema_version": RESOURCE_RUNTIME_TELEMETRY_SCHEMA_VERSION,
        "wall_seconds": _metadata_float(metadata, "runtime_seconds"),
        "cpu_seconds": _metadata_float(metadata, "cpu_seconds"),
        "peak_rss_bytes": _metadata_int(metadata, "peak_rss_bytes"),
        "peak_scratch_bytes": _metadata_int(metadata, "scratch_bytes"),
        "stage_output_bytes": _metadata_int(metadata, "output_bytes"),
        "candidate_event_count": _metadata_int(
            metadata, "candidate_event_count"
        ),
        "poisson_work_units": _metadata_int(metadata, "poisson_work_units"),
        "temporal_history_bytes": _metadata_int(
            metadata, "temporal_history_bytes"
        ),
        "checkpoint_bytes": _metadata_int(metadata, "checkpoint_bytes"),
        "cleanup_status": SUCCESS_CLEANUP_STATUS,
        "uncommitted_bytes_after_cleanup": 0,
    }


def _verify_storage_candidate_binding(
    storage: ReconstructionStorageQualificationV1,
    candidate: ReconstructionReleaseCandidateV1,
) -> None:
    roots = {item.role: item for item in candidate.filesystem_roots}
    output = roots.get("output")
    scratch = roots.get("scratch")
    if output is None or scratch is None:
        raise ReconstructionResourceAuditError(
            "release candidate lacks output/scratch storage roots"
        )
    if (
        Path(output.path) != Path(storage.output_root)
        or Path(scratch.path) != Path(storage.scratch_root)
        or output.filesystem_id != storage.filesystem_id
        or scratch.filesystem_id != storage.filesystem_id
        or output.device_id != storage.device_id
        or scratch.device_id != storage.device_id
    ):
        raise ReconstructionResourceAuditError(
            "storage qualification differs from release candidate roots"
        )


def _parquet_physical_metadata(paths: Sequence[Path]) -> tuple[int, int]:
    try:
        import pyarrow.parquet as pq
    except (
        ImportError
    ) as err:  # pragma: no cover - optional dependency boundary
        raise ReconstructionResourceAuditError(
            "resource measurement requires the arrow extra"
        ) from err
    uncompressed = 0
    row_groups = 0
    for path in paths:
        metadata = pq.ParquetFile(path).metadata
        row_groups += metadata.num_row_groups
        for row_group_index in range(metadata.num_row_groups):
            row_group = metadata.row_group(row_group_index)
            for column_index in range(row_group.num_columns):
                column = row_group.column(column_index)
                uncompressed += max(0, int(column.total_uncompressed_size))
    return uncompressed, row_groups


def _tree_physical_usage(root: Path) -> tuple[int, int]:
    total_bytes = 0
    inode_count = 1
    for path in root.rglob("*"):
        inode_count += 1
        if path.is_file() and not path.is_symlink():
            total_bytes += path.stat().st_size
    return total_bytes, inode_count


def _aggregate_workload(
    measurements: Sequence[ReconstructionResourceMeasurementV1],
) -> dict[str, JSONValue]:
    successful = tuple(
        item for item in measurements if item.terminal_outcome == "success"
    )
    return {
        "measurement_count": len(measurements),
        "successful_product_count": len(successful),
        "logical_event_count": sum(
            item.logical_event_count for item in successful
        ),
        "observed_event_count": sum(
            item.observed_event_count for item in successful
        ),
        "synthetic_event_count": sum(
            item.synthetic_event_count for item in successful
        ),
        "physical_row_count": sum(
            item.physical_row_count for item in successful
        ),
        "parquet_bytes": sum(item.parquet_bytes for item in successful),
        "manifest_bytes": sum(item.manifest_bytes for item in successful),
        "directory_bytes": sum(item.directory_bytes for item in successful),
        "inode_count": sum(item.inode_count for item in successful),
        "row_group_count": sum(item.row_group_count for item in successful),
        "verification_read_bytes": sum(
            item.verification_read_bytes for item in successful
        ),
        "stage_output_bytes": sum(
            item.stage_output_bytes for item in measurements
        ),
        "candidate_event_count": sum(
            item.candidate_event_count for item in measurements
        ),
        "poisson_work_units": sum(
            item.poisson_work_units for item in measurements
        ),
        "temporal_history_bytes": sum(
            item.temporal_history_bytes for item in measurements
        ),
        "checkpoint_bytes": sum(item.checkpoint_bytes for item in measurements),
        "wall_seconds": round(
            sum(item.wall_seconds for item in measurements), 12
        ),
        "cpu_seconds": round(
            sum(item.cpu_seconds for item in measurements), 12
        ),
        "maximum_peak_rss_bytes": max(
            item.peak_rss_bytes for item in measurements
        ),
        "maximum_peak_scratch_bytes": max(
            item.peak_scratch_bytes for item in measurements
        ),
    }


def _nearest_rank(
    values: Sequence[float] | Sequence[int], quantile: float
) -> float:
    ordered = sorted(float(item) for item in values)
    if not ordered:
        raise ReconstructionResourceAuditError("quantile requires observations")
    rank = max(1, math.ceil(quantile * len(ordered)))
    return ordered[rank - 1]


def _write_json_artifact(
    payload: Mapping[str, JSONValue],
    output_directory: str | Path,
    prefix: str,
    identity: str,
) -> Path:
    root = Path(output_directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    digest = identity.rsplit(":", 1)[-1]
    path = root / f"{prefix}-{digest}.json"
    encoded = (canonical_contract_json(payload) + "\n").encode("utf-8")
    if len(encoded) > MAX_RESOURCE_ARTIFACT_BYTES:
        raise ReconstructionResourceAuditError(
            "resource artifact exceeds size limit"
        )
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    with temporary.open("xb") as stream:
        stream.write(encoded)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    return path


def _read_json(path: str | Path) -> Mapping[str, Any]:
    target = Path(path).expanduser().resolve()
    if (
        not target.is_file()
        or target.stat().st_size > MAX_RESOURCE_ARTIFACT_BYTES
    ):
        raise ReconstructionResourceAuditError(
            "resource artifact is missing or oversized"
        )
    try:
        value = json.loads(target.read_text(encoding="utf-8"))
    except (UnicodeError, json.JSONDecodeError) as err:
        raise ReconstructionResourceAuditError(
            "resource artifact is invalid JSON"
        ) from err
    return _mapping(value, "resource artifact")


def _stable_id(domain: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(canonical_contract_json(payload).encode("utf-8"))
    return f"{domain}:sha256:{digest.hexdigest()}"


def _strata(value: Mapping[str, str]) -> dict[str, str]:
    result = {
        _required_text(str(axis), "stratum axis"): _required_text(
            str(item), "stratum value"
        )
        for axis, item in value.items()
    }
    unknown = set(result).difference(REQUIRED_MEASUREMENT_STRATA)
    if unknown:
        raise ReconstructionResourceAuditError(
            "unknown resource strata: " + ", ".join(sorted(unknown))
        )
    for axis, item in result.items():
        if item not in REQUIRED_MEASUREMENT_STRATA[axis]:
            raise ReconstructionResourceAuditError(
                f"unsupported {axis} stratum: {item}"
            )
    return dict(sorted(result.items()))


def _require_strong_ref(ref: ArtifactRef) -> None:
    if ref.sha256 is None or ref.size_bytes is None:
        raise ReconstructionResourceAuditError(
            "resource evidence reference is weak"
        )
    _sha256(ref.sha256, "artifact sha256")
    _nonnegative_int(ref.size_bytes, "artifact size")


def _absolute_path(value: str, name: str) -> Path:
    path = Path(_required_text(value, name)).expanduser()
    if not path.is_absolute():
        raise ReconstructionResourceAuditError(f"{name} is relative")
    return path.resolve()


def _ensure_size(value: Mapping[str, JSONValue], name: str) -> None:
    if (
        len(canonical_contract_json(value).encode("utf-8"))
        > MAX_RESOURCE_ARTIFACT_BYTES
    ):
        raise ReconstructionResourceAuditError(f"{name} exceeds size limit")


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ReconstructionResourceAuditError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _sequence(value: Any, name: str) -> Sequence[Any]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise ReconstructionResourceAuditError(f"{name} must be an array")
    return value


def _int_mapping(value: Any, name: str) -> dict[str, int]:
    return {
        str(key): _strict_int(item, f"{name}.{key}")
        for key, item in _mapping(value, name).items()
    }


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ReconstructionResourceAuditError(f"{name} must be an integer")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    result = _strict_int(value, name)
    if result < 0:
        raise ReconstructionResourceAuditError(f"{name} must be nonnegative")
    return result


def _positive_int(value: Any, name: str) -> int:
    result = _nonnegative_int(value, name)
    if result == 0:
        raise ReconstructionResourceAuditError(f"{name} must be positive")
    return result


def _strict_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ReconstructionResourceAuditError(f"{name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ReconstructionResourceAuditError(f"{name} must be finite")
    return result


def _nonnegative_float(value: Any, name: str) -> float:
    result = _strict_float(value, name)
    if result < 0.0:
        raise ReconstructionResourceAuditError(f"{name} must be nonnegative")
    return result


def _positive_float(value: Any, name: str) -> float:
    result = _nonnegative_float(value, name)
    if result <= 0.0:
        raise ReconstructionResourceAuditError(f"{name} must be positive")
    return result


def _required_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > 4_096:
        raise ReconstructionResourceAuditError(f"{name} must be bounded text")
    return value.strip()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    return _required_text(value, "optional text")


def _sha256(value: Any, name: str) -> str:
    text = _required_text(value, name)
    if len(text) != _SHA256_RE_LENGTH or any(
        character not in "0123456789abcdef" for character in text
    ):
        raise ReconstructionResourceAuditError(f"{name} is not sha256")
    return text


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ReconstructionResourceAuditError(f"unsupported schema: {actual}")


def _require_derived(data: Mapping[str, Any], key: str, expected: Any) -> None:
    if data.get(key) != expected:
        raise ReconstructionResourceAuditError(f"derived field {key} differs")


def _metadata_int(metadata: Mapping[str, JSONValue], key: str) -> int:
    value = metadata.get(key, 0)
    return value if type(value) is int and value >= 0 else 0


def _metadata_float(metadata: Mapping[str, JSONValue], key: str) -> float:
    value = metadata.get(key, 0.0)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    return max(0.0, float(value))


__all__ = [
    "CAMPAIGN_RESOURCE_AUDIT_ARTIFACT_KIND",
    "CAMPAIGN_RESOURCE_AUDIT_SCHEMA_VERSION",
    "CAMPAIGN_RESOURCE_AUDIT_SPEC_SCHEMA_VERSION",
    "CAMPAIGN_RESOURCE_FORECAST_SCHEMA_VERSION",
    "CAMPAIGN_RESOURCE_POLICY_SCHEMA_VERSION",
    "PACKING_REVIEW_SCHEMA_VERSION",
    "REQUIRED_MEASUREMENT_STRATA",
    "RESOURCE_ENVELOPE_SCHEMA_VERSION",
    "RESOURCE_MEASUREMENT_ARTIFACT_KIND",
    "RESOURCE_MEASUREMENT_CORPUS_ARTIFACT_KIND",
    "RESOURCE_MEASUREMENT_CORPUS_SCHEMA_VERSION",
    "RESOURCE_MEASUREMENT_SCHEMA_VERSION",
    "RESOURCE_PROBE_SCHEMA_VERSION",
    "RESOURCE_RUNTIME_TELEMETRY_SCHEMA_VERSION",
    "STORAGE_QUALIFICATION_ARTIFACT_KIND",
    "STORAGE_QUALIFICATION_SCHEMA_VERSION",
    "CampaignResourceForecastV1",
    "CampaignResourcePolicyV1",
    "ReconstructionCampaignResourceAuditV1",
    "ReconstructionPhysicalPackingReviewV1",
    "ReconstructionResourceAuditError",
    "ReconstructionResourceEnvelopeV1",
    "ReconstructionResourceMeasurementCorpusV1",
    "ReconstructionResourceMeasurementV1",
    "ReconstructionResourceProbeV1",
    "ReconstructionResourceRuntimeTelemetryV1",
    "ReconstructionStorageQualificationV1",
    "admit_campaign_resources",
    "build_campaign_resource_audit",
    "build_campaign_resource_audit_from_spec",
    "build_resource_measurement_corpus",
    "fit_resource_envelopes",
    "forecast_campaign_resources",
    "measure_reconstruction_resource_probe",
    "read_campaign_resource_audit",
    "read_resource_measurement_corpus",
    "read_storage_qualification",
    "review_physical_packing",
    "write_campaign_resource_audit",
    "write_resource_measurement_corpus",
    "write_storage_qualification",
]
