"""Atomic persistence for narrow reconstructed event products.

This module is the durable boundary between a fully validated delivered group
and the final reconstructed tick archive.  Version one retains the legacy
broker-rendered contract; version two accepts an explicit generic delivery
without inventing a broker identity.  Both write only the exact
``SyntheticEventV1`` logical schema plus compact manifests.  Version-three
synthetic-delta Parquet omits only deterministic ``event_id`` and
``anchor_interval_id`` columns and reconstructs them from their retained
identity inputs on every verified read.  Analytical feature frames, candidate
rows, and rejection rows are never accepted by this API.

Publication is a directory-level transaction on one filesystem.  Parquet
partitions and the manifest are written below undiscoverable scratch, validated
there, and promoted with one atomic rename.  Discovery looks only below
``commits`` and therefore cannot advertise partial output.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
import shutil
import sys
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path, PurePosixPath
from typing import Any, cast
from urllib.parse import quote

from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.broker_transfer import (
    BrokerRenderedGroupV1,
    BrokerTransferStatus,
)
from histdatacom.synthetic.contracts import (
    SYNTHETIC_EVENT_ARROW_COLUMNS,
    SYNTHETIC_EVENT_SCHEMA_VERSION,
    SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION,
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    SyntheticEventV1,
    canonical_contract_json,
    derive_anchor_interval_id,
    synthetic_event_arrow_schema,
    synthetic_event_stream_to_arrow,
)
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyValidationReportV1,
    CrossCurrencyValidationStage,
)
from histdatacom.synthetic.delivery import (
    ReconstructionDeliveredGroupV1,
    ReconstructionDeliveryMode,
    ReconstructionDeliveryStatus,
    reconstruction_streams_content_sha256,
)
from histdatacom.synthetic.streaming import ReconstructionStoragePolicyV1

RECONSTRUCTION_PRODUCT_SCHEMA_VERSION = "histdatacom.reconstruction-product.v1"
RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION = (
    "histdatacom.reconstruction-product.v2"
)
RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION = (
    "histdatacom.reconstruction-product.v3"
)
RECONSTRUCTION_OBSERVED_ANCHOR_SEGMENT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-observed-anchor-segment.v1"
)
RECONSTRUCTION_PARTITION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-product-partition.v1"
)
RECONSTRUCTION_SOURCE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-source-manifest.v1"
)
RECONSTRUCTION_CONSTRAINT_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-constraint-manifest.v1"
)
RECONSTRUCTION_QUALITY_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-quality-manifest.v1"
)
RECONSTRUCTION_DELIVERY_QUALITY_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-delivery-quality-manifest.v1"
)
RECONSTRUCTION_REPLAY_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-replay-manifest.v1"
)
RECONSTRUCTION_REPLAY_V2_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-replay-manifest.v2"
)
RECONSTRUCTION_RETENTION_PLAN_SCHEMA_VERSION = (
    "histdatacom.reconstruction-retention-plan.v1"
)
RECONSTRUCTION_ENSEMBLE_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-ensemble-product-manifest.v1"
)

RECONSTRUCTION_PRODUCT_DIRECTORY = "reconstruction-products"
RECONSTRUCTION_MANIFEST_FILENAME = "manifest.json"
RECONSTRUCTION_MANIFEST_ARTIFACT_KIND = "reconstruction-product-manifest"
RECONSTRUCTION_PARQUET_ARTIFACT_KIND = "reconstruction-product-parquet"
RECONSTRUCTION_LOGICAL_HASH_ALGORITHM = "sha256-canonical-event-json-lines-v1"
RECONSTRUCTION_BYTE_HASH_ALGORITHM = "sha256-path-byte-digests-v1"
RECONSTRUCTION_WRITER_ID = "histdatacom.pyarrow-parquet-zstd.v1"
RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_V1_ID = (
    "histdatacom.pyarrow-parquet-zstd.synthetic-delta.v1"
)
RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_ID = (
    "histdatacom.pyarrow-parquet-zstd.synthetic-delta-derived-ids.v2"
)
SUPPORTED_RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_IDS = frozenset(
    {
        RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_V1_ID,
        RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_ID,
    }
)
RECONSTRUCTION_SYNTHETIC_DELTA_DERIVED_COLUMNS = (
    "anchor_interval_id",
    "event_id",
    "left_anchor_event_id",
    "right_anchor_event_id",
)
_SYNTHETIC_DELTA_ANCHOR_ORDINAL_COLUMNS = (
    "left_anchor_ordinal",
    "right_anchor_ordinal",
)
_SYNTHETIC_DELTA_STORAGE_METADATA_KEY = (
    b"histdatacom.synthetic_delta_storage_schema"
)
_SYNTHETIC_DELTA_STORAGE_SCHEMA_VERSION = (
    "histdatacom.synthetic-delta-storage.v2"
)
RECONSTRUCTION_COMPRESSION = "zstd"
DEFAULT_RECONSTRUCTION_ROW_GROUP_SIZE = 65_536
DEFAULT_ESTIMATED_BYTES_PER_EVENT = 256
DEFAULT_ESTIMATED_COMPRESSION_RATIO = 0.35
MAX_RECONSTRUCTION_BENCHMARK_EVIDENCE_BYTES = 64 * 1024
DEFAULT_MANIFEST_BYTES_PER_PARTITION = 4_096
DEFAULT_MANIFEST_BYTES_PER_PRODUCT = 32_768
MAX_RECONSTRUCTION_PARTITIONS = 4_096
MAX_RECONSTRUCTION_MANIFEST_BYTES = 4 * 1024**2
MAX_RECONSTRUCTION_TEXT = 1_024

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_EVENT_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_LOGICAL_HASH_HEADER = (RECONSTRUCTION_LOGICAL_HASH_ALGORITHM + "\n").encode(
    "ascii"
)
_OBSERVED_HASH_HEADER = b"histdatacom-observed-anchors-v1\n"


class ReconstructionPersistenceError(ValueError):
    """Base failure for a refused final-product publication."""


class ReconstructionStoragePreflightError(ReconstructionPersistenceError):
    """A final-product estimate exceeds its retention/storage policy."""

    def __init__(
        self,
        estimate: ReconstructionRetentionPlanV1,
        violations: Sequence[str],
    ) -> None:
        self.estimate = estimate
        self.violations = tuple(violations)
        super().__init__(
            "reconstruction persistence preflight failed: "
            + "; ".join(self.violations)
        )


@dataclass(frozen=True, slots=True)
class ReconstructionRetentionPlanV1:
    """Pre-run primary/retained-member storage estimate and policy binding."""

    run_id: str
    primary_member_id: str
    retained_member_ids: tuple[str, ...]
    member_event_counts: Mapping[str, int]
    estimated_partition_count: int
    estimated_bytes_per_event: int
    estimated_compression_ratio: float
    estimated_primary_bytes: int
    estimated_retained_bytes: int
    estimated_manifest_bytes: int
    estimated_total_output_bytes: int
    storage_policy_id: str
    member_physical_event_counts: Mapping[str, int] = field(
        default_factory=dict
    )
    estimated_product_count: int = 0
    plan_id: str = ""
    schema_version: str = RECONSTRUCTION_RETENTION_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_RETENTION_PLAN_SCHEMA_VERSION,
            "reconstruction retention plan",
        )
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        object.__setattr__(
            self,
            "primary_member_id",
            _required_text(self.primary_member_id),
        )
        retained = _normalized_text_tuple(self.retained_member_ids)
        if not retained or self.primary_member_id not in retained:
            raise ValueError("retained members must include the primary member")
        object.__setattr__(self, "retained_member_ids", retained)
        counts = {
            _required_text(member): _nonnegative_int(
                count, f"member_event_counts.{member}"
            )
            for member, count in self.member_event_counts.items()
        }
        if set(counts) != set(retained):
            raise ValueError(
                "member event estimates must cover retained members exactly"
            )
        object.__setattr__(
            self, "member_event_counts", dict(sorted(counts.items()))
        )
        supplied_physical = dict(self.member_physical_event_counts)
        physical_counts = (
            {
                _required_text(member): _nonnegative_int(
                    count, f"member_physical_event_counts.{member}"
                )
                for member, count in supplied_physical.items()
            }
            if supplied_physical
            else dict(counts)
        )
        if set(physical_counts) != set(retained):
            raise ValueError(
                "physical member event estimates must cover retained members exactly"
            )
        if any(physical_counts[member] > counts[member] for member in retained):
            raise ValueError(
                "physical member events exceed logical member events"
            )
        object.__setattr__(
            self,
            "member_physical_event_counts",
            dict(sorted(physical_counts.items())),
        )
        for name in (
            "estimated_partition_count",
            "estimated_product_count",
            "estimated_bytes_per_event",
            "estimated_primary_bytes",
            "estimated_retained_bytes",
            "estimated_manifest_bytes",
            "estimated_total_output_bytes",
        ):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        has_estimated_events = any(physical_counts.values())
        if (self.estimated_partition_count == 0) != (not has_estimated_events):
            raise ValueError(
                "zero-event retention plans must have zero partitions"
            )
        if self.estimated_bytes_per_event < 1:
            raise ValueError("estimated_bytes_per_event must be positive")
        ratio = _finite_float(
            self.estimated_compression_ratio,
            "estimated_compression_ratio",
        )
        if not 0.0 < ratio <= 1.0:
            raise ValueError("estimated_compression_ratio must be in (0, 1]")
        object.__setattr__(self, "estimated_compression_ratio", ratio)
        expected_primary = _estimated_event_bytes(
            physical_counts[self.primary_member_id],
            self.estimated_bytes_per_event,
            ratio,
        )
        expected_retained = sum(
            _estimated_event_bytes(
                physical_counts[member], self.estimated_bytes_per_event, ratio
            )
            for member in retained
        )
        if self.estimated_primary_bytes != expected_primary:
            raise ValueError("estimated primary bytes do not reconcile")
        if self.estimated_retained_bytes != expected_retained:
            raise ValueError("estimated retained bytes do not reconcile")
        if self.estimated_total_output_bytes != (
            self.estimated_retained_bytes + self.estimated_manifest_bytes
        ):
            raise ValueError("estimated total output bytes do not reconcile")
        object.__setattr__(
            self,
            "storage_policy_id",
            _required_text(self.storage_policy_id),
        )
        expected = _stable_id("reconstruction-retention", self.payload())
        supplied = _optional_text(self.plan_id)
        if supplied is not None and supplied != expected:
            raise ValueError("retention plan_id differs from its content")
        object.__setattr__(self, "plan_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return deterministic estimate fields."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "primary_member_id": self.primary_member_id,
            "retained_member_ids": list(self.retained_member_ids),
            "member_event_counts": dict(self.member_event_counts),
            "estimated_partition_count": self.estimated_partition_count,
            "estimated_bytes_per_event": self.estimated_bytes_per_event,
            "estimated_compression_ratio": (self.estimated_compression_ratio),
            "estimated_primary_bytes": self.estimated_primary_bytes,
            "estimated_retained_bytes": self.estimated_retained_bytes,
            "estimated_manifest_bytes": self.estimated_manifest_bytes,
            "estimated_total_output_bytes": (self.estimated_total_output_bytes),
            "storage_policy_id": self.storage_policy_id,
            "estimate_basis": "event-count-compression-upper-bound-v1",
        }
        if dict(self.member_physical_event_counts) != dict(
            self.member_event_counts
        ):
            payload["member_physical_event_counts"] = dict(
                self.member_physical_event_counts
            )
            payload["physical_storage_layout"] = (
                "synthetic-delta-with-referenced-observed-anchors-v1"
            )
        if self.estimated_product_count:
            payload["estimated_product_count"] = self.estimated_product_count
            payload["manifest_estimate_basis"] = (
                "partition-plus-product-manifest-upper-bound-v1"
            )
        return payload

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the compact retention plan."""
        return {**self.payload(), "plan_id": self.plan_id}

    def to_json(self) -> str:
        """Return deterministic JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionRetentionPlanV1:
        """Restore and verify a retention plan."""
        _require_schema(data, RECONSTRUCTION_RETENTION_PLAN_SCHEMA_VERSION)
        _require_derived(
            data,
            "estimate_basis",
            "event-count-compression-upper-bound-v1",
        )
        if "member_physical_event_counts" in data:
            _require_derived(
                data,
                "physical_storage_layout",
                "synthetic-delta-with-referenced-observed-anchors-v1",
            )
        if "estimated_product_count" in data:
            _require_derived(
                data,
                "manifest_estimate_basis",
                "partition-plus-product-manifest-upper-bound-v1",
            )
        return cls(
            run_id=str(data.get("run_id", "")),
            primary_member_id=str(data.get("primary_member_id", "")),
            retained_member_ids=_string_tuple(
                data.get("retained_member_ids"), "retained_member_ids"
            ),
            member_event_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("member_event_counts"), "member_event_counts"
                ).items()
            },
            estimated_partition_count=_strict_int(
                data.get("estimated_partition_count"),
                "estimated_partition_count",
            ),
            estimated_bytes_per_event=_strict_int(
                data.get("estimated_bytes_per_event"),
                "estimated_bytes_per_event",
            ),
            estimated_compression_ratio=_strict_float(
                data.get("estimated_compression_ratio"),
                "estimated_compression_ratio",
            ),
            estimated_primary_bytes=_strict_int(
                data.get("estimated_primary_bytes"),
                "estimated_primary_bytes",
            ),
            estimated_retained_bytes=_strict_int(
                data.get("estimated_retained_bytes"),
                "estimated_retained_bytes",
            ),
            estimated_manifest_bytes=_strict_int(
                data.get("estimated_manifest_bytes"),
                "estimated_manifest_bytes",
            ),
            estimated_total_output_bytes=_strict_int(
                data.get("estimated_total_output_bytes"),
                "estimated_total_output_bytes",
            ),
            storage_policy_id=str(data.get("storage_policy_id", "")),
            member_physical_event_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("member_physical_event_counts", {}),
                    "member_physical_event_counts",
                ).items()
            },
            estimated_product_count=_strict_int(
                data.get("estimated_product_count", 0),
                "estimated_product_count",
            ),
            plan_id=str(data.get("plan_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


def estimate_reconstruction_retention(
    *,
    run_id: str,
    primary_member_id: str,
    retained_member_event_counts: Mapping[str, int],
    retained_member_physical_event_counts: Mapping[str, int] | None = None,
    estimated_partition_count: int,
    estimated_product_count: int = 0,
    storage_policy: ReconstructionStoragePolicyV1,
    estimated_bytes_per_event: int = DEFAULT_ESTIMATED_BYTES_PER_EVENT,
    estimated_compression_ratio: float = (DEFAULT_ESTIMATED_COMPRESSION_RATIO),
) -> ReconstructionRetentionPlanV1:
    """Estimate the primary and all retained outputs before reconstruction."""
    if not isinstance(storage_policy, ReconstructionStoragePolicyV1):
        raise TypeError("storage preflight requires a v1 storage policy")
    counts = {
        _required_text(member): _nonnegative_int(count, str(member))
        for member, count in retained_member_event_counts.items()
    }
    physical_counts = (
        {
            _required_text(member): _nonnegative_int(count, str(member))
            for member, count in retained_member_physical_event_counts.items()
        }
        if retained_member_physical_event_counts is not None
        else dict(counts)
    )
    if set(physical_counts) != set(counts):
        raise ValueError(
            "physical retained estimates must cover logical members exactly"
        )
    if any(physical_counts[member] > counts[member] for member in counts):
        raise ValueError("physical retained estimates exceed logical estimates")
    primary = _required_text(primary_member_id)
    if primary not in counts:
        raise ValueError("primary member is absent from retained estimates")
    partition_count = _nonnegative_int(
        estimated_partition_count, "estimated_partition_count"
    )
    product_count = _nonnegative_int(
        estimated_product_count, "estimated_product_count"
    )
    bytes_per_event = _positive_int(
        estimated_bytes_per_event, "estimated_bytes_per_event"
    )
    ratio = _finite_float(
        estimated_compression_ratio, "estimated_compression_ratio"
    )
    if not 0.0 < ratio <= 1.0:
        raise ValueError("estimated_compression_ratio must be in (0, 1]")
    primary_bytes = _estimated_event_bytes(
        physical_counts[primary], bytes_per_event, ratio
    )
    retained_bytes = sum(
        _estimated_event_bytes(value, bytes_per_event, ratio)
        for value in physical_counts.values()
    )
    manifest_bytes = (
        partition_count * DEFAULT_MANIFEST_BYTES_PER_PARTITION
        + product_count * DEFAULT_MANIFEST_BYTES_PER_PRODUCT
    )
    plan = ReconstructionRetentionPlanV1(
        run_id=run_id,
        primary_member_id=primary,
        retained_member_ids=tuple(counts),
        member_event_counts=counts,
        estimated_partition_count=partition_count,
        estimated_bytes_per_event=bytes_per_event,
        estimated_compression_ratio=ratio,
        estimated_primary_bytes=primary_bytes,
        estimated_retained_bytes=retained_bytes,
        estimated_manifest_bytes=manifest_bytes,
        estimated_total_output_bytes=retained_bytes + manifest_bytes,
        storage_policy_id=storage_policy.policy_id,
        member_physical_event_counts=physical_counts,
        estimated_product_count=product_count,
    )
    violations: list[str] = []
    if len(counts) > storage_policy.max_retained_ensemble_members:
        violations.append(
            f"retained members {len(counts)} exceed "
            f"{storage_policy.max_retained_ensemble_members}"
        )
    if plan.estimated_total_output_bytes > storage_policy.max_output_bytes:
        violations.append(
            f"estimated output {plan.estimated_total_output_bytes} exceeds "
            f"{storage_policy.max_output_bytes}"
        )
    if partition_count > MAX_RECONSTRUCTION_PARTITIONS:
        violations.append(
            f"estimated partitions {partition_count} exceed "
            f"{MAX_RECONSTRUCTION_PARTITIONS}"
        )
    if violations:
        raise ReconstructionStoragePreflightError(plan, violations)
    return plan


@dataclass(frozen=True, slots=True)
class ReconstructionEnsembleManifestV1:
    """Compact retained-ensemble identity for one materialized member."""

    run_id: str
    materialized_member_id: str
    primary_member_id: str
    retained_member_ids: tuple[str, ...]
    member_event_estimates: Mapping[str, int]
    retention_plan_id: str
    ensemble_manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_ENSEMBLE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_ENSEMBLE_MANIFEST_SCHEMA_VERSION,
            "reconstruction ensemble product manifest",
        )
        for name in (
            "run_id",
            "materialized_member_id",
            "primary_member_id",
            "retention_plan_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        retained = _normalized_text_tuple(self.retained_member_ids)
        if (
            not retained
            or self.primary_member_id not in retained
            or self.materialized_member_id not in retained
        ):
            raise ValueError(
                "ensemble manifest members differ from retained membership"
            )
        object.__setattr__(self, "retained_member_ids", retained)
        estimates = {
            _required_text(member): _nonnegative_int(
                count, f"member_event_estimates.{member}"
            )
            for member, count in self.member_event_estimates.items()
        }
        if set(estimates) != set(retained):
            raise ValueError(
                "ensemble event estimates do not cover retained members"
            )
        object.__setattr__(
            self, "member_event_estimates", dict(sorted(estimates.items()))
        )
        expected = _stable_id("reconstruction-ensemble", self.payload())
        supplied = _optional_text(self.ensemble_manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("ensemble manifest_id differs")
        object.__setattr__(self, "ensemble_manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return compact retained-member identity."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "materialized_member_id": self.materialized_member_id,
            "primary_member_id": self.primary_member_id,
            "retained_member_ids": list(self.retained_member_ids),
            "member_event_estimates": dict(self.member_event_estimates),
            "retention_plan_id": self.retention_plan_id,
            "automatic_winner": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact ensemble-manifest JSON."""
        return {
            **self.payload(),
            "ensemble_manifest_id": self.ensemble_manifest_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionEnsembleManifestV1:
        """Restore and verify retained-ensemble identity."""
        _require_schema(data, RECONSTRUCTION_ENSEMBLE_MANIFEST_SCHEMA_VERSION)
        _require_derived(data, "automatic_winner", False)
        return cls(
            run_id=str(data.get("run_id", "")),
            materialized_member_id=str(data.get("materialized_member_id", "")),
            primary_member_id=str(data.get("primary_member_id", "")),
            retained_member_ids=_string_tuple(
                data.get("retained_member_ids"), "retained_member_ids"
            ),
            member_event_estimates={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("member_event_estimates"),
                    "member_event_estimates",
                ).items()
            },
            retention_plan_id=str(data.get("retention_plan_id", "")),
            ensemble_manifest_id=str(data.get("ensemble_manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionProductPartitionV1:
    """Physical and logical evidence for one symbol/date Parquet file."""

    relative_path: str
    symbol: str
    event_date: str
    stream_id: str
    source_version_ids: tuple[str, ...]
    row_count: int
    observed_event_count: int
    synthetic_event_count: int
    min_event_time_ns: int
    max_event_time_ns: int
    logical_content_sha256: str
    byte_sha256: str
    size_bytes: int
    row_group_count: int
    partition_id: str = ""
    schema_version: str = RECONSTRUCTION_PARTITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_PARTITION_SCHEMA_VERSION,
            "reconstruction product partition",
        )
        symbol = _normalized_symbol(self.symbol)
        object.__setattr__(self, "symbol", symbol)
        event_date = _required_event_date(self.event_date)
        object.__setattr__(self, "event_date", event_date)
        path = _safe_relative_path(self.relative_path)
        expected_path = _partition_relative_path(symbol, event_date)
        if path != expected_path:
            raise ValueError("partition relative path differs from layout")
        object.__setattr__(self, "relative_path", path)
        object.__setattr__(self, "stream_id", _required_text(self.stream_id))
        sources = _normalized_text_tuple(self.source_version_ids)
        if not sources:
            raise ValueError("partition requires source version IDs")
        object.__setattr__(self, "source_version_ids", sources)
        for name in (
            "row_count",
            "observed_event_count",
            "synthetic_event_count",
            "size_bytes",
            "row_group_count",
        ):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        if self.row_count < 1 or self.row_group_count < 1:
            raise ValueError("durable partitions cannot be empty")
        if self.row_count != (
            self.observed_event_count + self.synthetic_event_count
        ):
            raise ValueError("partition origin counts do not reconcile")
        minimum = _strict_int(self.min_event_time_ns, "min_event_time_ns")
        maximum = _strict_int(self.max_event_time_ns, "max_event_time_ns")
        if maximum < minimum:
            raise ValueError("partition event-time bounds are reversed")
        if (
            _event_date(minimum) != event_date
            or _event_date(maximum) != event_date
        ):
            raise ValueError("partition rows cross the event-date boundary")
        object.__setattr__(self, "min_event_time_ns", minimum)
        object.__setattr__(self, "max_event_time_ns", maximum)
        for name in ("logical_content_sha256", "byte_sha256"):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name), name)
            )
        expected = _stable_id("reconstruction-partition", self.payload())
        supplied = _optional_text(self.partition_id)
        if supplied is not None and supplied != expected:
            raise ValueError("partition_id differs from partition evidence")
        object.__setattr__(self, "partition_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return deterministic partition evidence."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "stream_schema_version": SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION,
            "relative_path": self.relative_path,
            "symbol": self.symbol,
            "event_date": self.event_date,
            "stream_id": self.stream_id,
            "source_version_ids": list(self.source_version_ids),
            "row_count": self.row_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "min_event_time_ns": self.min_event_time_ns,
            "max_event_time_ns": self.max_event_time_ns,
            "logical_content_sha256": self.logical_content_sha256,
            "byte_sha256": self.byte_sha256,
            "size_bytes": self.size_bytes,
            "row_group_count": self.row_group_count,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact partition JSON."""
        return {**self.payload(), "partition_id": self.partition_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionProductPartitionV1:
        """Restore and verify partition evidence."""
        _require_schema(data, RECONSTRUCTION_PARTITION_SCHEMA_VERSION)
        _require_derived(
            data, "event_schema_version", SYNTHETIC_EVENT_SCHEMA_VERSION
        )
        _require_derived(
            data,
            "stream_schema_version",
            SYNTHETIC_EVENT_STREAM_SCHEMA_VERSION,
        )
        return cls(
            relative_path=str(data.get("relative_path", "")),
            symbol=str(data.get("symbol", "")),
            event_date=str(data.get("event_date", "")),
            stream_id=str(data.get("stream_id", "")),
            source_version_ids=_string_tuple(
                data.get("source_version_ids"), "source_version_ids"
            ),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count"), "synthetic_event_count"
            ),
            min_event_time_ns=_strict_int(
                data.get("min_event_time_ns"), "min_event_time_ns"
            ),
            max_event_time_ns=_strict_int(
                data.get("max_event_time_ns"), "max_event_time_ns"
            ),
            logical_content_sha256=str(data.get("logical_content_sha256", "")),
            byte_sha256=str(data.get("byte_sha256", "")),
            size_bytes=_strict_int(data.get("size_bytes"), "size_bytes"),
            row_group_count=_strict_int(
                data.get("row_group_count"), "row_group_count"
            ),
            partition_id=str(data.get("partition_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionSourceManifestV1:
    """Compact immutable-source and observed-anchor evidence."""

    source_version_ids: tuple[str, ...]
    source_series_ids: tuple[str, ...]
    source_periods: tuple[str, ...]
    observed_event_count: int
    observed_content_sha256: str
    observed_event_ids_sha256: str
    experiment_id: str | None = None
    source_manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_SOURCE_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_SOURCE_MANIFEST_SCHEMA_VERSION,
            "reconstruction source manifest",
        )
        versions = _normalized_text_tuple(self.source_version_ids)
        if not versions:
            raise ValueError("source manifest requires source versions")
        object.__setattr__(self, "source_version_ids", versions)
        object.__setattr__(
            self,
            "source_series_ids",
            _normalized_text_tuple(self.source_series_ids),
        )
        object.__setattr__(
            self,
            "source_periods",
            _normalized_text_tuple(self.source_periods),
        )
        object.__setattr__(
            self,
            "observed_event_count",
            _nonnegative_int(self.observed_event_count, "observed_event_count"),
        )
        for name in ("observed_content_sha256", "observed_event_ids_sha256"):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name), name)
            )
        object.__setattr__(
            self, "experiment_id", _optional_text(self.experiment_id)
        )
        expected = _stable_id("reconstruction-source", self.payload())
        supplied = _optional_text(self.source_manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("source manifest_id differs")
        object.__setattr__(self, "source_manifest_id", expected)

    @property
    def observed_values_verified_exactly(self) -> bool:
        """Return the immutable-anchor verification invariant."""
        return True

    def payload(self) -> dict[str, JSONValue]:
        """Return immutable-source evidence."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "source_version_ids": list(self.source_version_ids),
            "source_series_ids": list(self.source_series_ids),
            "source_periods": list(self.source_periods),
            "observed_event_count": self.observed_event_count,
            "observed_content_sha256": self.observed_content_sha256,
            "observed_event_ids_sha256": self.observed_event_ids_sha256,
            "observed_values_verified_exactly": self.observed_values_verified_exactly,
        }
        if self.experiment_id is not None:
            payload["experiment_id"] = self.experiment_id
        return payload

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact source-manifest JSON."""
        return {**self.payload(), "source_manifest_id": self.source_manifest_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionSourceManifestV1:
        """Restore and verify source evidence."""
        _require_schema(data, RECONSTRUCTION_SOURCE_MANIFEST_SCHEMA_VERSION)
        _require_derived(data, "observed_values_verified_exactly", True)
        return cls(
            source_version_ids=_string_tuple(
                data.get("source_version_ids"), "source_version_ids"
            ),
            source_series_ids=_string_tuple(
                data.get("source_series_ids"), "source_series_ids"
            ),
            source_periods=_string_tuple(
                data.get("source_periods"), "source_periods"
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            observed_content_sha256=str(
                data.get("observed_content_sha256", "")
            ),
            observed_event_ids_sha256=str(
                data.get("observed_event_ids_sha256", "")
            ),
            experiment_id=_optional_text(data.get("experiment_id")),
            source_manifest_id=str(data.get("source_manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionConstraintManifestV1:
    """Compact generator, constraint, epoch, motif, and reference lineage."""

    synthetic_event_count: int
    constraint_set_ids: tuple[str, ...]
    generator_ids: tuple[str, ...]
    generator_versions: tuple[str, ...]
    generator_config_ids: tuple[str, ...]
    feed_epoch_ids: tuple[str, ...]
    reference_assignment_count: int
    reference_assignments_sha256: str
    motif_assignment_count: int
    motif_assignments_sha256: str
    constraint_manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_CONSTRAINT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_CONSTRAINT_MANIFEST_SCHEMA_VERSION,
            "reconstruction constraint manifest",
        )
        count = _nonnegative_int(
            self.synthetic_event_count, "synthetic_event_count"
        )
        object.__setattr__(self, "synthetic_event_count", count)
        for name in (
            "constraint_set_ids",
            "generator_ids",
            "generator_versions",
            "generator_config_ids",
            "feed_epoch_ids",
        ):
            object.__setattr__(
                self, name, _normalized_text_tuple(getattr(self, name))
            )
        for name in ("reference_assignment_count", "motif_assignment_count"):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        for name in (
            "reference_assignments_sha256",
            "motif_assignments_sha256",
        ):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name), name)
            )
        if (
            self.reference_assignment_count > count
            or self.motif_assignment_count > count
        ):
            raise ValueError("lineage assignment counts exceed synthetic rows")
        if count and not (
            self.constraint_set_ids
            and self.generator_ids
            and self.generator_versions
            and self.generator_config_ids
        ):
            raise ValueError(
                "synthetic rows require generator and constraint lineage"
            )
        expected = _stable_id("reconstruction-constraint", self.payload())
        supplied = _optional_text(self.constraint_manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("constraint manifest_id differs")
        object.__setattr__(self, "constraint_manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return compact constraint lineage."""
        return {
            "schema_version": self.schema_version,
            "synthetic_event_count": self.synthetic_event_count,
            "constraint_set_ids": list(self.constraint_set_ids),
            "generator_ids": list(self.generator_ids),
            "generator_versions": list(self.generator_versions),
            "generator_config_ids": list(self.generator_config_ids),
            "feed_epoch_ids": list(self.feed_epoch_ids),
            "reference_assignment_count": self.reference_assignment_count,
            "reference_assignments_sha256": self.reference_assignments_sha256,
            "motif_assignment_count": self.motif_assignment_count,
            "motif_assignments_sha256": self.motif_assignments_sha256,
            "candidate_rows_inline": False,
            "rejected_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact constraint-manifest JSON."""
        return {
            **self.payload(),
            "constraint_manifest_id": self.constraint_manifest_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionConstraintManifestV1:
        """Restore and verify constraint lineage."""
        _require_schema(data, RECONSTRUCTION_CONSTRAINT_MANIFEST_SCHEMA_VERSION)
        _require_derived(data, "candidate_rows_inline", False)
        _require_derived(data, "rejected_rows_inline", False)
        return cls(
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count"), "synthetic_event_count"
            ),
            constraint_set_ids=_string_tuple(
                data.get("constraint_set_ids"), "constraint_set_ids"
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
            feed_epoch_ids=_string_tuple(
                data.get("feed_epoch_ids"), "feed_epoch_ids"
            ),
            reference_assignment_count=_strict_int(
                data.get("reference_assignment_count"),
                "reference_assignment_count",
            ),
            reference_assignments_sha256=str(
                data.get("reference_assignments_sha256", "")
            ),
            motif_assignment_count=_strict_int(
                data.get("motif_assignment_count"), "motif_assignment_count"
            ),
            motif_assignments_sha256=str(
                data.get("motif_assignments_sha256", "")
            ),
            constraint_manifest_id=str(data.get("constraint_manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionQualityManifestV1:
    """Passing broker-transfer, validation, and quality evidence."""

    broker_transfer_manifest_id: str
    broker_fingerprint_id: str
    transfer_output_content_sha256: str
    post_broker_validation_id: str
    post_broker_validation_status: str
    cross_instrument_quality_status: str
    cross_instrument_quality_sha256: str
    broker_observed_event_count: int
    broker_synthetic_event_count: int
    broker_lineage_count: int
    broker_lineage_content_sha256: str
    broker_action_counts: Mapping[str, int]
    benchmark_comparison_ids: tuple[str, ...]
    quality_manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_QUALITY_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_QUALITY_MANIFEST_SCHEMA_VERSION,
            "reconstruction quality manifest",
        )
        for name in (
            "broker_transfer_manifest_id",
            "broker_fingerprint_id",
            "post_broker_validation_id",
            "post_broker_validation_status",
            "cross_instrument_quality_status",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "transfer_output_content_sha256",
            _required_sha256(
                self.transfer_output_content_sha256,
                "transfer_output_content_sha256",
            ),
        )
        object.__setattr__(
            self,
            "cross_instrument_quality_sha256",
            _required_sha256(
                self.cross_instrument_quality_sha256,
                "cross_instrument_quality_sha256",
            ),
        )
        for name in (
            "broker_observed_event_count",
            "broker_synthetic_event_count",
            "broker_lineage_count",
        ):
            object.__setattr__(
                self,
                name,
                _nonnegative_int(getattr(self, name), name),
            )
        object.__setattr__(
            self,
            "broker_lineage_content_sha256",
            _required_sha256(
                self.broker_lineage_content_sha256,
                "broker_lineage_content_sha256",
            ),
        )
        actions = {
            _required_text(name): _positive_int(count, f"action.{name}")
            for name, count in self.broker_action_counts.items()
        }
        object.__setattr__(
            self, "broker_action_counts", dict(sorted(actions.items()))
        )
        if self.broker_lineage_count != self.broker_synthetic_event_count:
            raise ValueError("broker lineage and synthetic counts differ")
        comparisons = _normalized_text_tuple(self.benchmark_comparison_ids)
        object.__setattr__(self, "benchmark_comparison_ids", comparisons)
        if self.post_broker_validation_status != "passed":
            raise ValueError("post-broker validation is not passing")
        if self.cross_instrument_quality_status == "failed":
            raise ValueError("cross-instrument quality is failed")
        expected = _stable_id("reconstruction-quality", self.payload())
        supplied = _optional_text(self.quality_manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("quality manifest_id differs")
        object.__setattr__(self, "quality_manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return compact quality evidence."""
        return {
            "schema_version": self.schema_version,
            "broker_transfer_manifest_id": (self.broker_transfer_manifest_id),
            "broker_fingerprint_id": self.broker_fingerprint_id,
            "transfer_output_content_sha256": (
                self.transfer_output_content_sha256
            ),
            "post_broker_validation_id": self.post_broker_validation_id,
            "post_broker_validation_status": (
                self.post_broker_validation_status
            ),
            "cross_instrument_quality_status": (
                self.cross_instrument_quality_status
            ),
            "cross_instrument_quality_sha256": (
                self.cross_instrument_quality_sha256
            ),
            "broker_observed_event_count": self.broker_observed_event_count,
            "broker_synthetic_event_count": self.broker_synthetic_event_count,
            "broker_lineage_count": self.broker_lineage_count,
            "broker_lineage_content_sha256": (
                self.broker_lineage_content_sha256
            ),
            "broker_action_counts": dict(self.broker_action_counts),
            "benchmark_comparison_ids": list(self.benchmark_comparison_ids),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact quality-manifest JSON."""
        return {
            **self.payload(),
            "quality_manifest_id": self.quality_manifest_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionQualityManifestV1:
        """Restore and verify final quality evidence."""
        _require_schema(data, RECONSTRUCTION_QUALITY_MANIFEST_SCHEMA_VERSION)
        return cls(
            broker_transfer_manifest_id=str(
                data.get("broker_transfer_manifest_id", "")
            ),
            broker_fingerprint_id=str(data.get("broker_fingerprint_id", "")),
            transfer_output_content_sha256=str(
                data.get("transfer_output_content_sha256", "")
            ),
            post_broker_validation_id=str(
                data.get("post_broker_validation_id", "")
            ),
            post_broker_validation_status=str(
                data.get("post_broker_validation_status", "")
            ),
            cross_instrument_quality_status=str(
                data.get("cross_instrument_quality_status", "")
            ),
            cross_instrument_quality_sha256=str(
                data.get("cross_instrument_quality_sha256", "")
            ),
            broker_observed_event_count=_strict_int(
                data.get("broker_observed_event_count"),
                "broker_observed_event_count",
            ),
            broker_synthetic_event_count=_strict_int(
                data.get("broker_synthetic_event_count"),
                "broker_synthetic_event_count",
            ),
            broker_lineage_count=_strict_int(
                data.get("broker_lineage_count"), "broker_lineage_count"
            ),
            broker_lineage_content_sha256=str(
                data.get("broker_lineage_content_sha256", "")
            ),
            broker_action_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("broker_action_counts"), "broker_action_counts"
                ).items()
            },
            benchmark_comparison_ids=_string_tuple(
                data.get("benchmark_comparison_ids"),
                "benchmark_comparison_ids",
            ),
            quality_manifest_id=str(data.get("quality_manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionDeliveryQualityManifestV1:
    """Passing generic delivery, final validation, and benchmark evidence."""

    delivery_manifest_id: str
    delivery_profile_id: str
    delivery_mode: ReconstructionDeliveryMode
    delivery_output_content_sha256: str
    final_validation_id: str
    final_validation_status: str
    cross_instrument_quality_status: str
    cross_instrument_quality_sha256: str
    observed_event_count: int
    synthetic_event_count: int
    identity_event_count: int
    identity_lineage_sha256: str
    delivery_action_counts: Mapping[str, int]
    benchmark_artifact_ids: tuple[str, ...]
    benchmark_evidence: Mapping[str, JSONValue] = field(default_factory=dict)
    point_in_time_evidence_projection_ids: tuple[str, ...] = ()
    point_in_time_evidence_decision_ids: tuple[str, ...] = ()
    cross_series_constraint_bundle_ids: tuple[str, ...] = ()
    cross_series_constraint_window_ids: tuple[str, ...] = ()
    cross_series_constraint_decision_ids: tuple[str, ...] = ()
    quality_manifest_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_DELIVERY_QUALITY_MANIFEST_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_DELIVERY_QUALITY_MANIFEST_SCHEMA_VERSION,
            "reconstruction delivery quality manifest",
        )
        for name in (
            "delivery_manifest_id",
            "delivery_profile_id",
            "final_validation_id",
            "final_validation_status",
            "cross_instrument_quality_status",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "delivery_mode",
            ReconstructionDeliveryMode(self.delivery_mode),
        )
        for name in (
            "delivery_output_content_sha256",
            "cross_instrument_quality_sha256",
            "identity_lineage_sha256",
        ):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name), name)
            )
        for name in (
            "observed_event_count",
            "synthetic_event_count",
            "identity_event_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        actions = {
            _required_text(name): _positive_int(count, f"action.{name}")
            for name, count in self.delivery_action_counts.items()
        }
        object.__setattr__(
            self, "delivery_action_counts", dict(sorted(actions.items()))
        )
        if self.delivery_mode is ReconstructionDeliveryMode.MODERN_REFERENCE:
            if self.identity_event_count != self.synthetic_event_count:
                raise ValueError(
                    "identity delivery count differs from synthetic"
                )
            expected_actions = (
                {"identity": self.identity_event_count}
                if self.identity_event_count
                else {}
            )
            if actions != expected_actions:
                raise ValueError(
                    "modern delivery actions are not identity-only"
                )
        artifacts = _normalized_text_tuple(self.benchmark_artifact_ids)
        if not artifacts:
            raise ValueError("delivery quality lacks benchmark artifacts")
        object.__setattr__(self, "benchmark_artifact_ids", artifacts)
        object.__setattr__(
            self,
            "benchmark_evidence",
            _bounded_json_value_mapping(
                self.benchmark_evidence,
                "benchmark_evidence",
                maximum_bytes=MAX_RECONSTRUCTION_BENCHMARK_EVIDENCE_BYTES,
            ),
        )
        projections = _normalized_text_tuple(
            self.point_in_time_evidence_projection_ids
        )
        decisions = _normalized_text_tuple(
            self.point_in_time_evidence_decision_ids
        )
        cross_bundles = _normalized_text_tuple(
            self.cross_series_constraint_bundle_ids
        )
        cross_windows = _normalized_text_tuple(
            self.cross_series_constraint_window_ids
        )
        cross_decisions = _normalized_text_tuple(
            self.cross_series_constraint_decision_ids
        )
        if len(projections) > 64 or len(decisions) > 256:
            raise ValueError("delivery quality evidence lineage is unbounded")
        if (
            len(cross_bundles) > 64
            or len(cross_windows) > 256
            or len(cross_decisions) > 256
        ):
            raise ValueError(
                "delivery cross-series constraint lineage is unbounded"
            )
        object.__setattr__(
            self, "point_in_time_evidence_projection_ids", projections
        )
        object.__setattr__(
            self, "point_in_time_evidence_decision_ids", decisions
        )
        object.__setattr__(
            self, "cross_series_constraint_bundle_ids", cross_bundles
        )
        object.__setattr__(
            self, "cross_series_constraint_window_ids", cross_windows
        )
        object.__setattr__(
            self, "cross_series_constraint_decision_ids", cross_decisions
        )
        if self.final_validation_status != "passed":
            raise ValueError("final delivery validation is not passing")
        if self.cross_instrument_quality_status == "failed":
            raise ValueError("cross-instrument delivery quality is failed")
        expected = _stable_id("reconstruction-delivery-quality", self.payload())
        supplied = _optional_text(self.quality_manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("delivery quality manifest_id differs")
        object.__setattr__(self, "quality_manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return compact generic quality evidence."""
        payload: dict[str, JSONValue] = {
            "schema_version": self.schema_version,
            "delivery_manifest_id": self.delivery_manifest_id,
            "delivery_profile_id": self.delivery_profile_id,
            "delivery_mode": self.delivery_mode.value,
            "delivery_output_content_sha256": (
                self.delivery_output_content_sha256
            ),
            "final_validation_id": self.final_validation_id,
            "final_validation_status": self.final_validation_status,
            "cross_instrument_quality_status": (
                self.cross_instrument_quality_status
            ),
            "cross_instrument_quality_sha256": (
                self.cross_instrument_quality_sha256
            ),
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "identity_event_count": self.identity_event_count,
            "identity_lineage_sha256": self.identity_lineage_sha256,
            "delivery_action_counts": dict(self.delivery_action_counts),
            "benchmark_artifact_ids": list(self.benchmark_artifact_ids),
        }
        if self.benchmark_evidence:
            payload["benchmark_evidence"] = dict(self.benchmark_evidence)
        if self.point_in_time_evidence_projection_ids:
            payload["point_in_time_evidence_projection_ids"] = list(
                self.point_in_time_evidence_projection_ids
            )
        if self.point_in_time_evidence_decision_ids:
            payload["point_in_time_evidence_decision_ids"] = list(
                self.point_in_time_evidence_decision_ids
            )
        if self.cross_series_constraint_bundle_ids:
            payload["cross_series_constraint_bundle_ids"] = list(
                self.cross_series_constraint_bundle_ids
            )
        if self.cross_series_constraint_window_ids:
            payload["cross_series_constraint_window_ids"] = list(
                self.cross_series_constraint_window_ids
            )
        if self.cross_series_constraint_decision_ids:
            payload["cross_series_constraint_decision_ids"] = list(
                self.cross_series_constraint_decision_ids
            )
        return payload

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact JSON-compatible quality evidence."""
        return {
            **self.payload(),
            "quality_manifest_id": self.quality_manifest_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionDeliveryQualityManifestV1:
        """Restore and verify generic delivery quality evidence."""
        _require_schema(
            data, RECONSTRUCTION_DELIVERY_QUALITY_MANIFEST_SCHEMA_VERSION
        )
        return cls(
            delivery_manifest_id=str(data.get("delivery_manifest_id", "")),
            delivery_profile_id=str(data.get("delivery_profile_id", "")),
            delivery_mode=ReconstructionDeliveryMode(
                str(data.get("delivery_mode", ""))
            ),
            delivery_output_content_sha256=str(
                data.get("delivery_output_content_sha256", "")
            ),
            final_validation_id=str(data.get("final_validation_id", "")),
            final_validation_status=str(
                data.get("final_validation_status", "")
            ),
            cross_instrument_quality_status=str(
                data.get("cross_instrument_quality_status", "")
            ),
            cross_instrument_quality_sha256=str(
                data.get("cross_instrument_quality_sha256", "")
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count"), "synthetic_event_count"
            ),
            identity_event_count=_strict_int(
                data.get("identity_event_count"), "identity_event_count"
            ),
            identity_lineage_sha256=str(
                data.get("identity_lineage_sha256", "")
            ),
            delivery_action_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("delivery_action_counts"),
                    "delivery_action_counts",
                ).items()
            },
            benchmark_artifact_ids=_string_tuple(
                data.get("benchmark_artifact_ids"), "benchmark_artifact_ids"
            ),
            benchmark_evidence=_mapping(
                data.get("benchmark_evidence", {}), "benchmark_evidence"
            ),
            point_in_time_evidence_projection_ids=_string_tuple(
                data.get("point_in_time_evidence_projection_ids", ()),
                "point_in_time_evidence_projection_ids",
            ),
            point_in_time_evidence_decision_ids=_string_tuple(
                data.get("point_in_time_evidence_decision_ids", ()),
                "point_in_time_evidence_decision_ids",
            ),
            cross_series_constraint_bundle_ids=_string_tuple(
                data.get("cross_series_constraint_bundle_ids", ()),
                "cross_series_constraint_bundle_ids",
            ),
            cross_series_constraint_window_ids=_string_tuple(
                data.get("cross_series_constraint_window_ids", ()),
                "cross_series_constraint_window_ids",
            ),
            cross_series_constraint_decision_ids=_string_tuple(
                data.get("cross_series_constraint_decision_ids", ()),
                "cross_series_constraint_decision_ids",
            ),
            quality_manifest_id=str(data.get("quality_manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionReplayManifestV1:
    """Logical replay hash and pinned physical writer evidence."""

    logical_content_sha256: str
    partition_byte_sha256: str
    logical_hash_algorithm: str
    byte_hash_algorithm: str
    writer_id: str
    writer_library: str
    writer_library_version: str
    python_runtime: str
    compression: str
    row_group_size: int
    canonicalized_metadata_exclusions: tuple[str, ...]
    replay_manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_REPLAY_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_REPLAY_MANIFEST_SCHEMA_VERSION,
            "reconstruction replay manifest",
        )
        for name in ("logical_content_sha256", "partition_byte_sha256"):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name), name)
            )
        for name in (
            "logical_hash_algorithm",
            "byte_hash_algorithm",
            "writer_id",
            "writer_library",
            "writer_library_version",
            "python_runtime",
            "compression",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.logical_hash_algorithm != RECONSTRUCTION_LOGICAL_HASH_ALGORITHM:
            raise ValueError("unsupported reconstruction logical hash")
        if self.byte_hash_algorithm != RECONSTRUCTION_BYTE_HASH_ALGORITHM:
            raise ValueError("unsupported reconstruction byte hash")
        if self.writer_id != RECONSTRUCTION_WRITER_ID:
            raise ValueError("unsupported reconstruction writer")
        if self.compression != RECONSTRUCTION_COMPRESSION:
            raise ValueError("unsupported reconstruction compression")
        object.__setattr__(
            self,
            "row_group_size",
            _positive_int(self.row_group_size, "row_group_size"),
        )
        exclusions = _normalized_text_tuple(
            self.canonicalized_metadata_exclusions
        )
        object.__setattr__(
            self, "canonicalized_metadata_exclusions", exclusions
        )
        expected = _stable_id("reconstruction-replay", self.payload())
        supplied = _optional_text(self.replay_manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("replay manifest_id differs")
        object.__setattr__(self, "replay_manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return replay and writer evidence."""
        return {
            "schema_version": self.schema_version,
            "logical_content_sha256": self.logical_content_sha256,
            "partition_byte_sha256": self.partition_byte_sha256,
            "logical_hash_algorithm": self.logical_hash_algorithm,
            "byte_hash_algorithm": self.byte_hash_algorithm,
            "writer_id": self.writer_id,
            "writer_library": self.writer_library,
            "writer_library_version": self.writer_library_version,
            "python_runtime": self.python_runtime,
            "compression": self.compression,
            "row_group_size": self.row_group_size,
            "canonicalized_metadata_exclusions": list(
                self.canonicalized_metadata_exclusions
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact replay-manifest JSON."""
        return {**self.payload(), "replay_manifest_id": self.replay_manifest_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionReplayManifestV1:
        """Restore and verify replay evidence."""
        _require_schema(data, RECONSTRUCTION_REPLAY_MANIFEST_SCHEMA_VERSION)
        return cls(
            logical_content_sha256=str(data.get("logical_content_sha256", "")),
            partition_byte_sha256=str(data.get("partition_byte_sha256", "")),
            logical_hash_algorithm=str(data.get("logical_hash_algorithm", "")),
            byte_hash_algorithm=str(data.get("byte_hash_algorithm", "")),
            writer_id=str(data.get("writer_id", "")),
            writer_library=str(data.get("writer_library", "")),
            writer_library_version=str(data.get("writer_library_version", "")),
            python_runtime=str(data.get("python_runtime", "")),
            compression=str(data.get("compression", "")),
            row_group_size=_strict_int(
                data.get("row_group_size"), "row_group_size"
            ),
            canonicalized_metadata_exclusions=_string_tuple(
                data.get("canonicalized_metadata_exclusions"),
                "canonicalized_metadata_exclusions",
            ),
            replay_manifest_id=str(data.get("replay_manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionReplayManifestV2:
    """Logical replay plus synthetic-delta physical writer evidence."""

    logical_content_sha256: str
    partition_byte_sha256: str
    logical_hash_algorithm: str
    byte_hash_algorithm: str
    writer_id: str
    writer_library: str
    writer_library_version: str
    python_runtime: str
    compression: str
    row_group_size: int
    canonicalized_metadata_exclusions: tuple[str, ...]
    replay_manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_REPLAY_V2_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_REPLAY_V2_MANIFEST_SCHEMA_VERSION,
            "reconstruction replay v2 manifest",
        )
        for name in ("logical_content_sha256", "partition_byte_sha256"):
            object.__setattr__(
                self, name, _required_sha256(getattr(self, name), name)
            )
        for name in (
            "logical_hash_algorithm",
            "byte_hash_algorithm",
            "writer_id",
            "writer_library",
            "writer_library_version",
            "python_runtime",
            "compression",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        if self.logical_hash_algorithm != RECONSTRUCTION_LOGICAL_HASH_ALGORITHM:
            raise ValueError("unsupported reconstruction logical hash")
        if self.byte_hash_algorithm != RECONSTRUCTION_BYTE_HASH_ALGORITHM:
            raise ValueError("unsupported reconstruction byte hash")
        if (
            self.writer_id
            not in SUPPORTED_RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_IDS
        ):
            raise ValueError(
                "unsupported reconstruction synthetic-delta writer"
            )
        if self.compression != RECONSTRUCTION_COMPRESSION:
            raise ValueError("unsupported reconstruction compression")
        object.__setattr__(
            self,
            "row_group_size",
            _positive_int(self.row_group_size, "row_group_size"),
        )
        object.__setattr__(
            self,
            "canonicalized_metadata_exclusions",
            _normalized_text_tuple(self.canonicalized_metadata_exclusions),
        )
        expected = _stable_id("reconstruction-replay-v2", self.payload())
        supplied = _optional_text(self.replay_manifest_id)
        if supplied is not None and supplied != expected:
            raise ValueError("replay v2 manifest_id differs")
        object.__setattr__(self, "replay_manifest_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return replay and synthetic-delta writer evidence."""
        return {
            "schema_version": self.schema_version,
            "logical_content_sha256": self.logical_content_sha256,
            "partition_byte_sha256": self.partition_byte_sha256,
            "logical_hash_algorithm": self.logical_hash_algorithm,
            "byte_hash_algorithm": self.byte_hash_algorithm,
            "writer_id": self.writer_id,
            "writer_library": self.writer_library,
            "writer_library_version": self.writer_library_version,
            "python_runtime": self.python_runtime,
            "compression": self.compression,
            "row_group_size": self.row_group_size,
            "canonicalized_metadata_exclusions": list(
                self.canonicalized_metadata_exclusions
            ),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return compact replay v2 JSON."""
        return {**self.payload(), "replay_manifest_id": self.replay_manifest_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionReplayManifestV2:
        """Restore and verify synthetic-delta replay evidence."""
        _require_schema(data, RECONSTRUCTION_REPLAY_V2_MANIFEST_SCHEMA_VERSION)
        return cls(
            logical_content_sha256=str(data.get("logical_content_sha256", "")),
            partition_byte_sha256=str(data.get("partition_byte_sha256", "")),
            logical_hash_algorithm=str(data.get("logical_hash_algorithm", "")),
            byte_hash_algorithm=str(data.get("byte_hash_algorithm", "")),
            writer_id=str(data.get("writer_id", "")),
            writer_library=str(data.get("writer_library", "")),
            writer_library_version=str(data.get("writer_library_version", "")),
            python_runtime=str(data.get("python_runtime", "")),
            compression=str(data.get("compression", "")),
            row_group_size=_strict_int(
                data.get("row_group_size"), "row_group_size"
            ),
            canonicalized_metadata_exclusions=_string_tuple(
                data.get("canonicalized_metadata_exclusions"),
                "canonicalized_metadata_exclusions",
            ),
            replay_manifest_id=str(data.get("replay_manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionProductManifestV1:
    """One atomically committed synchronized reconstruction unit."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    broker_profile_id: str
    symbol_group_id: str
    symbols: tuple[str, ...]
    symbol_event_counts: Mapping[str, int]
    partitions: tuple[ReconstructionProductPartitionV1, ...]
    source: ReconstructionSourceManifestV1
    constraints: ReconstructionConstraintManifestV1
    quality: ReconstructionQualityManifestV1
    replay: ReconstructionReplayManifestV1
    ensemble: ReconstructionEnsembleManifestV1
    retention: ReconstructionRetentionPlanV1
    publication_id: str = ""
    manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_PRODUCT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_PRODUCT_SCHEMA_VERSION,
            "reconstruction product manifest",
        )
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
            "broker_profile_id",
            "symbol_group_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        symbols = tuple(
            sorted({_normalized_symbol(item) for item in self.symbols})
        )
        if not symbols:
            raise ValueError("product manifest requires symbols")
        object.__setattr__(self, "symbols", symbols)
        counts = {
            _normalized_symbol(symbol): _nonnegative_int(
                count, f"symbol_event_counts.{symbol}"
            )
            for symbol, count in self.symbol_event_counts.items()
        }
        if set(counts) != set(symbols):
            raise ValueError("symbol counts do not cover the product group")
        object.__setattr__(
            self, "symbol_event_counts", dict(sorted(counts.items()))
        )
        partitions = tuple(
            sorted(
                self.partitions,
                key=lambda item: (
                    item.symbol,
                    item.event_date,
                    item.partition_id,
                ),
            )
        )
        if not partitions or len(partitions) > MAX_RECONSTRUCTION_PARTITIONS:
            raise ValueError("product partition count is empty or unbounded")
        if len({item.relative_path for item in partitions}) != len(partitions):
            raise ValueError("product contains duplicate partition paths")
        actual_counts = dict.fromkeys(symbols, 0)
        for partition in partitions:
            if partition.symbol not in actual_counts:
                raise ValueError("partition symbol is outside product group")
            actual_counts[partition.symbol] += partition.row_count
        if actual_counts != counts:
            raise ValueError("product partition counts do not reconcile")
        object.__setattr__(self, "partitions", partitions)
        if not isinstance(self.source, ReconstructionSourceManifestV1):
            raise TypeError("product requires source-manifest evidence")
        if not isinstance(self.constraints, ReconstructionConstraintManifestV1):
            raise TypeError("product requires constraint-manifest evidence")
        if not isinstance(self.quality, ReconstructionQualityManifestV1):
            raise TypeError("product requires quality-manifest evidence")
        if not isinstance(self.replay, ReconstructionReplayManifestV1):
            raise TypeError("product requires replay-manifest evidence")
        if not isinstance(self.ensemble, ReconstructionEnsembleManifestV1):
            raise TypeError("product requires ensemble-manifest evidence")
        if not isinstance(self.retention, ReconstructionRetentionPlanV1):
            raise TypeError("product requires retention-plan evidence")
        if self.retention.run_id != self.run_id:
            raise ValueError("retention run differs from product run")
        if self.ensemble_member_id not in self.retention.retained_member_ids:
            raise ValueError("product member is not retained")
        if (
            self.ensemble.run_id != self.run_id
            or self.ensemble.materialized_member_id != self.ensemble_member_id
            or self.ensemble.primary_member_id
            != self.retention.primary_member_id
            or self.ensemble.retained_member_ids
            != self.retention.retained_member_ids
            or dict(self.ensemble.member_event_estimates)
            != dict(self.retention.member_event_counts)
            or self.ensemble.retention_plan_id != self.retention.plan_id
        ):
            raise ValueError(
                "ensemble manifest does not reconcile with retention preflight"
            )
        actual_rows = sum(counts.values())
        if len(partitions) > self.retention.estimated_partition_count:
            raise ValueError("product partitions exceed the preflight estimate")
        if (
            actual_rows
            > self.retention.member_event_counts[self.ensemble_member_id]
        ):
            raise ValueError("product rows exceed the preflight estimate")
        if (
            self.source.observed_event_count
            + (self.constraints.synthetic_event_count)
            != actual_rows
        ):
            raise ValueError("source/constraint counts do not reconcile")
        if self.quality.broker_observed_event_count != (
            self.source.observed_event_count
        ):
            raise ValueError("broker/source observed counts do not reconcile")
        if self.quality.broker_synthetic_event_count != (
            self.constraints.synthetic_event_count
        ):
            raise ValueError(
                "broker/constraint synthetic counts do not reconcile"
            )
        expected_publication = _stable_id(
            "reconstruction-publication", self.publication_payload()
        )
        supplied_publication = _optional_text(self.publication_id)
        if (
            supplied_publication is not None
            and supplied_publication != expected_publication
        ):
            raise ValueError("publication_id differs from logical identity")
        object.__setattr__(self, "publication_id", expected_publication)
        expected_manifest = _stable_id(
            "reconstruction-manifest", self.payload()
        )
        supplied_manifest = _optional_text(self.manifest_id)
        if (
            supplied_manifest is not None
            and supplied_manifest != expected_manifest
        ):
            raise ValueError("manifest_id differs from physical evidence")
        object.__setattr__(self, "manifest_id", expected_manifest)
        encoded = self.to_json().encode("utf-8")
        if len(encoded) > MAX_RECONSTRUCTION_MANIFEST_BYTES:
            raise ValueError("reconstruction manifest exceeds size limit")

    @property
    def event_count(self) -> int:
        """Return total durable events."""
        return sum(self.symbol_event_counts.values())

    @property
    def observed_event_count(self) -> int:
        """Return total immutable observed rows."""
        return self.source.observed_event_count

    @property
    def synthetic_event_count(self) -> int:
        """Return total accepted synthetic rows."""
        return self.constraints.synthetic_event_count

    @property
    def min_event_time_ns(self) -> int:
        """Return the first event time in the synchronized product."""
        return min(item.min_event_time_ns for item in self.partitions)

    @property
    def max_event_time_ns(self) -> int:
        """Return the last event time in the synchronized product."""
        return max(item.max_event_time_ns for item in self.partitions)

    def publication_payload(self) -> dict[str, JSONValue]:
        """Return writer-independent publication identity."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "broker_profile_id": self.broker_profile_id,
            "symbol_group_id": self.symbol_group_id,
            "symbols": list(self.symbols),
            "symbol_event_counts": dict(self.symbol_event_counts),
            "logical_content_sha256": self.replay.logical_content_sha256,
            "source_manifest_id": self.source.source_manifest_id,
            "constraint_manifest_id": self.constraints.constraint_manifest_id,
            "quality_manifest_id": self.quality.quality_manifest_id,
            "ensemble_manifest_id": self.ensemble.ensemble_manifest_id,
            "retention_plan_id": self.retention.plan_id,
        }

    def payload(self) -> dict[str, JSONValue]:
        """Return complete compact manifest evidence."""
        return {
            **self.publication_payload(),
            "publication_id": self.publication_id,
            "partitions": [item.to_dict() for item in self.partitions],
            "source": self.source.to_dict(),
            "constraints": self.constraints.to_dict(),
            "quality": self.quality.to_dict(),
            "replay": self.replay.to_dict(),
            "ensemble": self.ensemble.to_dict(),
            "retention": self.retention.to_dict(),
            "event_count": self.event_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "min_event_time_ns": self.min_event_time_ns,
            "max_event_time_ns": self.max_event_time_ns,
            "event_rows_inline": False,
            "analytical_frame_columns_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic final-product JSON."""
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionProductManifestV1:
        """Restore and reconcile a final-product manifest."""
        _require_schema(data, RECONSTRUCTION_PRODUCT_SCHEMA_VERSION)
        _require_derived(
            data, "event_schema_version", SYNTHETIC_EVENT_SCHEMA_VERSION
        )
        _require_derived(data, "event_rows_inline", False)
        _require_derived(data, "analytical_frame_columns_inline", False)
        manifest = cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            broker_profile_id=str(data.get("broker_profile_id", "")),
            symbol_group_id=str(data.get("symbol_group_id", "")),
            symbols=_string_tuple(data.get("symbols"), "symbols"),
            symbol_event_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("symbol_event_counts"), "symbol_event_counts"
                ).items()
            },
            partitions=tuple(
                ReconstructionProductPartitionV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("partitions"), "partitions"
                )
            ),
            source=ReconstructionSourceManifestV1.from_dict(
                _mapping(data.get("source"), "source")
            ),
            constraints=ReconstructionConstraintManifestV1.from_dict(
                _mapping(data.get("constraints"), "constraints")
            ),
            quality=ReconstructionQualityManifestV1.from_dict(
                _mapping(data.get("quality"), "quality")
            ),
            replay=ReconstructionReplayManifestV1.from_dict(
                _mapping(data.get("replay"), "replay")
            ),
            ensemble=ReconstructionEnsembleManifestV1.from_dict(
                _mapping(data.get("ensemble"), "ensemble")
            ),
            retention=ReconstructionRetentionPlanV1.from_dict(
                _mapping(data.get("retention"), "retention")
            ),
            publication_id=str(data.get("publication_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        for name in (
            "event_count",
            "observed_event_count",
            "synthetic_event_count",
            "min_event_time_ns",
            "max_event_time_ns",
        ):
            if data.get(name) != getattr(manifest, name):
                raise ValueError(f"derived manifest field {name} differs")
        return manifest

    @classmethod
    def from_json(cls, text: str) -> ReconstructionProductManifestV1:
        """Restore a final-product manifest from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionProductManifestV2:
    """Generic-delivery synchronized product without broker impersonation."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    delivery_profile_id: str
    symbol_group_id: str
    symbols: tuple[str, ...]
    symbol_event_counts: Mapping[str, int]
    partitions: tuple[ReconstructionProductPartitionV1, ...]
    source: ReconstructionSourceManifestV1
    constraints: ReconstructionConstraintManifestV1
    quality: ReconstructionDeliveryQualityManifestV1
    replay: ReconstructionReplayManifestV1
    ensemble: ReconstructionEnsembleManifestV1
    retention: ReconstructionRetentionPlanV1
    publication_id: str = ""
    manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION,
            "reconstruction product v2 manifest",
        )
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
            "delivery_profile_id",
            "symbol_group_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        symbols = tuple(
            sorted({_normalized_symbol(item) for item in self.symbols})
        )
        if not symbols:
            raise ValueError("product v2 manifest requires symbols")
        object.__setattr__(self, "symbols", symbols)
        counts = {
            _normalized_symbol(symbol): _nonnegative_int(
                count, f"symbol_event_counts.{symbol}"
            )
            for symbol, count in self.symbol_event_counts.items()
        }
        if set(counts) != set(symbols):
            raise ValueError("symbol counts do not cover product v2 group")
        object.__setattr__(
            self, "symbol_event_counts", dict(sorted(counts.items()))
        )
        partitions = tuple(
            sorted(
                self.partitions,
                key=lambda item: (
                    item.symbol,
                    item.event_date,
                    item.partition_id,
                ),
            )
        )
        if not partitions or len(partitions) > MAX_RECONSTRUCTION_PARTITIONS:
            raise ValueError("product v2 partition count is empty or unbounded")
        if len({item.relative_path for item in partitions}) != len(partitions):
            raise ValueError("product v2 has duplicate partition paths")
        actual_counts = dict.fromkeys(symbols, 0)
        for partition in partitions:
            if partition.symbol not in actual_counts:
                raise ValueError("partition symbol is outside product v2 group")
            actual_counts[partition.symbol] += partition.row_count
        if actual_counts != counts:
            raise ValueError("product v2 partition counts do not reconcile")
        object.__setattr__(self, "partitions", partitions)
        if not isinstance(self.source, ReconstructionSourceManifestV1):
            raise TypeError("product v2 requires source evidence")
        if not isinstance(self.constraints, ReconstructionConstraintManifestV1):
            raise TypeError("product v2 requires constraint evidence")
        if not isinstance(
            self.quality, ReconstructionDeliveryQualityManifestV1
        ):
            raise TypeError("product v2 requires delivery quality evidence")
        if not isinstance(self.replay, ReconstructionReplayManifestV1):
            raise TypeError("product v2 requires replay evidence")
        if not isinstance(self.ensemble, ReconstructionEnsembleManifestV1):
            raise TypeError("product v2 requires ensemble evidence")
        if not isinstance(self.retention, ReconstructionRetentionPlanV1):
            raise TypeError("product v2 requires retention evidence")
        if self.retention.run_id != self.run_id:
            raise ValueError("retention run differs from product v2 run")
        if self.ensemble_member_id not in self.retention.retained_member_ids:
            raise ValueError("product v2 member is not retained")
        if (
            self.ensemble.run_id != self.run_id
            or self.ensemble.materialized_member_id != self.ensemble_member_id
            or self.ensemble.primary_member_id
            != self.retention.primary_member_id
            or self.ensemble.retained_member_ids
            != self.retention.retained_member_ids
            or dict(self.ensemble.member_event_estimates)
            != dict(self.retention.member_event_counts)
            or self.ensemble.retention_plan_id != self.retention.plan_id
        ):
            raise ValueError("product v2 ensemble does not reconcile")
        actual_rows = sum(counts.values())
        if len(partitions) > self.retention.estimated_partition_count:
            raise ValueError("product v2 partitions exceed preflight")
        if (
            actual_rows
            > self.retention.member_event_counts[self.ensemble_member_id]
        ):
            raise ValueError("product v2 rows exceed preflight")
        if (
            self.source.observed_event_count
            + self.constraints.synthetic_event_count
            != actual_rows
        ):
            raise ValueError("product v2 source/constraint counts differ")
        if (
            self.quality.observed_event_count
            != self.source.observed_event_count
        ):
            raise ValueError("delivery/source observed counts differ")
        if (
            self.quality.synthetic_event_count
            != self.constraints.synthetic_event_count
        ):
            raise ValueError("delivery/constraint synthetic counts differ")
        if self.quality.delivery_profile_id != self.delivery_profile_id:
            raise ValueError("delivery profile differs from product v2 axis")
        expected_publication = _stable_id(
            "reconstruction-publication-v2", self.publication_payload()
        )
        supplied_publication = _optional_text(self.publication_id)
        if (
            supplied_publication is not None
            and supplied_publication != expected_publication
        ):
            raise ValueError("product v2 publication_id differs")
        object.__setattr__(self, "publication_id", expected_publication)
        expected_manifest = _stable_id(
            "reconstruction-manifest-v2", self.payload()
        )
        supplied_manifest = _optional_text(self.manifest_id)
        if (
            supplied_manifest is not None
            and supplied_manifest != expected_manifest
        ):
            raise ValueError("product v2 manifest_id differs")
        object.__setattr__(self, "manifest_id", expected_manifest)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_RECONSTRUCTION_MANIFEST_BYTES
        ):
            raise ValueError("reconstruction product v2 manifest exceeds limit")

    @property
    def event_count(self) -> int:
        """Return total durable events."""
        return sum(self.symbol_event_counts.values())

    @property
    def observed_event_count(self) -> int:
        """Return total immutable observed rows."""
        return self.source.observed_event_count

    @property
    def synthetic_event_count(self) -> int:
        """Return total accepted synthetic rows."""
        return self.constraints.synthetic_event_count

    @property
    def min_event_time_ns(self) -> int:
        """Return the first product event time."""
        return min(item.min_event_time_ns for item in self.partitions)

    @property
    def max_event_time_ns(self) -> int:
        """Return the last product event time."""
        return max(item.max_event_time_ns for item in self.partitions)

    def publication_payload(self) -> dict[str, JSONValue]:
        """Return writer-independent generic-delivery publication identity."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "delivery_profile_id": self.delivery_profile_id,
            "delivery_mode": self.quality.delivery_mode.value,
            "symbol_group_id": self.symbol_group_id,
            "symbols": list(self.symbols),
            "symbol_event_counts": dict(self.symbol_event_counts),
            "logical_content_sha256": self.replay.logical_content_sha256,
            "source_manifest_id": self.source.source_manifest_id,
            "constraint_manifest_id": self.constraints.constraint_manifest_id,
            "quality_manifest_id": self.quality.quality_manifest_id,
            "ensemble_manifest_id": self.ensemble.ensemble_manifest_id,
            "retention_plan_id": self.retention.plan_id,
        }

    def payload(self) -> dict[str, JSONValue]:
        """Return complete compact product v2 evidence."""
        return {
            **self.publication_payload(),
            "publication_id": self.publication_id,
            "partitions": [item.to_dict() for item in self.partitions],
            "source": self.source.to_dict(),
            "constraints": self.constraints.to_dict(),
            "quality": self.quality.to_dict(),
            "replay": self.replay.to_dict(),
            "ensemble": self.ensemble.to_dict(),
            "retention": self.retention.to_dict(),
            "event_count": self.event_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "min_event_time_ns": self.min_event_time_ns,
            "max_event_time_ns": self.max_event_time_ns,
            "event_rows_inline": False,
            "analytical_frame_columns_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic product v2 JSON."""
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        """Return deterministic compact product v2 JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionProductManifestV2:
        """Restore and reconcile a generic-delivery product manifest."""
        _require_schema(data, RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION)
        _require_derived(
            data, "event_schema_version", SYNTHETIC_EVENT_SCHEMA_VERSION
        )
        _require_derived(data, "event_rows_inline", False)
        _require_derived(data, "analytical_frame_columns_inline", False)
        manifest = cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            delivery_profile_id=str(data.get("delivery_profile_id", "")),
            symbol_group_id=str(data.get("symbol_group_id", "")),
            symbols=_string_tuple(data.get("symbols"), "symbols"),
            symbol_event_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("symbol_event_counts"), "symbol_event_counts"
                ).items()
            },
            partitions=tuple(
                ReconstructionProductPartitionV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("partitions"), "partitions"
                )
            ),
            source=ReconstructionSourceManifestV1.from_dict(
                _mapping(data.get("source"), "source")
            ),
            constraints=ReconstructionConstraintManifestV1.from_dict(
                _mapping(data.get("constraints"), "constraints")
            ),
            quality=ReconstructionDeliveryQualityManifestV1.from_dict(
                _mapping(data.get("quality"), "quality")
            ),
            replay=ReconstructionReplayManifestV1.from_dict(
                _mapping(data.get("replay"), "replay")
            ),
            ensemble=ReconstructionEnsembleManifestV1.from_dict(
                _mapping(data.get("ensemble"), "ensemble")
            ),
            retention=ReconstructionRetentionPlanV1.from_dict(
                _mapping(data.get("retention"), "retention")
            ),
            publication_id=str(data.get("publication_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(
            data, "delivery_mode", manifest.quality.delivery_mode.value
        )
        for name in (
            "event_count",
            "observed_event_count",
            "synthetic_event_count",
            "min_event_time_ns",
            "max_event_time_ns",
        ):
            if data.get(name) != getattr(manifest, name):
                raise ValueError(f"derived product v2 field {name} differs")
        return manifest

    @classmethod
    def from_json(cls, text: str) -> ReconstructionProductManifestV2:
        """Restore product v2 from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionObservedAnchorSegmentV1:
    """One contiguous immutable row range in a canonical source partition."""

    symbol: str
    source_version_id: str
    source_series_id: str
    source_period: str
    source_artifact: ArtifactRef
    source_row_start_id: int
    source_row_end_id: int
    row_count: int
    segment_id: str = ""
    schema_version: str = RECONSTRUCTION_OBSERVED_ANCHOR_SEGMENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_OBSERVED_ANCHOR_SEGMENT_SCHEMA_VERSION,
            "observed anchor segment",
        )
        object.__setattr__(self, "symbol", _normalized_symbol(self.symbol))
        for name in ("source_version_id", "source_series_id", "source_period"):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        artifact = self.source_artifact
        if not isinstance(artifact, ArtifactRef):
            raise TypeError("observed anchor segment requires an artifact ref")
        _required_text(artifact.kind)
        _required_text(artifact.path)
        _required_sha256(artifact.sha256, "source_artifact.sha256")
        if artifact.size_bytes is None:
            raise ValueError("observed anchor artifact requires a byte size")
        _positive_int(artifact.size_bytes, "source_artifact.size_bytes")
        start = _positive_int(self.source_row_start_id, "source_row_start_id")
        end = _positive_int(self.source_row_end_id, "source_row_end_id")
        if end < start:
            raise ValueError("observed anchor row range is reversed")
        count = _positive_int(self.row_count, "row_count")
        if count != end - start + 1:
            raise ValueError("observed anchor segment row count differs")
        object.__setattr__(self, "source_row_start_id", start)
        object.__setattr__(self, "source_row_end_id", end)
        object.__setattr__(self, "row_count", count)
        expected_symbol = artifact.metadata.get("symbol")
        if (
            expected_symbol is not None
            and str(expected_symbol).lower() != self.symbol
        ):
            raise ValueError("observed anchor artifact symbol differs")
        expected_period = artifact.metadata.get("period")
        if (
            expected_period is not None
            and str(expected_period) != self.source_period
        ):
            raise ValueError("observed anchor artifact period differs")
        expected = _stable_id(
            "reconstruction-observed-anchor-segment", self.payload()
        )
        supplied = _optional_text(self.segment_id)
        if supplied is not None and supplied != expected:
            raise ValueError("observed anchor segment_id differs")
        object.__setattr__(self, "segment_id", expected)

    def payload(self) -> dict[str, JSONValue]:
        """Return path-independent logical row-range identity."""
        return {
            "schema_version": self.schema_version,
            "symbol": self.symbol,
            "source_version_id": self.source_version_id,
            "source_series_id": self.source_series_id,
            "source_period": self.source_period,
            "source_artifact_kind": self.source_artifact.kind,
            "source_artifact_sha256": self.source_artifact.sha256,
            "source_artifact_size_bytes": self.source_artifact.size_bytes,
            "source_row_start_id": self.source_row_start_id,
            "source_row_end_id": self.source_row_end_id,
            "row_count": self.row_count,
            "row_identity_basis": "one-based-arrow-row-id-inclusive-v1",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return portable segment evidence plus its resolvable artifact ref."""
        return {
            **self.payload(),
            "source_artifact": self.source_artifact.to_dict(),
            "segment_id": self.segment_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionObservedAnchorSegmentV1:
        """Restore and verify one immutable source row range."""
        _require_schema(
            data, RECONSTRUCTION_OBSERVED_ANCHOR_SEGMENT_SCHEMA_VERSION
        )
        _require_derived(
            data, "row_identity_basis", "one-based-arrow-row-id-inclusive-v1"
        )
        artifact = ArtifactRef.from_dict(
            _mapping(data.get("source_artifact"), "source_artifact")
        )
        _require_derived(data, "source_artifact_kind", artifact.kind)
        _require_derived(data, "source_artifact_sha256", artifact.sha256)
        _require_derived(
            data, "source_artifact_size_bytes", artifact.size_bytes
        )
        return cls(
            symbol=str(data.get("symbol", "")),
            source_version_id=str(data.get("source_version_id", "")),
            source_series_id=str(data.get("source_series_id", "")),
            source_period=str(data.get("source_period", "")),
            source_artifact=artifact,
            source_row_start_id=_strict_int(
                data.get("source_row_start_id"), "source_row_start_id"
            ),
            source_row_end_id=_strict_int(
                data.get("source_row_end_id"), "source_row_end_id"
            ),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            segment_id=str(data.get("segment_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionProductManifestV3:
    """Logical observed-plus-synthetic product with synthetic-only Parquet."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    delivery_profile_id: str
    symbol_group_id: str
    symbols: tuple[str, ...]
    symbol_event_counts: Mapping[str, int]
    partitions: tuple[ReconstructionProductPartitionV1, ...]
    observed_anchor_segments: tuple[ReconstructionObservedAnchorSegmentV1, ...]
    logical_min_event_time_ns: int
    logical_max_event_time_ns: int
    source: ReconstructionSourceManifestV1
    constraints: ReconstructionConstraintManifestV1
    quality: ReconstructionDeliveryQualityManifestV1
    replay: ReconstructionReplayManifestV2
    ensemble: ReconstructionEnsembleManifestV1
    retention: ReconstructionRetentionPlanV1
    publication_id: str = ""
    manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_version(
            self.schema_version,
            RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION,
            "reconstruction product v3 manifest",
        )
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
            "delivery_profile_id",
            "symbol_group_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        symbols = tuple(
            sorted({_normalized_symbol(item) for item in self.symbols})
        )
        if not symbols:
            raise ValueError("product v3 manifest requires symbols")
        object.__setattr__(self, "symbols", symbols)
        counts = {
            _normalized_symbol(symbol): _nonnegative_int(
                count, f"symbol_event_counts.{symbol}"
            )
            for symbol, count in self.symbol_event_counts.items()
        }
        if set(counts) != set(symbols) or not sum(counts.values()):
            raise ValueError("product v3 logical symbol counts are incomplete")
        object.__setattr__(
            self, "symbol_event_counts", dict(sorted(counts.items()))
        )
        partitions = tuple(
            sorted(
                self.partitions,
                key=lambda item: (
                    item.symbol,
                    item.event_date,
                    item.partition_id,
                ),
            )
        )
        if len(partitions) > MAX_RECONSTRUCTION_PARTITIONS:
            raise ValueError("product v3 partition count is unbounded")
        if len({item.relative_path for item in partitions}) != len(partitions):
            raise ValueError("product v3 has duplicate partition paths")
        if any(item.observed_event_count for item in partitions):
            raise ValueError(
                "product v3 Parquet must contain synthetic rows only"
            )
        if any(item.symbol not in symbols for item in partitions):
            raise ValueError("product v3 partition symbol is outside the group")
        if sum(item.synthetic_event_count for item in partitions) != (
            self.constraints.synthetic_event_count
        ):
            raise ValueError("product v3 synthetic partition counts differ")
        object.__setattr__(self, "partitions", partitions)
        segments = tuple(
            sorted(
                self.observed_anchor_segments,
                key=lambda item: (
                    item.symbol,
                    item.source_period,
                    item.source_row_start_id,
                    item.segment_id,
                ),
            )
        )
        if not segments:
            raise ValueError("product v3 requires immutable observed anchors")
        if len({item.segment_id for item in segments}) != len(segments):
            raise ValueError(
                "product v3 has duplicate observed anchor segments"
            )
        if any(item.symbol not in symbols for item in segments):
            raise ValueError("product v3 anchor symbol is outside the group")
        if (
            sum(item.row_count for item in segments)
            != self.source.observed_event_count
        ):
            raise ValueError("product v3 observed segment counts differ")
        object.__setattr__(self, "observed_anchor_segments", segments)
        minimum = _strict_int(
            self.logical_min_event_time_ns, "logical_min_event_time_ns"
        )
        maximum = _strict_int(
            self.logical_max_event_time_ns, "logical_max_event_time_ns"
        )
        if maximum < minimum:
            raise ValueError("product v3 logical time bounds are reversed")
        object.__setattr__(self, "logical_min_event_time_ns", minimum)
        object.__setattr__(self, "logical_max_event_time_ns", maximum)
        if not isinstance(self.source, ReconstructionSourceManifestV1):
            raise TypeError("product v3 requires source evidence")
        if not isinstance(self.constraints, ReconstructionConstraintManifestV1):
            raise TypeError("product v3 requires constraint evidence")
        if not isinstance(
            self.quality, ReconstructionDeliveryQualityManifestV1
        ):
            raise TypeError("product v3 requires delivery quality evidence")
        if not isinstance(self.replay, ReconstructionReplayManifestV2):
            raise TypeError("product v3 requires replay evidence")
        if (
            self.replay.writer_id
            not in SUPPORTED_RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_IDS
        ):
            raise ValueError("product v3 requires the synthetic-delta writer")
        expected_exclusions = (
            RECONSTRUCTION_SYNTHETIC_DELTA_DERIVED_COLUMNS
            if self.replay.writer_id == RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_ID
            else ()
        )
        if self.replay.canonicalized_metadata_exclusions != expected_exclusions:
            raise ValueError(
                "product v3 derived-column exclusions differ from its writer"
            )
        if not isinstance(self.ensemble, ReconstructionEnsembleManifestV1):
            raise TypeError("product v3 requires ensemble evidence")
        if not isinstance(self.retention, ReconstructionRetentionPlanV1):
            raise TypeError("product v3 requires retention evidence")
        if self.retention.run_id != self.run_id:
            raise ValueError("retention run differs from product v3 run")
        if self.ensemble_member_id not in self.retention.retained_member_ids:
            raise ValueError("product v3 member is not retained")
        if (
            self.ensemble.run_id != self.run_id
            or self.ensemble.materialized_member_id != self.ensemble_member_id
            or self.ensemble.primary_member_id
            != self.retention.primary_member_id
            or self.ensemble.retained_member_ids
            != self.retention.retained_member_ids
            or dict(self.ensemble.member_event_estimates)
            != dict(self.retention.member_event_counts)
            or self.ensemble.retention_plan_id != self.retention.plan_id
        ):
            raise ValueError("product v3 ensemble does not reconcile")
        logical_rows = sum(counts.values())
        if (
            logical_rows
            > self.retention.member_event_counts[self.ensemble_member_id]
        ):
            raise ValueError("product v3 logical rows exceed preflight")
        if len(partitions) > self.retention.estimated_partition_count:
            raise ValueError("product v3 partitions exceed preflight")
        if (
            self.source.observed_event_count
            + self.constraints.synthetic_event_count
            != logical_rows
        ):
            raise ValueError("product v3 source/constraint counts differ")
        if (
            self.quality.observed_event_count
            != self.source.observed_event_count
        ):
            raise ValueError("product v3 delivery/source counts differ")
        if (
            self.quality.synthetic_event_count
            != self.constraints.synthetic_event_count
        ):
            raise ValueError("product v3 delivery/constraint counts differ")
        if self.quality.delivery_profile_id != self.delivery_profile_id:
            raise ValueError("delivery profile differs from product v3 axis")
        expected_publication = _stable_id(
            "reconstruction-publication-v3", self.publication_payload()
        )
        supplied_publication = _optional_text(self.publication_id)
        if (
            supplied_publication is not None
            and supplied_publication != expected_publication
        ):
            raise ValueError("product v3 publication_id differs")
        object.__setattr__(self, "publication_id", expected_publication)
        expected_manifest = _stable_id(
            "reconstruction-manifest-v3", self.payload()
        )
        supplied_manifest = _optional_text(self.manifest_id)
        if (
            supplied_manifest is not None
            and supplied_manifest != expected_manifest
        ):
            raise ValueError("product v3 manifest_id differs")
        object.__setattr__(self, "manifest_id", expected_manifest)
        if (
            len(self.to_json().encode("utf-8"))
            > MAX_RECONSTRUCTION_MANIFEST_BYTES
        ):
            raise ValueError("reconstruction product v3 manifest exceeds limit")

    @property
    def event_count(self) -> int:
        """Return logical observed-plus-synthetic event count."""
        return sum(self.symbol_event_counts.values())

    @property
    def observed_event_count(self) -> int:
        """Return immutable observed row count referenced by the product."""
        return self.source.observed_event_count

    @property
    def synthetic_event_count(self) -> int:
        """Return physically materialized synthetic row count."""
        return self.constraints.synthetic_event_count

    @property
    def physical_event_count(self) -> int:
        """Return rows stored in synthetic-delta Parquet partitions."""
        return sum(item.row_count for item in self.partitions)

    @property
    def min_event_time_ns(self) -> int:
        """Return the first logical event time."""
        return self.logical_min_event_time_ns

    @property
    def max_event_time_ns(self) -> int:
        """Return the last logical event time."""
        return self.logical_max_event_time_ns

    def publication_payload(self) -> dict[str, JSONValue]:
        """Return path-independent logical publication identity."""
        return {
            "schema_version": self.schema_version,
            "event_schema_version": SYNTHETIC_EVENT_SCHEMA_VERSION,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "delivery_profile_id": self.delivery_profile_id,
            "delivery_mode": self.quality.delivery_mode.value,
            "symbol_group_id": self.symbol_group_id,
            "symbols": list(self.symbols),
            "symbol_event_counts": dict(self.symbol_event_counts),
            "logical_content_sha256": self.replay.logical_content_sha256,
            "source_manifest_id": self.source.source_manifest_id,
            "observed_anchor_segment_ids": [
                item.segment_id for item in self.observed_anchor_segments
            ],
            "constraint_manifest_id": self.constraints.constraint_manifest_id,
            "quality_manifest_id": self.quality.quality_manifest_id,
            "ensemble_manifest_id": self.ensemble.ensemble_manifest_id,
            "retention_plan_id": self.retention.plan_id,
        }

    def payload(self) -> dict[str, JSONValue]:
        """Return complete synthetic-delta manifest evidence."""
        return {
            **self.publication_payload(),
            "publication_id": self.publication_id,
            "partitions": [item.to_dict() for item in self.partitions],
            "observed_anchor_segments": [
                item.to_dict() for item in self.observed_anchor_segments
            ],
            "source": self.source.to_dict(),
            "constraints": self.constraints.to_dict(),
            "quality": self.quality.to_dict(),
            "replay": self.replay.to_dict(),
            "ensemble": self.ensemble.to_dict(),
            "retention": self.retention.to_dict(),
            "event_count": self.event_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "min_event_time_ns": self.min_event_time_ns,
            "max_event_time_ns": self.max_event_time_ns,
            "physical_event_count": self.physical_event_count,
            "observed_anchor_storage": "immutable-arrow-row-range-reference-v1",
            "event_rows_inline": False,
            "analytical_frame_columns_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic product v3 JSON."""
        return {**self.payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        """Return deterministic compact product v3 JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionProductManifestV3:
        """Restore and reconcile a synthetic-delta product manifest."""
        _require_schema(data, RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION)
        _require_derived(
            data, "event_schema_version", SYNTHETIC_EVENT_SCHEMA_VERSION
        )
        _require_derived(
            data,
            "observed_anchor_storage",
            "immutable-arrow-row-range-reference-v1",
        )
        _require_derived(data, "event_rows_inline", False)
        _require_derived(data, "analytical_frame_columns_inline", False)
        manifest = cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            delivery_profile_id=str(data.get("delivery_profile_id", "")),
            symbol_group_id=str(data.get("symbol_group_id", "")),
            symbols=_string_tuple(data.get("symbols"), "symbols"),
            symbol_event_counts={
                str(key): _strict_int(value, str(key))
                for key, value in _mapping(
                    data.get("symbol_event_counts"), "symbol_event_counts"
                ).items()
            },
            partitions=tuple(
                ReconstructionProductPartitionV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("partitions"), "partitions"
                )
            ),
            observed_anchor_segments=tuple(
                ReconstructionObservedAnchorSegmentV1.from_dict(item)
                for item in _mapping_sequence(
                    data.get("observed_anchor_segments"),
                    "observed_anchor_segments",
                )
            ),
            logical_min_event_time_ns=_strict_int(
                data.get("min_event_time_ns"), "min_event_time_ns"
            ),
            logical_max_event_time_ns=_strict_int(
                data.get("max_event_time_ns"), "max_event_time_ns"
            ),
            source=ReconstructionSourceManifestV1.from_dict(
                _mapping(data.get("source"), "source")
            ),
            constraints=ReconstructionConstraintManifestV1.from_dict(
                _mapping(data.get("constraints"), "constraints")
            ),
            quality=ReconstructionDeliveryQualityManifestV1.from_dict(
                _mapping(data.get("quality"), "quality")
            ),
            replay=ReconstructionReplayManifestV2.from_dict(
                _mapping(data.get("replay"), "replay")
            ),
            ensemble=ReconstructionEnsembleManifestV1.from_dict(
                _mapping(data.get("ensemble"), "ensemble")
            ),
            retention=ReconstructionRetentionPlanV1.from_dict(
                _mapping(data.get("retention"), "retention")
            ),
            publication_id=str(data.get("publication_id", "")),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )
        _require_derived(
            data, "delivery_mode", manifest.quality.delivery_mode.value
        )
        _require_derived(
            data,
            "observed_anchor_segment_ids",
            [item.segment_id for item in manifest.observed_anchor_segments],
        )
        _require_derived(
            data,
            "physical_event_count",
            manifest.physical_event_count,
        )
        for name in (
            "event_count",
            "observed_event_count",
            "synthetic_event_count",
        ):
            _require_derived(data, name, getattr(manifest, name))
        return manifest

    @classmethod
    def from_json(cls, text: str) -> ReconstructionProductManifestV3:
        """Restore product v3 from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class StagedReconstructionPublicationV1:
    """Process-local reference to a validated, undiscoverable transaction."""

    root: Path
    staging_directory: Path
    committed_directory: Path
    manifest: ReconstructionProductManifestV1

    @property
    def manifest_path(self) -> Path:
        """Return the temporary manifest path."""
        return self.staging_directory / RECONSTRUCTION_MANIFEST_FILENAME

    @property
    def manifest_ref(self) -> ArtifactRef:
        """Return a strong temporary manifest reference for checkpoints."""
        return _artifact_ref_for_manifest(self.manifest_path, self.manifest)


@dataclass(frozen=True, slots=True)
class PublishedReconstructionV1:
    """One committed publication and whether it was an idempotent retry."""

    manifest: ReconstructionProductManifestV1
    manifest_path: Path
    manifest_ref: ArtifactRef
    idempotent_retry: bool


@dataclass(frozen=True, slots=True)
class StagedReconstructionPublicationV2:
    """Validated generic-delivery transaction awaiting atomic promotion."""

    root: Path
    staging_directory: Path
    committed_directory: Path
    manifest: ReconstructionProductManifestV2

    @property
    def manifest_path(self) -> Path:
        """Return the temporary manifest path."""
        return self.staging_directory / RECONSTRUCTION_MANIFEST_FILENAME


@dataclass(frozen=True, slots=True)
class PublishedReconstructionV2:
    """One committed generic-delivery publication and retry status."""

    manifest: ReconstructionProductManifestV2
    manifest_path: Path
    manifest_ref: ArtifactRef
    idempotent_retry: bool


@dataclass(frozen=True, slots=True)
class StagedReconstructionPublicationV3:
    """Validated synthetic-delta transaction awaiting atomic promotion."""

    root: Path
    staging_directory: Path
    committed_directory: Path
    manifest: ReconstructionProductManifestV3

    @property
    def manifest_path(self) -> Path:
        """Return the temporary manifest path."""
        return self.staging_directory / RECONSTRUCTION_MANIFEST_FILENAME


@dataclass(frozen=True, slots=True)
class PublishedReconstructionV3:
    """One committed synthetic-delta publication and retry status."""

    manifest: ReconstructionProductManifestV3
    manifest_path: Path
    manifest_ref: ArtifactRef
    idempotent_retry: bool


def reconstruction_logical_content_sha256(
    events: Iterable[SyntheticEventV1],
) -> str:
    """Hash exact event rows independently of partition placement."""
    ordered = sorted(events, key=_event_order_key)
    digest = hashlib.sha256(_LOGICAL_HASH_HEADER)
    for event in ordered:
        _update_event_digest(digest, event)
    return digest.hexdigest()


def stage_reconstruction_publication(
    root: str | Path,
    rendered_group: BrokerRenderedGroupV1,
    *,
    immutable_source_anchors: Iterable[SyntheticEventV1],
    symbol_group_id: str,
    retention_plan: ReconstructionRetentionPlanV1,
    storage_policy: ReconstructionStoragePolicyV1,
    row_group_size: int = DEFAULT_RECONSTRUCTION_ROW_GROUP_SIZE,
) -> StagedReconstructionPublicationV1:
    """Write and validate one synchronized group below hidden scratch."""
    _validate_publication_inputs(rendered_group, retention_plan, storage_policy)
    group_id = _required_text(symbol_group_id)
    row_group = _positive_int(row_group_size, "row_group_size")
    events = tuple(
        event
        for stream in sorted(
            rendered_group.streams, key=lambda item: item.symbol
        )
        for event in stream.events
    )
    if not events:
        raise ReconstructionPersistenceError(
            "final reconstruction group contains no events"
        )
    anchors = tuple(immutable_source_anchors)
    _validate_immutable_anchors(events, anchors)
    logical_hash = reconstruction_logical_content_sha256(events)
    root_path = Path(root).expanduser().resolve()
    axis_directory = _axis_directory(
        root_path,
        run_id=rendered_group.manifest.run_id,
        broker_profile_id=rendered_group.manifest.fingerprint_id,
        ensemble_member_id=rendered_group.manifest.ensemble_member_id,
        symbol_group_id=group_id,
    )
    scratch = axis_directory / ".scratch"
    scratch.mkdir(parents=True, exist_ok=True)
    staging_directory = Path(
        tempfile.mkdtemp(prefix="publication.tmp-", dir=scratch)
    )
    try:
        partitions = _write_product_partitions(
            staging_directory,
            rendered_group.streams,
            row_group_size=row_group,
        )
        source = _source_manifest(events, anchors)
        constraints = _constraint_manifest(events)
        quality = _quality_manifest(rendered_group)
        ensemble = ReconstructionEnsembleManifestV1(
            run_id=retention_plan.run_id,
            materialized_member_id=(rendered_group.manifest.ensemble_member_id),
            primary_member_id=retention_plan.primary_member_id,
            retained_member_ids=retention_plan.retained_member_ids,
            member_event_estimates=retention_plan.member_event_counts,
            retention_plan_id=retention_plan.plan_id,
        )
        pa, _ = _arrow_modules()
        replay = ReconstructionReplayManifestV1(
            logical_content_sha256=logical_hash,
            partition_byte_sha256=_partition_byte_digest(partitions),
            logical_hash_algorithm=RECONSTRUCTION_LOGICAL_HASH_ALGORITHM,
            byte_hash_algorithm=RECONSTRUCTION_BYTE_HASH_ALGORITHM,
            writer_id=RECONSTRUCTION_WRITER_ID,
            writer_library="pyarrow",
            writer_library_version=str(pa.__version__),
            python_runtime=(
                f"{sys.version_info.major}.{sys.version_info.minor}."
                f"{sys.version_info.micro}"
            ),
            compression=RECONSTRUCTION_COMPRESSION,
            row_group_size=row_group,
            canonicalized_metadata_exclusions=(),
        )
        counts = {
            stream.symbol: len(stream.events)
            for stream in rendered_group.streams
        }
        manifest = ReconstructionProductManifestV1(
            run_id=rendered_group.manifest.run_id,
            window_id=rendered_group.manifest.window_id,
            synchronization_unit_id=(
                rendered_group.manifest.synchronization_unit_id
            ),
            ensemble_member_id=(rendered_group.manifest.ensemble_member_id),
            broker_profile_id=rendered_group.manifest.fingerprint_id,
            symbol_group_id=group_id,
            symbols=tuple(counts),
            symbol_event_counts=counts,
            partitions=partitions,
            source=source,
            constraints=constraints,
            quality=quality,
            replay=replay,
            ensemble=ensemble,
            retention=retention_plan,
        )
        manifest_bytes = manifest.to_json().encode("utf-8")
        _validate_actual_storage(partitions, manifest_bytes, storage_policy)
        committed_directory = (
            axis_directory
            / "commits"
            / _path_component(manifest.publication_id)
        )
        _atomic_write_bytes(
            staging_directory / RECONSTRUCTION_MANIFEST_FILENAME,
            manifest_bytes,
        )
        staged = StagedReconstructionPublicationV1(
            root=root_path,
            staging_directory=staging_directory,
            committed_directory=committed_directory,
            manifest=manifest,
        )
        _verify_publication_directory(
            staging_directory,
            manifest,
            require_committed_layout=False,
        )
        return staged
    except Exception:
        _remove_scratch_entry(staging_directory, root_path)
        raise


def commit_reconstruction_publication(
    staged: StagedReconstructionPublicationV1,
) -> PublishedReconstructionV1:
    """Revalidate and atomically promote one staged publication."""
    if not isinstance(staged, StagedReconstructionPublicationV1):
        raise TypeError("commit requires a staged reconstruction publication")
    manifest = staged.manifest
    final_directory = staged.committed_directory
    if final_directory.exists():
        existing_path = final_directory / RECONSTRUCTION_MANIFEST_FILENAME
        existing = verify_reconstruction_publication(existing_path)
        if not isinstance(existing, ReconstructionProductManifestV1):
            raise ReconstructionPersistenceError(
                "legacy publication identity contains a delivery manifest"
            )
        if existing != manifest:
            raise ReconstructionPersistenceError(
                "publication identity already contains different evidence"
            )
        if staged.staging_directory.exists():
            _remove_scratch_entry(staged.staging_directory, staged.root)
        return PublishedReconstructionV1(
            manifest=existing,
            manifest_path=existing_path,
            manifest_ref=_artifact_ref_for_manifest(existing_path, existing),
            idempotent_retry=True,
        )
    _verify_publication_directory(
        staged.staging_directory,
        manifest,
        require_committed_layout=False,
    )
    final_directory.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(staged.staging_directory, final_directory)
    except OSError:
        if not final_directory.exists():
            raise
        existing_path = final_directory / RECONSTRUCTION_MANIFEST_FILENAME
        existing = verify_reconstruction_publication(existing_path)
        if not isinstance(existing, ReconstructionProductManifestV1):
            raise ReconstructionPersistenceError(
                "concurrent legacy commit produced a delivery manifest"
            )
        if existing != manifest:
            raise ReconstructionPersistenceError(
                "concurrent publication committed different evidence"
            )
        if staged.staging_directory.exists():
            _remove_scratch_entry(staged.staging_directory, staged.root)
        return PublishedReconstructionV1(
            manifest=existing,
            manifest_path=existing_path,
            manifest_ref=_artifact_ref_for_manifest(existing_path, existing),
            idempotent_retry=True,
        )
    _fsync_directory(final_directory.parent)
    manifest_path = final_directory / RECONSTRUCTION_MANIFEST_FILENAME
    committed = verify_reconstruction_publication(manifest_path)
    if not isinstance(committed, ReconstructionProductManifestV1):
        raise ReconstructionPersistenceError(
            "committed legacy publication restored a delivery manifest"
        )
    return PublishedReconstructionV1(
        manifest=committed,
        manifest_path=manifest_path,
        manifest_ref=_artifact_ref_for_manifest(manifest_path, committed),
        idempotent_retry=False,
    )


def publish_reconstruction_group(
    root: str | Path,
    rendered_group: BrokerRenderedGroupV1,
    *,
    immutable_source_anchors: Iterable[SyntheticEventV1],
    symbol_group_id: str,
    retention_plan: ReconstructionRetentionPlanV1,
    storage_policy: ReconstructionStoragePolicyV1,
    row_group_size: int = DEFAULT_RECONSTRUCTION_ROW_GROUP_SIZE,
) -> PublishedReconstructionV1:
    """Stage, validate, and atomically commit one final reconstruction group."""
    group_id = _required_text(symbol_group_id)
    row_group = _positive_int(row_group_size, "row_group_size")
    root_path = Path(root).expanduser().resolve()
    _validate_publication_inputs(rendered_group, retention_plan, storage_policy)
    output_events = tuple(
        event for stream in rendered_group.streams for event in stream.events
    )
    source_anchors = tuple(immutable_source_anchors)
    _validate_immutable_anchors(output_events, source_anchors)
    existing = _find_matching_publication(
        root_path,
        rendered_group,
        symbol_group_id=group_id,
        retention_plan=retention_plan,
        row_group_size=row_group,
    )
    if existing is not None:
        return existing
    staged = stage_reconstruction_publication(
        root_path,
        rendered_group,
        immutable_source_anchors=source_anchors,
        symbol_group_id=group_id,
        retention_plan=retention_plan,
        storage_policy=storage_policy,
        row_group_size=row_group,
    )
    try:
        return commit_reconstruction_publication(staged)
    except Exception:
        if staged.staging_directory.exists():
            _remove_scratch_entry(staged.staging_directory, root_path)
        raise


def stage_delivery_reconstruction_publication(
    root: str | Path,
    delivered_group: ReconstructionDeliveredGroupV1,
    *,
    final_validation: CrossCurrencyValidationReportV1,
    benchmark_artifact_ids: Sequence[str],
    benchmark_evidence: Mapping[str, JSONValue],
    point_in_time_evidence_projection_ids: Sequence[str] = (),
    point_in_time_evidence_decision_ids: Sequence[str] = (),
    cross_series_constraint_bundle_ids: Sequence[str] = (),
    cross_series_constraint_window_ids: Sequence[str] = (),
    cross_series_constraint_decision_ids: Sequence[str] = (),
    immutable_source_anchors: Iterable[SyntheticEventV1],
    immutable_source_artifacts: Mapping[str, ArtifactRef] | None = None,
    symbol_group_id: str,
    retention_plan: ReconstructionRetentionPlanV1,
    storage_policy: ReconstructionStoragePolicyV1,
    staging_root: str | Path,
    experiment_id: str | None = None,
    row_group_size: int = DEFAULT_RECONSTRUCTION_ROW_GROUP_SIZE,
) -> StagedReconstructionPublicationV2 | StagedReconstructionPublicationV3:
    """Stage one validated generic-delivery group in cancellable scratch."""
    _validate_delivery_publication_inputs(
        delivered_group,
        final_validation,
        retention_plan,
        storage_policy,
    )
    if immutable_source_artifacts is None and dict(
        retention_plan.member_physical_event_counts
    ) != dict(retention_plan.member_event_counts):
        raise ReconstructionPersistenceError(
            "product v2 cannot use a synthetic-delta retention estimate"
        )
    group_id = _required_text(symbol_group_id)
    row_group = _positive_int(row_group_size, "row_group_size")
    events = tuple(
        event
        for stream in sorted(
            delivered_group.streams, key=lambda item: item.symbol
        )
        for event in stream.events
    )
    if not events:
        raise ReconstructionPersistenceError(
            "final generic-delivery group contains no events"
        )
    anchors = tuple(immutable_source_anchors)
    _validate_immutable_anchors(events, anchors)
    root_path = Path(root).expanduser().resolve()
    portable_source_artifacts = (
        _materialize_portable_source_artifacts(
            root_path, immutable_source_artifacts
        )
        if immutable_source_artifacts is not None
        else None
    )
    anchor_segments = (
        _observed_anchor_segments(anchors, portable_source_artifacts)
        if portable_source_artifacts is not None
        else None
    )
    logical_hash = reconstruction_logical_content_sha256(events)
    manifest = delivered_group.manifest
    axis_directory = _delivery_axis_directory(
        root_path,
        run_id=manifest.run_id,
        delivery_profile_id=manifest.delivery_profile_id,
        ensemble_member_id=manifest.ensemble_member_id,
        symbol_group_id=group_id,
        schema_version=(
            RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION
            if anchor_segments is not None
            else RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION
        ),
    )
    axis_directory.mkdir(parents=True, exist_ok=True)
    scratch = Path(staging_root).expanduser().resolve()
    scratch.mkdir(parents=True, exist_ok=True)
    if scratch.stat().st_dev != axis_directory.stat().st_dev:
        raise ReconstructionPersistenceError(
            "window scratch and output root are on different filesystems"
        )
    staging_directory = Path(
        tempfile.mkdtemp(prefix="publication.tmp-", dir=scratch)
    )
    try:
        if anchor_segments is None:
            partitions = _write_product_partitions(
                staging_directory,
                delivered_group.streams,
                row_group_size=row_group,
            )
        else:
            partitions = _write_synthetic_product_partitions(
                staging_directory,
                delivered_group.streams,
                immutable_source_anchors=anchors,
                row_group_size=row_group,
            )
        source = _source_manifest(events, anchors, experiment_id=experiment_id)
        constraints = _constraint_manifest(events)
        quality = _delivery_quality_manifest(
            delivered_group,
            final_validation=final_validation,
            benchmark_artifact_ids=benchmark_artifact_ids,
            benchmark_evidence=benchmark_evidence,
            point_in_time_evidence_projection_ids=(
                point_in_time_evidence_projection_ids
            ),
            point_in_time_evidence_decision_ids=(
                point_in_time_evidence_decision_ids
            ),
            cross_series_constraint_bundle_ids=(
                cross_series_constraint_bundle_ids
            ),
            cross_series_constraint_window_ids=(
                cross_series_constraint_window_ids
            ),
            cross_series_constraint_decision_ids=(
                cross_series_constraint_decision_ids
            ),
        )
        ensemble = ReconstructionEnsembleManifestV1(
            run_id=retention_plan.run_id,
            materialized_member_id=manifest.ensemble_member_id,
            primary_member_id=retention_plan.primary_member_id,
            retained_member_ids=retention_plan.retained_member_ids,
            member_event_estimates=retention_plan.member_event_counts,
            retention_plan_id=retention_plan.plan_id,
        )
        pa, _ = _arrow_modules()
        counts = {
            stream.symbol: len(stream.events)
            for stream in delivered_group.streams
        }
        product: (
            ReconstructionProductManifestV2 | ReconstructionProductManifestV3
        )
        if anchor_segments is None:
            replay_v1 = ReconstructionReplayManifestV1(
                logical_content_sha256=logical_hash,
                partition_byte_sha256=_partition_byte_digest(partitions),
                logical_hash_algorithm=RECONSTRUCTION_LOGICAL_HASH_ALGORITHM,
                byte_hash_algorithm=RECONSTRUCTION_BYTE_HASH_ALGORITHM,
                writer_id=RECONSTRUCTION_WRITER_ID,
                writer_library="pyarrow",
                writer_library_version=str(pa.__version__),
                python_runtime=(
                    f"{sys.version_info.major}.{sys.version_info.minor}."
                    f"{sys.version_info.micro}"
                ),
                compression=RECONSTRUCTION_COMPRESSION,
                row_group_size=row_group,
                canonicalized_metadata_exclusions=(),
            )
            product = ReconstructionProductManifestV2(
                run_id=manifest.run_id,
                window_id=manifest.window_id,
                synchronization_unit_id=manifest.synchronization_unit_id,
                ensemble_member_id=manifest.ensemble_member_id,
                delivery_profile_id=manifest.delivery_profile_id,
                symbol_group_id=group_id,
                symbols=tuple(counts),
                symbol_event_counts=counts,
                partitions=partitions,
                source=source,
                constraints=constraints,
                quality=quality,
                replay=replay_v1,
                ensemble=ensemble,
                retention=retention_plan,
            )
        else:
            replay_v2 = ReconstructionReplayManifestV2(
                logical_content_sha256=logical_hash,
                partition_byte_sha256=_partition_byte_digest(partitions),
                logical_hash_algorithm=RECONSTRUCTION_LOGICAL_HASH_ALGORITHM,
                byte_hash_algorithm=RECONSTRUCTION_BYTE_HASH_ALGORITHM,
                writer_id=RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_ID,
                writer_library="pyarrow",
                writer_library_version=str(pa.__version__),
                python_runtime=(
                    f"{sys.version_info.major}.{sys.version_info.minor}."
                    f"{sys.version_info.micro}"
                ),
                compression=RECONSTRUCTION_COMPRESSION,
                row_group_size=row_group,
                canonicalized_metadata_exclusions=(
                    RECONSTRUCTION_SYNTHETIC_DELTA_DERIVED_COLUMNS
                ),
            )
            product = ReconstructionProductManifestV3(
                run_id=manifest.run_id,
                window_id=manifest.window_id,
                synchronization_unit_id=manifest.synchronization_unit_id,
                ensemble_member_id=manifest.ensemble_member_id,
                delivery_profile_id=manifest.delivery_profile_id,
                symbol_group_id=group_id,
                symbols=tuple(counts),
                symbol_event_counts=counts,
                partitions=partitions,
                observed_anchor_segments=anchor_segments,
                logical_min_event_time_ns=min(
                    event.event_time_ns for event in events
                ),
                logical_max_event_time_ns=max(
                    event.event_time_ns for event in events
                ),
                source=source,
                constraints=constraints,
                quality=quality,
                replay=replay_v2,
                ensemble=ensemble,
                retention=retention_plan,
            )
        manifest_bytes = product.to_json().encode("utf-8")
        _validate_actual_storage(partitions, manifest_bytes, storage_policy)
        committed_directory = (
            axis_directory / "commits" / _path_component(product.publication_id)
        )
        _atomic_write_bytes(
            staging_directory / RECONSTRUCTION_MANIFEST_FILENAME,
            manifest_bytes,
        )
        staged: (
            StagedReconstructionPublicationV2
            | StagedReconstructionPublicationV3
        )
        if isinstance(product, ReconstructionProductManifestV3):
            staged = StagedReconstructionPublicationV3(
                root=root_path,
                staging_directory=staging_directory,
                committed_directory=committed_directory,
                manifest=product,
            )
        else:
            staged = StagedReconstructionPublicationV2(
                root=root_path,
                staging_directory=staging_directory,
                committed_directory=committed_directory,
                manifest=product,
            )
        _verify_publication_directory(
            staging_directory,
            product,
            require_committed_layout=False,
            source_artifact_root=(
                root_path / RECONSTRUCTION_PRODUCT_DIRECTORY
                if isinstance(product, ReconstructionProductManifestV3)
                else None
            ),
        )
        return staged
    except Exception:
        if staging_directory.exists():
            shutil.rmtree(staging_directory)
        raise


def commit_delivery_reconstruction_publication(
    staged: (
        StagedReconstructionPublicationV2 | StagedReconstructionPublicationV3
    ),
) -> PublishedReconstructionV2 | PublishedReconstructionV3:
    """Atomically promote or recover one generic-delivery publication."""
    if not isinstance(
        staged,
        (StagedReconstructionPublicationV2, StagedReconstructionPublicationV3),
    ):
        raise TypeError(
            "delivery commit requires a staged delivery publication"
        )
    manifest = staged.manifest
    expected_type = type(manifest)
    final_directory = staged.committed_directory
    if final_directory.exists():
        existing_path = final_directory / RECONSTRUCTION_MANIFEST_FILENAME
        existing = verify_reconstruction_publication(existing_path)
        if not isinstance(existing, expected_type):
            raise ReconstructionPersistenceError(
                "delivery publication identity contains another schema"
            )
        if existing != manifest:
            raise ReconstructionPersistenceError(
                "delivery publication contains different evidence"
            )
        if staged.staging_directory.exists():
            shutil.rmtree(staged.staging_directory)
        return _published_delivery_reconstruction(
            existing,
            existing_path,
            idempotent_retry=True,
        )
    _verify_publication_directory(
        staged.staging_directory,
        manifest,
        require_committed_layout=False,
        source_artifact_root=(
            staged.root / RECONSTRUCTION_PRODUCT_DIRECTORY
            if isinstance(manifest, ReconstructionProductManifestV3)
            else None
        ),
    )
    final_directory.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.replace(staged.staging_directory, final_directory)
    except OSError:
        if not final_directory.exists():
            raise
        existing_path = final_directory / RECONSTRUCTION_MANIFEST_FILENAME
        existing = verify_reconstruction_publication(existing_path)
        if not isinstance(existing, expected_type):
            raise ReconstructionPersistenceError(
                "concurrent delivery commit produced another schema"
            )
        if existing != manifest:
            raise ReconstructionPersistenceError(
                "concurrent delivery commit produced different evidence"
            )
        if staged.staging_directory.exists():
            shutil.rmtree(staged.staging_directory)
        return _published_delivery_reconstruction(
            existing,
            existing_path,
            idempotent_retry=True,
        )
    _fsync_directory(final_directory.parent)
    manifest_path = final_directory / RECONSTRUCTION_MANIFEST_FILENAME
    committed = verify_reconstruction_publication(manifest_path)
    if not isinstance(committed, expected_type):
        raise ReconstructionPersistenceError(
            "committed delivery publication restored another schema"
        )
    return _published_delivery_reconstruction(
        committed,
        manifest_path,
        idempotent_retry=False,
    )


def _published_delivery_reconstruction(
    manifest: ReconstructionProductManifestV2 | ReconstructionProductManifestV3,
    manifest_path: Path,
    *,
    idempotent_retry: bool,
) -> PublishedReconstructionV2 | PublishedReconstructionV3:
    """Build a schema-narrow published-delivery result."""
    manifest_ref = _artifact_ref_for_manifest(manifest_path, manifest)
    if isinstance(manifest, ReconstructionProductManifestV3):
        return PublishedReconstructionV3(
            manifest=manifest,
            manifest_path=manifest_path,
            manifest_ref=manifest_ref,
            idempotent_retry=idempotent_retry,
        )
    return PublishedReconstructionV2(
        manifest=manifest,
        manifest_path=manifest_path,
        manifest_ref=manifest_ref,
        idempotent_retry=idempotent_retry,
    )


def load_reconstruction_manifest(
    path: str | Path,
) -> (
    ReconstructionProductManifestV1
    | ReconstructionProductManifestV2
    | ReconstructionProductManifestV3
):
    """Load and verify compact manifest identities without reading Parquet."""
    target = Path(path)
    payload = target.read_bytes()
    if len(payload) > MAX_RECONSTRUCTION_MANIFEST_BYTES:
        raise ReconstructionPersistenceError(
            "reconstruction manifest exceeds size limit"
        )
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as err:
        raise ReconstructionPersistenceError(
            "reconstruction manifest is not UTF-8"
        ) from err
    data = _json_mapping(text)
    version = str(data.get("schema_version", ""))
    if version == RECONSTRUCTION_PRODUCT_SCHEMA_VERSION:
        return ReconstructionProductManifestV1.from_dict(data)
    if version == RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION:
        return ReconstructionProductManifestV2.from_dict(data)
    if version == RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION:
        return ReconstructionProductManifestV3.from_dict(data)
    raise ReconstructionPersistenceError(
        "unsupported reconstruction product manifest version"
    )


def verify_reconstruction_publication(
    manifest_path: str | Path,
) -> (
    ReconstructionProductManifestV1
    | ReconstructionProductManifestV2
    | ReconstructionProductManifestV3
):
    """Fail closed unless every committed file and replay hash reconciles."""
    path = Path(manifest_path).expanduser().resolve()
    manifest = load_reconstruction_manifest(path)
    _validate_committed_manifest_location(path, manifest)
    _verify_publication_directory(
        path.parent, manifest, require_committed_layout=True
    )
    return manifest


def discover_reconstruction_manifests(
    root: str | Path,
    *,
    run_id: str | None = None,
    broker_profile_id: str | None = None,
    delivery_profile_id: str | None = None,
    ensemble_member_id: str | None = None,
    symbol_group_id: str | None = None,
) -> tuple[Path, ...]:
    """List only fully committed, verified product manifests."""
    product_root = (
        Path(root).expanduser().resolve() / RECONSTRUCTION_PRODUCT_DIRECTORY
    )
    if not product_root.exists():
        return ()
    matches: list[Path] = []
    for path in sorted(product_root.glob("**/commits/*/manifest.json")):
        manifest = load_reconstruction_manifest(path)
        _validate_committed_manifest_location(path.resolve(), manifest)
        if run_id is not None and manifest.run_id != run_id:
            continue
        if broker_profile_id is not None:
            if not isinstance(manifest, ReconstructionProductManifestV1):
                continue
            if manifest.broker_profile_id != broker_profile_id:
                continue
        if delivery_profile_id is not None:
            if not isinstance(
                manifest,
                (
                    ReconstructionProductManifestV2,
                    ReconstructionProductManifestV3,
                ),
            ):
                continue
            if manifest.delivery_profile_id != delivery_profile_id:
                continue
        if (
            ensemble_member_id is not None
            and manifest.ensemble_member_id != ensemble_member_id
        ):
            continue
        if (
            symbol_group_id is not None
            and manifest.symbol_group_id != symbol_group_id
        ):
            continue
        matches.append(path.resolve())
    return tuple(matches)


def reconstruction_parquet_paths(
    manifest_path: str | Path,
    *,
    symbols: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> tuple[Path, ...]:
    """Select only physical partitions overlapping symbol/time predicates."""
    path = Path(manifest_path).expanduser().resolve()
    manifest = load_reconstruction_manifest(path)
    _validate_committed_manifest_location(path, manifest)
    selected_symbols = {_normalized_symbol(symbol) for symbol in symbols}
    if selected_symbols and not selected_symbols.issubset(manifest.symbols):
        raise ValueError("query symbols are outside the product manifest")
    start = _optional_int(start_ns, "start_ns")
    end = _optional_int(end_ns, "end_ns")
    if start is not None and end is not None and end <= start:
        raise ValueError("query end_ns must be greater than start_ns")
    selected: list[Path] = []
    for partition in manifest.partitions:
        if selected_symbols and partition.symbol not in selected_symbols:
            continue
        if start is not None and partition.max_event_time_ns < start:
            continue
        if end is not None and partition.min_event_time_ns >= end:
            continue
        selected.append(path.parent / partition.relative_path)
    return tuple(selected)


def iter_reconstruction_event_batches(
    manifest_path: str | Path,
    *,
    columns: Iterable[str] = SYNTHETIC_EVENT_ARROW_COLUMNS,
    symbols: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
    batch_size: int = 65_536,
) -> Iterator[Any]:
    """Stream projected Arrow batches with file and row predicate pruning."""
    requested = tuple(columns)
    if not requested:
        raise ValueError("reconstruction scan requires at least one column")
    unknown = set(requested).difference(SYNTHETIC_EVENT_ARROW_COLUMNS)
    if unknown:
        raise ValueError(f"unknown reconstruction columns: {sorted(unknown)}")
    size = _positive_int(batch_size, "batch_size")
    lower = None if start_ns is None else _strict_int(start_ns, "start_ns")
    upper = None if end_ns is None else _strict_int(end_ns, "end_ns")
    if lower is not None and upper is not None and upper <= lower:
        raise ValueError("end_ns must be greater than start_ns")
    manifest = load_reconstruction_manifest(manifest_path)
    if isinstance(manifest, ReconstructionProductManifestV3):
        selected_symbols = {_normalized_symbol(symbol) for symbol in symbols}
        if selected_symbols and not selected_symbols.issubset(manifest.symbols):
            raise ValueError("query symbols are outside the product manifest")
        pa, _ = _arrow_modules()
        rows = [
            event.to_dict()
            for stream in read_reconstruction_streams(manifest_path)
            if not selected_symbols or stream.symbol in selected_symbols
            for event in stream.events
            if (lower is None or event.event_time_ns >= lower)
            and (upper is None or event.event_time_ns < upper)
        ]
        table = pa.Table.from_pylist(
            rows, schema=synthetic_event_arrow_schema()
        )
        yield from table.select(list(requested)).to_batches(max_chunksize=size)
        return
    paths = reconstruction_parquet_paths(
        manifest_path,
        symbols=symbols,
        start_ns=lower,
        end_ns=upper,
    )
    _, _, ds = _arrow_dataset_modules()
    expression = None
    if lower is not None:
        expression = ds.field("event_time_ns") >= lower
    if upper is not None:
        upper_expression = ds.field("event_time_ns") < upper
        expression = (
            upper_expression
            if expression is None
            else expression & upper_expression
        )
    for partition_path in paths:
        dataset = ds.dataset(partition_path, format="parquet")
        scanner = dataset.scanner(
            columns=list(requested),
            filter=expression,
            batch_size=size,
            use_threads=False,
        )
        yield from scanner.to_batches()


def scan_reconstruction_events_polars(
    manifest_path: str | Path,
    *,
    columns: Iterable[str] = SYNTHETIC_EVENT_ARROW_COLUMNS,
    symbols: Iterable[str] = (),
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> Any:
    """Return a lazy Polars scan with projection and predicates pushed down."""
    requested = tuple(columns)
    if not requested:
        raise ValueError("reconstruction scan requires at least one column")
    unknown = set(requested).difference(SYNTHETIC_EVENT_ARROW_COLUMNS)
    if unknown:
        raise ValueError(f"unknown reconstruction columns: {sorted(unknown)}")
    lower = None if start_ns is None else _strict_int(start_ns, "start_ns")
    upper = None if end_ns is None else _strict_int(end_ns, "end_ns")
    if lower is not None and upper is not None and upper <= lower:
        raise ValueError("end_ns must be greater than start_ns")
    manifest = load_reconstruction_manifest(manifest_path)
    if isinstance(manifest, ReconstructionProductManifestV3):
        selected_symbols = {_normalized_symbol(symbol) for symbol in symbols}
        if selected_symbols and not selected_symbols.issubset(manifest.symbols):
            raise ValueError("query symbols are outside the product manifest")
        pa, _ = _arrow_modules()
        rows = [
            event.to_dict()
            for stream in read_reconstruction_streams(manifest_path)
            if not selected_symbols or stream.symbol in selected_symbols
            for event in stream.events
            if (lower is None or event.event_time_ns >= lower)
            and (upper is None or event.event_time_ns < upper)
        ]
        table = pa.Table.from_pylist(
            rows, schema=synthetic_event_arrow_schema()
        )
        return _polars_module().from_arrow(table).lazy().select(list(requested))
    paths = reconstruction_parquet_paths(
        manifest_path,
        symbols=symbols,
        start_ns=lower,
        end_ns=upper,
    )
    pl = _polars_module()
    if not paths:
        schema = synthetic_event_arrow_schema()
        empty = pl.from_arrow(schema.empty_table()).lazy()
        return empty.select(list(requested))
    lazy = pl.scan_parquet(
        [str(path) for path in paths],
        hive_partitioning=False,
        use_statistics=True,
    )
    if lower is not None:
        lazy = lazy.filter(pl.col("event_time_ns") >= lower)
    if upper is not None:
        lazy = lazy.filter(pl.col("event_time_ns") < upper)
    return lazy.select(list(requested))


def read_reconstruction_streams(
    manifest_path: str | Path,
) -> tuple[SyntheticEventStreamV1, ...]:
    """Replay a committed product into exact per-symbol event streams."""
    path = Path(manifest_path).expanduser().resolve()
    manifest = verify_reconstruction_publication(path)
    by_symbol: dict[str, list[SyntheticEventV1]] = {
        symbol: [] for symbol in manifest.symbols
    }
    anchor_event_ids: tuple[str, ...] = ()
    if isinstance(manifest, ReconstructionProductManifestV3):
        observed_events = _read_observed_anchor_events(
            manifest, _reconstruction_product_root_from_path(path)
        )
        anchor_event_ids = tuple(event.event_id for event in observed_events)
        for event in observed_events:
            by_symbol[event.symbol].append(event)
    for partition in manifest.partitions:
        for event in _iter_partition_events(
            path.parent / partition.relative_path,
            anchor_event_ids=anchor_event_ids,
        ):
            by_symbol[event.symbol].append(event)
    streams = tuple(
        SyntheticEventStreamV1(
            run_id=manifest.run_id,
            ensemble_member_id=manifest.ensemble_member_id,
            symbol=symbol,
            events=tuple(sorted(by_symbol[symbol], key=_event_order_key)),
            source_version_ids=(
                manifest.source.source_version_ids
                if isinstance(manifest, ReconstructionProductManifestV3)
                else tuple(
                    source
                    for partition in manifest.partitions
                    if partition.symbol == symbol
                    for source in partition.source_version_ids
                )
            ),
        )
        for symbol in manifest.symbols
    )
    replay_hash = reconstruction_logical_content_sha256(
        event for stream in streams for event in stream.events
    )
    if replay_hash != manifest.replay.logical_content_sha256:
        raise ReconstructionPersistenceError(
            "replayed streams differ from committed logical hash"
        )
    return streams


def cleanup_reconstruction_scratch(root: str | Path) -> tuple[Path, ...]:
    """Remove only unpublished transaction directories below ``.scratch``."""
    root_path = Path(root).expanduser().resolve()
    product_root = root_path / RECONSTRUCTION_PRODUCT_DIRECTORY
    if not product_root.exists():
        return ()
    removed: list[Path] = []
    for scratch in product_root.glob("**/.scratch"):
        if not scratch.is_dir() or scratch.is_symlink():
            continue
        for child in tuple(scratch.iterdir()):
            if not child.name.startswith("publication.tmp-"):
                continue
            removed.append(child)
            _remove_scratch_entry(child, root_path)
        try:
            scratch.rmdir()
        except OSError:
            pass
    return tuple(removed)


def _validate_publication_inputs(
    rendered_group: BrokerRenderedGroupV1,
    retention: ReconstructionRetentionPlanV1,
    policy: ReconstructionStoragePolicyV1,
) -> None:
    if not isinstance(rendered_group, BrokerRenderedGroupV1):
        raise TypeError("publication requires a broker-rendered group")
    if rendered_group.status is not BrokerTransferStatus.APPLIED:
        raise ReconstructionPersistenceError(
            "refused broker-rendered groups cannot be published"
        )
    if not isinstance(retention, ReconstructionRetentionPlanV1):
        raise TypeError("publication requires retention preflight evidence")
    if not isinstance(policy, ReconstructionStoragePolicyV1):
        raise TypeError("publication requires a v1 storage policy")
    if retention.storage_policy_id != policy.policy_id:
        raise ReconstructionPersistenceError(
            "retention preflight uses a different storage policy"
        )
    if dict(retention.member_physical_event_counts) != dict(
        retention.member_event_counts
    ):
        raise ReconstructionPersistenceError(
            "legacy publication cannot use a synthetic-delta estimate"
        )
    manifest = rendered_group.manifest
    if retention.run_id != manifest.run_id:
        raise ReconstructionPersistenceError(
            "retention preflight uses a different reconstruction run"
        )
    if manifest.ensemble_member_id not in retention.retained_member_ids:
        raise ReconstructionPersistenceError(
            "rendered member is absent from retention preflight"
        )
    actual = sum(len(stream.events) for stream in rendered_group.streams)
    if actual > retention.member_event_counts[manifest.ensemble_member_id]:
        raise ReconstructionPersistenceError(
            "rendered rows exceed the pre-run member estimate"
        )
    actual_symbols = tuple(stream.symbol for stream in rendered_group.streams)
    if len(set(actual_symbols)) != len(actual_symbols):
        raise ReconstructionPersistenceError(
            "rendered group contains duplicate symbol streams"
        )
    if (
        not policy.atomic_promotion_required
        or not policy.advertise_only_committed
    ):
        raise ReconstructionPersistenceError(
            "storage policy does not require committed-only atomic publication"
        )


def _validate_delivery_publication_inputs(
    delivered_group: ReconstructionDeliveredGroupV1,
    final_validation: CrossCurrencyValidationReportV1,
    retention: ReconstructionRetentionPlanV1,
    policy: ReconstructionStoragePolicyV1,
) -> None:
    if not isinstance(delivered_group, ReconstructionDeliveredGroupV1):
        raise TypeError("delivery publication requires a delivered group")
    if delivered_group.status is not ReconstructionDeliveryStatus.APPLIED:
        raise ReconstructionPersistenceError(
            "refused delivery groups cannot be published"
        )
    if not isinstance(final_validation, CrossCurrencyValidationReportV1):
        raise TypeError("delivery publication requires final validation")
    if not final_validation.passed:
        raise ReconstructionPersistenceError(
            "failed final validation cannot be published"
        )
    if final_validation.stage is not CrossCurrencyValidationStage.POST_BROKER:
        raise ReconstructionPersistenceError(
            "delivery publication requires the final post-delivery validation seam"
        )
    manifest = delivered_group.manifest
    if (
        final_validation.run_id != manifest.run_id
        or final_validation.window_id != manifest.window_id
        or final_validation.synchronization_unit_id
        != manifest.synchronization_unit_id
        or final_validation.ensemble_member_id != manifest.ensemble_member_id
    ):
        raise ReconstructionPersistenceError(
            "final validation scope differs from delivery group"
        )
    if not isinstance(retention, ReconstructionRetentionPlanV1):
        raise TypeError("delivery publication requires retention evidence")
    if not isinstance(policy, ReconstructionStoragePolicyV1):
        raise TypeError("delivery publication requires storage policy")
    if retention.storage_policy_id != policy.policy_id:
        raise ReconstructionPersistenceError(
            "delivery retention uses a different storage policy"
        )
    if retention.run_id != manifest.run_id:
        raise ReconstructionPersistenceError(
            "delivery retention uses a different reconstruction run"
        )
    if manifest.ensemble_member_id not in retention.retained_member_ids:
        raise ReconstructionPersistenceError(
            "delivered member is absent from retention preflight"
        )
    actual = sum(len(stream.events) for stream in delivered_group.streams)
    if actual > retention.member_event_counts[manifest.ensemble_member_id]:
        raise ReconstructionPersistenceError(
            "delivered rows exceed the pre-run member estimate"
        )
    symbols = tuple(stream.symbol for stream in delivered_group.streams)
    if len(set(symbols)) != len(symbols):
        raise ReconstructionPersistenceError(
            "delivered group contains duplicate symbol streams"
        )
    if (
        not policy.atomic_promotion_required
        or not policy.advertise_only_committed
    ):
        raise ReconstructionPersistenceError(
            "storage policy does not require committed-only atomic publication"
        )


def _validate_immutable_anchors(
    output_events: Iterable[SyntheticEventV1],
    source_anchors: Iterable[SyntheticEventV1],
) -> None:
    anchors: dict[str, dict[str, JSONValue]] = {}
    for event in source_anchors:
        if not isinstance(event, SyntheticEventV1):
            raise TypeError("source anchors must be SyntheticEventV1 rows")
        if event.origin is not SyntheticEventOrigin.OBSERVED:
            raise ReconstructionPersistenceError(
                "source anchor input contains a synthetic row"
            )
        if event.event_id in anchors:
            raise ReconstructionPersistenceError(
                "source anchor input contains duplicate event IDs"
            )
        anchors[event.event_id] = event.to_dict()
    outputs = {
        event.event_id: event.to_dict()
        for event in output_events
        if event.origin is SyntheticEventOrigin.OBSERVED
    }
    if outputs != anchors:
        raise ReconstructionPersistenceError(
            "observed output values or IDs differ from immutable anchors"
        )


def _observed_anchor_segments(
    anchors: Iterable[SyntheticEventV1],
    artifacts_by_series_id: Mapping[str, ArtifactRef],
) -> tuple[ReconstructionObservedAnchorSegmentV1, ...]:
    """Compress exact observed lineage into contiguous source row ranges."""
    anchor_rows = tuple(anchors)
    grouped: dict[tuple[str, str, str, str], tuple[ArtifactRef, list[int]]] = {}
    for event in anchor_rows:
        if event.origin is not SyntheticEventOrigin.OBSERVED:
            raise ReconstructionPersistenceError(
                "observed anchor segmentation received a synthetic row"
            )
        if (
            event.source_series_id is None
            or event.source_period is None
            or event.source_row_id is None
        ):
            raise ReconstructionPersistenceError(
                "observed anchor lacks immutable source row lineage"
            )
        artifact = artifacts_by_series_id.get(event.source_series_id)
        if artifact is None:
            raise ReconstructionPersistenceError(
                "observed anchor source artifact is absent"
            )
        key = (
            event.symbol,
            event.source_version_id,
            event.source_series_id,
            event.source_period,
        )
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = (artifact, [event.source_row_id])
        else:
            if existing[0].sha256 != artifact.sha256:
                raise ReconstructionPersistenceError(
                    "one source series resolves to multiple artifacts"
                )
            existing[1].append(event.source_row_id)
    segments: list[ReconstructionObservedAnchorSegmentV1] = []
    for (symbol, version, series, period), (artifact, row_ids) in sorted(
        grouped.items()
    ):
        ordered = sorted(row_ids)
        if len(set(ordered)) != len(ordered):
            raise ReconstructionPersistenceError(
                "observed anchors duplicate a source row identity"
            )
        range_start = ordered[0]
        previous = ordered[0]
        for row_id in (*ordered[1:], None):
            if row_id is not None and row_id == previous + 1:
                previous = row_id
                continue
            segments.append(
                ReconstructionObservedAnchorSegmentV1(
                    symbol=symbol,
                    source_version_id=version,
                    source_series_id=series,
                    source_period=period,
                    source_artifact=artifact,
                    source_row_start_id=range_start,
                    source_row_end_id=previous,
                    row_count=previous - range_start + 1,
                )
            )
            if row_id is not None:
                range_start = row_id
                previous = row_id
    if sum(item.row_count for item in segments) != len(anchor_rows):
        raise ReconstructionPersistenceError(
            "observed anchor segmentation lost source rows"
        )
    return tuple(segments)


def _materialize_portable_source_artifacts(
    root: Path,
    artifacts_by_series_id: Mapping[str, ArtifactRef],
) -> dict[str, ArtifactRef]:
    """Deduplicate exact source Arrow files beneath the retained bundle root."""
    product_root = root / RECONSTRUCTION_PRODUCT_DIRECTORY
    source_root = product_root / "source-artifacts"
    source_root.mkdir(parents=True, exist_ok=True)
    portable: dict[str, ArtifactRef] = {}
    for series_id, artifact in sorted(artifacts_by_series_id.items()):
        source = Path(_required_text(artifact.path)).expanduser().resolve()
        if not source.is_file():
            raise ReconstructionPersistenceError(
                "immutable source artifact is missing"
            )
        if (
            artifact.size_bytes is None
            or source.stat().st_size != artifact.size_bytes
        ):
            raise ReconstructionPersistenceError(
                "immutable source artifact byte size differs"
            )
        _verify_cached_exact_file(
            source,
            expected_size=artifact.size_bytes,
            expected_sha256=artifact.sha256,
            label="immutable source artifact",
        )
        relative = Path("source-artifacts") / f"{artifact.sha256}.arrow"
        target = product_root / relative
        if not target.exists():
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{artifact.sha256}.", dir=source_root
            )
            os.close(descriptor)
            temporary = Path(temporary_name)
            temporary.unlink()
            try:
                try:
                    os.link(source, temporary)
                except OSError:
                    shutil.copyfile(source, temporary)
                _fsync_file(temporary)
                os.replace(temporary, target)
                _fsync_directory(source_root)
            finally:
                temporary.unlink(missing_ok=True)
        if not os.path.samefile(source, target):
            _verify_cached_exact_file(
                target,
                expected_size=artifact.size_bytes,
                expected_sha256=artifact.sha256,
                label="portable source artifact",
            )
        portable[_required_text(series_id)] = ArtifactRef(
            kind=artifact.kind,
            path=relative.as_posix(),
            size_bytes=artifact.size_bytes,
            sha256=artifact.sha256,
            metadata=dict(artifact.metadata),
        )
    return portable


def _write_synthetic_product_partitions(
    staging_directory: Path,
    streams: Iterable[SyntheticEventStreamV1],
    *,
    immutable_source_anchors: Iterable[SyntheticEventV1],
    row_group_size: int,
) -> tuple[ReconstructionProductPartitionV1, ...]:
    """Persist only generated deltas; immutable anchors remain referenced."""
    synthetic_streams: list[SyntheticEventStreamV1] = []
    for stream in streams:
        events = tuple(
            event
            for event in stream.events
            if event.origin is SyntheticEventOrigin.SYNTHETIC
        )
        if not events:
            continue
        synthetic_streams.append(
            SyntheticEventStreamV1(
                run_id=stream.run_id,
                ensemble_member_id=stream.ensemble_member_id,
                symbol=stream.symbol,
                events=events,
                source_version_ids=stream.source_version_ids,
            )
        )
    if not synthetic_streams:
        return ()
    anchor_event_ids = tuple(
        event.event_id
        for event in sorted(immutable_source_anchors, key=_event_order_key)
    )
    if len(anchor_event_ids) != len(set(anchor_event_ids)):
        raise ReconstructionPersistenceError(
            "synthetic-delta anchors contain duplicate event IDs"
        )
    return _write_product_partitions(
        staging_directory,
        synthetic_streams,
        row_group_size=row_group_size,
        synthetic_delta=True,
        anchor_event_ids=anchor_event_ids,
    )


def _write_product_partitions(
    staging_directory: Path,
    streams: Iterable[SyntheticEventStreamV1],
    *,
    row_group_size: int,
    synthetic_delta: bool = False,
    anchor_event_ids: Sequence[str] = (),
) -> tuple[ReconstructionProductPartitionV1, ...]:
    partitions: list[ReconstructionProductPartitionV1] = []
    for stream in sorted(streams, key=lambda item: item.symbol):
        by_date: dict[str, list[SyntheticEventV1]] = {}
        for event in stream.events:
            by_date.setdefault(_event_date(event.event_time_ns), []).append(
                event
            )
        for event_date, events in sorted(by_date.items()):
            partition_stream = SyntheticEventStreamV1(
                run_id=stream.run_id,
                ensemble_member_id=stream.ensemble_member_id,
                symbol=stream.symbol,
                events=tuple(events),
                source_version_ids=stream.source_version_ids,
            )
            relative = _partition_relative_path(stream.symbol, event_date)
            target = staging_directory / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            _write_parquet_partition(
                partition_stream,
                target,
                row_group_size=row_group_size,
                synthetic_delta=synthetic_delta,
                anchor_event_ids=anchor_event_ids,
            )
            _, pq = _arrow_modules()
            parquet = pq.ParquetFile(target)
            partition = ReconstructionProductPartitionV1(
                relative_path=relative,
                symbol=stream.symbol,
                event_date=event_date,
                stream_id=partition_stream.stream_id,
                source_version_ids=partition_stream.source_version_ids,
                row_count=len(events),
                observed_event_count=partition_stream.observed_event_count,
                synthetic_event_count=partition_stream.synthetic_event_count,
                min_event_time_ns=events[0].event_time_ns,
                max_event_time_ns=events[-1].event_time_ns,
                logical_content_sha256=reconstruction_logical_content_sha256(
                    events
                ),
                byte_sha256=_file_sha256(target),
                size_bytes=target.stat().st_size,
                row_group_count=parquet.num_row_groups,
            )
            _validate_partition_file(
                target,
                partition,
                row_group_size,
                expected_writer_id=(
                    RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_ID
                    if synthetic_delta
                    else RECONSTRUCTION_WRITER_ID
                ),
                anchor_event_ids=anchor_event_ids,
            )
            partitions.append(partition)
    if not partitions:
        raise ReconstructionPersistenceError(
            "reconstruction publication produced no partitions"
        )
    return tuple(partitions)


def _validate_actual_storage(
    partitions: Iterable[ReconstructionProductPartitionV1],
    manifest_bytes: bytes,
    policy: ReconstructionStoragePolicyV1,
) -> None:
    partition_bytes = sum(item.size_bytes for item in partitions)
    output_bytes = partition_bytes + len(manifest_bytes)
    violations: list[str] = []
    if output_bytes > policy.max_output_bytes:
        violations.append(
            f"actual output {output_bytes} exceeds {policy.max_output_bytes}"
        )
    if output_bytes > policy.max_scratch_bytes:
        violations.append(
            f"staged output {output_bytes} exceeds {policy.max_scratch_bytes}"
        )
    if violations:
        raise ReconstructionPersistenceError(
            "reconstruction persistence limits failed: " + "; ".join(violations)
        )


def _synthetic_delta_arrow_schema() -> Any:
    """Return the physical v3 delta schema with only derived IDs omitted."""
    pa, _ = _arrow_modules()
    logical = synthetic_event_arrow_schema()
    omitted = set(RECONSTRUCTION_SYNTHETIC_DELTA_DERIVED_COLUMNS)
    metadata = dict(logical.metadata or {})
    metadata[_SYNTHETIC_DELTA_STORAGE_METADATA_KEY] = (
        _SYNTHETIC_DELTA_STORAGE_SCHEMA_VERSION.encode("ascii")
    )
    fields = []
    for arrow_field in logical:
        if arrow_field.name == "left_anchor_event_id":
            fields.append(
                pa.field("left_anchor_ordinal", pa.int32(), nullable=False)
            )
        elif arrow_field.name == "right_anchor_event_id":
            fields.append(
                pa.field("right_anchor_ordinal", pa.int32(), nullable=False)
            )
        elif arrow_field.name not in omitted:
            fields.append(arrow_field)
    return pa.schema(fields, metadata=metadata)


def _synthetic_delta_stream_to_arrow(
    stream: SyntheticEventStreamV1,
    anchor_event_ids: Sequence[str],
) -> Any:
    """Serialize logical events without duplicating deterministic row IDs."""
    pa, _ = _arrow_modules()
    schema = _synthetic_delta_arrow_schema()
    omitted = set(RECONSTRUCTION_SYNTHETIC_DELTA_DERIVED_COLUMNS)
    anchor_ordinals = {
        _required_text(event_id): ordinal
        for ordinal, event_id in enumerate(anchor_event_ids)
    }
    if len(anchor_ordinals) != len(anchor_event_ids):
        raise ReconstructionPersistenceError(
            "synthetic-delta anchor dictionary contains duplicates"
        )
    rows = []
    for event in stream.events:
        left = cast(str, event.left_anchor_event_id)
        right = cast(str, event.right_anchor_event_id)
        if left not in anchor_ordinals or right not in anchor_ordinals:
            raise ReconstructionPersistenceError(
                "synthetic-delta event references an unretained anchor"
            )
        rows.append(
            {
                key: value
                for key, value in event.to_dict().items()
                if key not in omitted
            }
            | {
                "left_anchor_ordinal": anchor_ordinals[left],
                "right_anchor_ordinal": anchor_ordinals[right],
            }
        )
    return pa.Table.from_pylist(rows, schema=schema)


def _write_parquet_partition(
    stream: SyntheticEventStreamV1,
    target: Path,
    *,
    row_group_size: int,
    synthetic_delta: bool = False,
    anchor_event_ids: Sequence[str] = (),
) -> None:
    _, pq = _arrow_modules()
    partial = target.with_name(target.name + ".partial")
    try:
        pq.write_table(
            (
                _synthetic_delta_stream_to_arrow(stream, anchor_event_ids)
                if synthetic_delta
                else synthetic_event_stream_to_arrow(stream)
            ),
            partial,
            compression=RECONSTRUCTION_COMPRESSION,
            use_dictionary=False,
            write_statistics=True,
            version="2.6",
            data_page_version="2.0",
            row_group_size=row_group_size,
            write_page_checksum=True,
        )
        _fsync_file(partial)
        os.replace(partial, target)
        _fsync_directory(target.parent)
    finally:
        if partial.exists():
            partial.unlink()


def _validate_partition_file(
    path: Path,
    expected: ReconstructionProductPartitionV1,
    row_group_size: int,
    *,
    expected_writer_id: str = RECONSTRUCTION_WRITER_ID,
    anchor_event_ids: Sequence[str] = (),
) -> None:
    if not path.is_file() or path.is_symlink():
        raise ReconstructionPersistenceError("partition is missing or unsafe")
    if path.stat().st_size != expected.size_bytes:
        raise ReconstructionPersistenceError("partition byte size differs")
    if _file_sha256(path) != expected.byte_sha256:
        raise ReconstructionPersistenceError("partition byte hash differs")
    _, pq = _arrow_modules()
    try:
        parquet = pq.ParquetFile(path)
    except Exception as err:
        raise ReconstructionPersistenceError(
            "partition footer is unreadable"
        ) from err
    actual_schema = parquet.schema_arrow.remove_metadata()
    required_schema = (
        _synthetic_delta_arrow_schema().remove_metadata()
        if expected_writer_id == RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_ID
        else synthetic_event_arrow_schema().remove_metadata()
    )
    if not actual_schema.equals(required_schema):
        raise ReconstructionPersistenceError(
            "partition does not contain the exact final event schema"
        )
    if parquet.num_row_groups != expected.row_group_count:
        raise ReconstructionPersistenceError(
            "partition row-group count differs"
        )
    for ordinal in range(parquet.num_row_groups):
        if parquet.metadata.row_group(ordinal).num_rows > row_group_size:
            raise ReconstructionPersistenceError(
                "partition row group exceeds the configured bound"
            )
    events = tuple(
        _iter_partition_events(path, anchor_event_ids=anchor_event_ids)
    )
    if len(events) != expected.row_count:
        raise ReconstructionPersistenceError("partition row count differs")
    if any(event.symbol != expected.symbol for event in events):
        raise ReconstructionPersistenceError("partition symbol differs")
    if any(
        _event_date(event.event_time_ns) != expected.event_date
        for event in events
    ):
        raise ReconstructionPersistenceError("partition event date differs")
    if tuple(sorted(events, key=_event_order_key)) != events:
        raise ReconstructionPersistenceError("partition event order differs")
    observed = sum(
        event.origin is SyntheticEventOrigin.OBSERVED for event in events
    )
    if observed != expected.observed_event_count:
        raise ReconstructionPersistenceError("partition origin counts differ")
    if events[0].event_time_ns != expected.min_event_time_ns:
        raise ReconstructionPersistenceError("partition minimum time differs")
    if events[-1].event_time_ns != expected.max_event_time_ns:
        raise ReconstructionPersistenceError("partition maximum time differs")
    if reconstruction_logical_content_sha256(events) != (
        expected.logical_content_sha256
    ):
        raise ReconstructionPersistenceError("partition logical hash differs")


def _iter_partition_events(
    path: Path,
    *,
    anchor_event_ids: Sequence[str] = (),
) -> Iterator[SyntheticEventV1]:
    _, pq = _arrow_modules()
    try:
        parquet = pq.ParquetFile(path)
        actual_schema = parquet.schema_arrow.remove_metadata()
        logical_schema = synthetic_event_arrow_schema().remove_metadata()
        compact_schema = _synthetic_delta_arrow_schema().remove_metadata()
        if not actual_schema.equals(
            logical_schema
        ) and not actual_schema.equals(compact_schema):
            raise ReconstructionPersistenceError(
                "partition does not contain a supported event schema"
            )
        compact = actual_schema.equals(compact_schema)
        anchors = tuple(anchor_event_ids)
        if compact and not anchors:
            raise ReconstructionPersistenceError(
                "synthetic-delta partition requires its anchor dictionary"
            )
        for batch in parquet.iter_batches(batch_size=65_536, use_threads=False):
            for row in batch.to_pylist():
                if compact:
                    left_ordinal = _strict_int(
                        row.pop("left_anchor_ordinal", None),
                        "left_anchor_ordinal",
                    )
                    right_ordinal = _strict_int(
                        row.pop("right_anchor_ordinal", None),
                        "right_anchor_ordinal",
                    )
                    if not (
                        0 <= left_ordinal < len(anchors)
                        and 0 <= right_ordinal < len(anchors)
                    ):
                        raise ReconstructionPersistenceError(
                            "synthetic-delta anchor ordinal is out of range"
                        )
                    left = anchors[left_ordinal]
                    right = anchors[right_ordinal]
                    row["left_anchor_event_id"] = left
                    row["right_anchor_event_id"] = right
                    row["anchor_interval_id"] = derive_anchor_interval_id(
                        left, right
                    )
                    row["event_id"] = ""
                yield SyntheticEventV1.from_dict(row)
    except ReconstructionPersistenceError:
        raise
    except Exception as err:
        raise ReconstructionPersistenceError(
            f"cannot read reconstruction partition {path.name}"
        ) from err


def _source_manifest(
    events: Iterable[SyntheticEventV1],
    anchors: Iterable[SyntheticEventV1],
    *,
    experiment_id: str | None = None,
) -> ReconstructionSourceManifestV1:
    rows = tuple(events)
    observed = tuple(sorted(anchors, key=_event_order_key))
    return ReconstructionSourceManifestV1(
        source_version_ids=tuple(event.source_version_id for event in rows),
        source_series_ids=tuple(
            event.source_series_id
            for event in observed
            if event.source_series_id is not None
        ),
        source_periods=tuple(
            event.source_period
            for event in observed
            if event.source_period is not None
        ),
        observed_event_count=len(observed),
        observed_content_sha256=_observed_content_sha256(observed),
        observed_event_ids_sha256=_text_sequence_sha256(
            event.event_id for event in observed
        ),
        experiment_id=experiment_id,
    )


def _constraint_manifest(
    events: Iterable[SyntheticEventV1],
) -> ReconstructionConstraintManifestV1:
    synthetic = tuple(
        event
        for event in events
        if event.origin is SyntheticEventOrigin.SYNTHETIC
    )
    return ReconstructionConstraintManifestV1(
        synthetic_event_count=len(synthetic),
        constraint_set_ids=tuple(
            event.constraint_set_id
            for event in synthetic
            if event.constraint_set_id is not None
        ),
        generator_ids=tuple(
            event.generator_id
            for event in synthetic
            if event.generator_id is not None
        ),
        generator_versions=tuple(
            event.generator_version
            for event in synthetic
            if event.generator_version is not None
        ),
        generator_config_ids=tuple(
            event.generator_config_id
            for event in synthetic
            if event.generator_config_id is not None
        ),
        feed_epoch_ids=tuple(
            event.feed_epoch_id
            for event in synthetic
            if event.feed_epoch_id is not None
        ),
        reference_assignment_count=sum(
            event.reference_id is not None for event in synthetic
        ),
        reference_assignments_sha256=_lineage_assignments_sha256(
            synthetic, "reference_id"
        ),
        motif_assignment_count=sum(
            event.motif_id is not None for event in synthetic
        ),
        motif_assignments_sha256=_lineage_assignments_sha256(
            synthetic, "motif_id"
        ),
    )


def _quality_manifest(
    group: BrokerRenderedGroupV1,
) -> ReconstructionQualityManifestV1:
    manifest = group.manifest
    output_hash = _broker_streams_content_sha256(group.streams)
    if manifest.output_content_sha256 != output_hash:
        raise ReconstructionPersistenceError(
            "broker transfer output hash differs before persistence"
        )
    if group.cross_instrument_quality_payload is None:
        raise ReconstructionPersistenceError(
            "final group lacks quality payload"
        )
    quality_hash = _content_sha256(group.cross_instrument_quality_payload)
    if manifest.cross_instrument_quality_sha256 != quality_hash:
        raise ReconstructionPersistenceError(
            "cross-instrument quality hash differs before persistence"
        )
    return ReconstructionQualityManifestV1(
        broker_transfer_manifest_id=manifest.manifest_id,
        broker_fingerprint_id=manifest.fingerprint_id,
        transfer_output_content_sha256=output_hash,
        post_broker_validation_id=cast(str, manifest.post_broker_validation_id),
        post_broker_validation_status=cast(
            str, manifest.post_broker_validation_status
        ),
        cross_instrument_quality_status=cast(
            str, manifest.cross_instrument_quality_status
        ),
        cross_instrument_quality_sha256=quality_hash,
        broker_observed_event_count=manifest.observed_event_count,
        broker_synthetic_event_count=manifest.synthetic_event_count,
        broker_lineage_count=manifest.lineage_count,
        broker_lineage_content_sha256=cast(
            str, manifest.lineage_content_sha256
        ),
        broker_action_counts=manifest.action_counts,
        benchmark_comparison_ids=manifest.benchmark_comparison_ids,
    )


def _delivery_quality_manifest(
    group: ReconstructionDeliveredGroupV1,
    *,
    final_validation: CrossCurrencyValidationReportV1,
    benchmark_artifact_ids: Sequence[str],
    benchmark_evidence: Mapping[str, JSONValue],
    point_in_time_evidence_projection_ids: Sequence[str],
    point_in_time_evidence_decision_ids: Sequence[str],
    cross_series_constraint_bundle_ids: Sequence[str],
    cross_series_constraint_window_ids: Sequence[str],
    cross_series_constraint_decision_ids: Sequence[str],
) -> ReconstructionDeliveryQualityManifestV1:
    manifest = group.manifest
    output_hash = reconstruction_streams_content_sha256(group.streams)
    if manifest.output_content_sha256 != output_hash:
        raise ReconstructionPersistenceError(
            "delivery output hash differs before persistence"
        )
    evidence: dict[str, JSONValue] = {
        "final_validation": final_validation.to_dict(),
        "benchmark_artifact_ids": list(benchmark_artifact_ids),
        "benchmark_evidence": dict(benchmark_evidence),
        "point_in_time_evidence_projection_ids": list(
            point_in_time_evidence_projection_ids
        ),
        "point_in_time_evidence_decision_ids": list(
            point_in_time_evidence_decision_ids
        ),
        "cross_series_constraint_bundle_ids": list(
            cross_series_constraint_bundle_ids
        ),
        "cross_series_constraint_window_ids": list(
            cross_series_constraint_window_ids
        ),
        "cross_series_constraint_decision_ids": list(
            cross_series_constraint_decision_ids
        ),
    }
    quality_hash = _content_sha256(evidence)
    identity_hash = cast(str, manifest.identity_lineage_sha256)
    return ReconstructionDeliveryQualityManifestV1(
        delivery_manifest_id=manifest.manifest_id,
        delivery_profile_id=manifest.delivery_profile_id,
        delivery_mode=manifest.delivery_mode,
        delivery_output_content_sha256=output_hash,
        final_validation_id=final_validation.validation_id,
        final_validation_status=final_validation.status.value,
        cross_instrument_quality_status=(
            "passed" if final_validation.passed else "failed"
        ),
        cross_instrument_quality_sha256=quality_hash,
        observed_event_count=manifest.observed_event_count,
        synthetic_event_count=manifest.synthetic_event_count,
        identity_event_count=manifest.identity_event_count,
        identity_lineage_sha256=identity_hash,
        delivery_action_counts=(
            {"identity": manifest.identity_event_count}
            if manifest.identity_event_count
            else {}
        ),
        benchmark_artifact_ids=tuple(benchmark_artifact_ids),
        benchmark_evidence=benchmark_evidence,
        point_in_time_evidence_projection_ids=tuple(
            point_in_time_evidence_projection_ids
        ),
        point_in_time_evidence_decision_ids=tuple(
            point_in_time_evidence_decision_ids
        ),
        cross_series_constraint_bundle_ids=tuple(
            cross_series_constraint_bundle_ids
        ),
        cross_series_constraint_window_ids=tuple(
            cross_series_constraint_window_ids
        ),
        cross_series_constraint_decision_ids=tuple(
            cross_series_constraint_decision_ids
        ),
    )


def _read_observed_anchor_events(
    manifest: ReconstructionProductManifestV3,
    source_artifact_root: Path,
) -> tuple[SyntheticEventV1, ...]:
    """Rehydrate immutable anchors from exact Arrow row-range references."""
    try:
        import pyarrow as pa
        from pyarrow import ipc
    except ImportError as err:  # pragma: no cover - package dependency
        raise RuntimeError("synthetic-delta replay requires pyarrow") from err
    by_artifact: dict[
        tuple[str, str], list[ReconstructionObservedAnchorSegmentV1]
    ] = {}
    for segment in manifest.observed_anchor_segments:
        key = (segment.source_artifact.path, segment.source_artifact.sha256)
        by_artifact.setdefault(key, []).append(segment)
    raw: dict[str, list[tuple[int, float, float, str, int, str]]] = {
        symbol: [] for symbol in manifest.symbols
    }
    seen_rows: set[tuple[str, int]] = set()
    for segments in by_artifact.values():
        artifact = segments[0].source_artifact
        artifact_path = Path(artifact.path).expanduser()
        artifact_path = (
            artifact_path.resolve()
            if artifact_path.is_absolute()
            else (source_artifact_root / artifact_path).resolve()
        )
        if artifact.size_bytes is None:
            raise ReconstructionPersistenceError(
                "observed anchor source artifact lacks byte size"
            )
        _verify_cached_exact_file(
            artifact_path,
            expected_size=artifact.size_bytes,
            expected_sha256=artifact.sha256,
            label="observed anchor source artifact",
        )
        source_stat = artifact_path.stat()
        batch_index = _source_arrow_batch_row_index(
            str(artifact_path),
            source_stat.st_size,
            source_stat.st_mtime_ns,
            source_stat.st_ctime_ns,
            artifact.sha256,
        )
        with pa.memory_map(str(artifact_path), "r") as source:
            try:
                reader = ipc.open_file(source)
            except Exception as err:
                raise ReconstructionPersistenceError(
                    "observed anchor source artifact is not Arrow IPC"
                ) from err
            dt_index = reader.schema.get_field_index("datetime")
            bid_index = reader.schema.get_field_index("bid")
            ask_index = reader.schema.get_field_index("ask")
            if min(dt_index, bid_index, ask_index) < 0:
                raise ReconstructionPersistenceError(
                    "observed anchor source lacks datetime/bid/ask"
                )
            remaining = set(range(len(segments)))
            for batch_ordinal, batch_start, row_count in batch_index:
                batch = reader.get_batch(batch_ordinal)
                if batch.num_rows != row_count:
                    raise ReconstructionPersistenceError(
                        "observed anchor source batch index changed"
                    )
                batch_end = batch_start + row_count - 1
                for index in tuple(remaining):
                    segment = segments[index]
                    if segment.source_row_end_id < batch_start:
                        remaining.remove(index)
                        continue
                    if segment.source_row_start_id > batch_end:
                        continue
                    overlap_start = max(
                        segment.source_row_start_id, batch_start
                    )
                    overlap_end = min(segment.source_row_end_id, batch_end)
                    offset = overlap_start - batch_start
                    length = overlap_end - overlap_start + 1
                    times = (
                        batch.column(dt_index).slice(offset, length).to_pylist()
                    )
                    bids = (
                        batch.column(bid_index)
                        .slice(offset, length)
                        .to_pylist()
                    )
                    asks = (
                        batch.column(ask_index)
                        .slice(offset, length)
                        .to_pylist()
                    )
                    quote_policy = artifact.metadata.get(
                        "quote_order_projection_policy"
                    )
                    for position, (timestamp_ms, bid_raw, ask_raw) in enumerate(
                        zip(times, bids, asks, strict=True)
                    ):
                        row_id = overlap_start + position
                        row_key = (segment.source_series_id, row_id)
                        if row_key in seen_rows:
                            raise ReconstructionPersistenceError(
                                "observed anchor segments overlap"
                            )
                        seen_rows.add(row_key)
                        bid, ask = _canonical_source_quote_for_replay(
                            bid_raw, ask_raw, quote_policy
                        )
                        raw[segment.symbol].append(
                            (
                                _strict_int(timestamp_ms, "source datetime")
                                * 1_000_000,
                                bid,
                                ask,
                                segment.source_period,
                                row_id,
                                segment.source_series_id,
                            )
                        )
                    if overlap_end == segment.source_row_end_id:
                        remaining.remove(index)
            if remaining:
                raise ReconstructionPersistenceError(
                    "observed anchor row range exceeds source artifact"
                )
    events: list[SyntheticEventV1] = []
    for symbol, rows in sorted(raw.items()):
        sequences: dict[int, int] = {}
        for timestamp, bid, ask, period, row_id, series_id in sorted(
            rows, key=lambda row: (row[0], row[3], row[4], row[5])
        ):
            sequence = sequences.get(timestamp, 0)
            sequences[timestamp] = sequence + 1
            events.append(
                SyntheticEventV1.observed(
                    symbol=symbol,
                    event_time_ns=timestamp,
                    event_sequence=sequence,
                    bid=bid,
                    ask=ask,
                    run_id=manifest.run_id,
                    ensemble_member_id=manifest.ensemble_member_id,
                    source_version_id=next(
                        item.source_version_id
                        for item in manifest.observed_anchor_segments
                        if item.source_series_id == series_id
                    ),
                    source_series_id=series_id,
                    source_period=period,
                    source_row_id=row_id,
                )
            )
    return tuple(sorted(events, key=_event_order_key))


def _canonical_source_quote_for_replay(
    bid_raw: object,
    ask_raw: object,
    quote_order_projection_policy: object,
) -> tuple[float, float]:
    """Apply only the source-declared HistData quote-order projection."""
    try:
        bid = float(cast(Any, bid_raw))
        ask = float(cast(Any, ask_raw))
    except (TypeError, ValueError) as err:
        raise ReconstructionPersistenceError(
            "observed anchor bid/ask are not numeric"
        ) from err
    if not math.isfinite(bid) or not math.isfinite(ask) or min(bid, ask) <= 0.0:
        raise ReconstructionPersistenceError(
            "observed anchor bid/ask are not finite and positive"
        )
    if ask >= bid:
        return bid, ask
    if (
        quote_order_projection_policy
        != "rowwise-min-bid-max-ask-preserve-raw-v1"
    ):
        raise ReconstructionPersistenceError(
            "negative source spread lacks its declared quote-order projection"
        )
    return ask, bid


def _verify_publication_directory(
    directory: Path,
    manifest: (
        ReconstructionProductManifestV1
        | ReconstructionProductManifestV2
        | ReconstructionProductManifestV3
    ),
    *,
    require_committed_layout: bool,
    source_artifact_root: Path | None = None,
) -> None:
    manifest_path = directory / RECONSTRUCTION_MANIFEST_FILENAME
    disk_manifest = load_reconstruction_manifest(manifest_path)
    if disk_manifest != manifest:
        raise ReconstructionPersistenceError(
            "manifest bytes differ from staged publication evidence"
        )
    if require_committed_layout:
        _validate_committed_manifest_location(manifest_path, manifest)
    observed_events: tuple[SyntheticEventV1, ...] = ()
    anchor_event_ids: tuple[str, ...] = ()
    if isinstance(manifest, ReconstructionProductManifestV3):
        observed_events = _read_observed_anchor_events(
            manifest,
            source_artifact_root
            or _reconstruction_product_root_from_path(directory),
        )
        anchor_event_ids = tuple(event.event_id for event in observed_events)
    for partition in manifest.partitions:
        _validate_partition_file(
            directory / partition.relative_path,
            partition,
            manifest.replay.row_group_size,
            expected_writer_id=manifest.replay.writer_id,
            anchor_event_ids=anchor_event_ids,
        )
    actual_byte_hash = _partition_byte_digest(manifest.partitions)
    if actual_byte_hash != manifest.replay.partition_byte_sha256:
        raise ReconstructionPersistenceError(
            "partition byte aggregate differs from replay manifest"
        )
    if isinstance(manifest, ReconstructionProductManifestV3):
        synthetic_events = tuple(
            event
            for partition in manifest.partitions
            for event in _iter_partition_events(
                directory / partition.relative_path,
                anchor_event_ids=anchor_event_ids,
            )
        )
        if any(
            event.origin is not SyntheticEventOrigin.SYNTHETIC
            for event in synthetic_events
        ):
            raise ReconstructionPersistenceError(
                "synthetic-delta partitions contain observed rows"
            )
        logical_events = tuple(
            sorted((*observed_events, *synthetic_events), key=_event_order_key)
        )
        if reconstruction_logical_content_sha256(logical_events) != (
            manifest.replay.logical_content_sha256
        ):
            raise ReconstructionPersistenceError(
                "synthetic-delta logical replay hash differs"
            )
        if _observed_content_sha256(observed_events) != (
            manifest.source.observed_content_sha256
        ):
            raise ReconstructionPersistenceError(
                "synthetic-delta observed-anchor hash differs"
            )
        if _text_sequence_sha256(
            event.event_id for event in observed_events
        ) != (manifest.source.observed_event_ids_sha256):
            raise ReconstructionPersistenceError(
                "synthetic-delta observed IDs differ"
            )
        if (
            len(logical_events),
            len(observed_events),
            len(synthetic_events),
        ) != (
            manifest.event_count,
            manifest.observed_event_count,
            manifest.synthetic_event_count,
        ):
            raise ReconstructionPersistenceError(
                "synthetic-delta event counts differ"
            )
        return
    digest = hashlib.sha256(_LOGICAL_HASH_HEADER)
    observed_digest = hashlib.sha256(_OBSERVED_HASH_HEADER)
    observed_ids: list[str] = []
    total = 0
    observed = 0
    synthetic = 0
    for partition in manifest.partitions:
        for event in _iter_partition_events(
            directory / partition.relative_path
        ):
            _update_event_digest(digest, event)
            total += 1
            if event.origin is SyntheticEventOrigin.OBSERVED:
                _update_event_digest(observed_digest, event)
                observed_ids.append(event.event_id)
                observed += 1
            else:
                synthetic += 1
    if digest.hexdigest() != manifest.replay.logical_content_sha256:
        raise ReconstructionPersistenceError(
            "clean replay logical hash differs from manifest"
        )
    if observed_digest.hexdigest() != manifest.source.observed_content_sha256:
        raise ReconstructionPersistenceError(
            "replayed observed-anchor hash differs from source manifest"
        )
    if _text_sequence_sha256(observed_ids) != (
        manifest.source.observed_event_ids_sha256
    ):
        raise ReconstructionPersistenceError(
            "replayed observed IDs differ from source manifest"
        )
    if (total, observed, synthetic) != (
        manifest.event_count,
        manifest.observed_event_count,
        manifest.synthetic_event_count,
    ):
        raise ReconstructionPersistenceError(
            "replayed event counts differ from manifest"
        )


def _find_matching_publication(
    root: Path,
    group: BrokerRenderedGroupV1,
    *,
    symbol_group_id: str,
    retention_plan: ReconstructionRetentionPlanV1,
    row_group_size: int,
) -> PublishedReconstructionV1 | None:
    logical_hash = reconstruction_logical_content_sha256(
        event for stream in group.streams for event in stream.events
    )
    for path in discover_reconstruction_manifests(
        root,
        run_id=group.manifest.run_id,
        broker_profile_id=group.manifest.fingerprint_id,
        ensemble_member_id=group.manifest.ensemble_member_id,
        symbol_group_id=symbol_group_id,
    ):
        manifest = load_reconstruction_manifest(path)
        if not isinstance(manifest, ReconstructionProductManifestV1):
            continue
        if (
            manifest.replay.logical_content_sha256 == logical_hash
            and manifest.quality.broker_transfer_manifest_id
            == group.manifest.manifest_id
            and manifest.retention.plan_id == retention_plan.plan_id
        ):
            if manifest.replay.row_group_size != row_group_size:
                raise ReconstructionPersistenceError(
                    "existing logical publication uses a different "
                    "row-group configuration"
                )
            manifest = verify_reconstruction_publication(path)
            if not isinstance(manifest, ReconstructionProductManifestV1):
                raise ReconstructionPersistenceError(
                    "legacy publication lookup restored a delivery manifest"
                )
            return PublishedReconstructionV1(
                manifest=manifest,
                manifest_path=path,
                manifest_ref=_artifact_ref_for_manifest(path, manifest),
                idempotent_retry=True,
            )
    return None


def _validate_committed_manifest_location(
    path: Path,
    manifest: (
        ReconstructionProductManifestV1
        | ReconstructionProductManifestV2
        | ReconstructionProductManifestV3
    ),
) -> None:
    if path.name != RECONSTRUCTION_MANIFEST_FILENAME:
        raise ReconstructionPersistenceError("unexpected manifest filename")
    publication = path.parent
    if publication.name != _path_component(manifest.publication_id):
        raise ReconstructionPersistenceError(
            "publication directory differs from manifest identity"
        )
    if publication.parent.name != "commits":
        raise ReconstructionPersistenceError(
            "manifest is not below the committed publication axis"
        )
    axis = publication.parent.parent
    profile_axis: str
    schema_version: str
    if isinstance(manifest, ReconstructionProductManifestV1):
        profile_axis = f"broker={_path_component(manifest.broker_profile_id)}"
        schema_version = RECONSTRUCTION_PRODUCT_SCHEMA_VERSION
    elif isinstance(manifest, ReconstructionProductManifestV2):
        profile_axis = (
            f"delivery={_path_component(manifest.delivery_profile_id)}"
        )
        schema_version = RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION
    else:
        profile_axis = (
            f"delivery={_path_component(manifest.delivery_profile_id)}"
        )
        schema_version = RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION
    expected_axes = (
        f"group={_path_component(manifest.symbol_group_id)}",
        f"member={_path_component(manifest.ensemble_member_id)}",
        profile_axis,
        f"run={_path_component(manifest.run_id)}",
        f"schema={_path_component(schema_version)}",
    )
    cursor = axis
    for expected in expected_axes:
        if cursor.name != expected:
            raise ReconstructionPersistenceError(
                "committed manifest axes differ from manifest content"
            )
        cursor = cursor.parent
    if cursor.name != RECONSTRUCTION_PRODUCT_DIRECTORY:
        raise ReconstructionPersistenceError(
            "manifest is outside the reconstruction product root"
        )


def _reconstruction_product_root_from_path(path: Path) -> Path:
    """Locate the portable product-bundle root from a committed descendant."""
    resolved = path.expanduser().resolve()
    for candidate in (resolved, *resolved.parents):
        if candidate.name == RECONSTRUCTION_PRODUCT_DIRECTORY:
            return candidate
    raise ReconstructionPersistenceError(
        "cannot resolve reconstruction product root for source artifacts"
    )


def _axis_directory(
    root: Path,
    *,
    run_id: str,
    broker_profile_id: str,
    ensemble_member_id: str,
    symbol_group_id: str,
) -> Path:
    return (
        root
        / RECONSTRUCTION_PRODUCT_DIRECTORY
        / f"schema={_path_component(RECONSTRUCTION_PRODUCT_SCHEMA_VERSION)}"
        / f"run={_path_component(run_id)}"
        / f"broker={_path_component(broker_profile_id)}"
        / f"member={_path_component(ensemble_member_id)}"
        / f"group={_path_component(symbol_group_id)}"
    )


def _delivery_axis_directory(
    root: Path,
    *,
    run_id: str,
    delivery_profile_id: str,
    ensemble_member_id: str,
    symbol_group_id: str,
    schema_version: str = RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION,
) -> Path:
    if schema_version not in {
        RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION,
        RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION,
    }:
        raise ValueError("unsupported generic-delivery product schema")
    return (
        root
        / RECONSTRUCTION_PRODUCT_DIRECTORY
        / f"schema={_path_component(schema_version)}"
        / f"run={_path_component(run_id)}"
        / f"delivery={_path_component(delivery_profile_id)}"
        / f"member={_path_component(ensemble_member_id)}"
        / f"group={_path_component(symbol_group_id)}"
    )


def _partition_relative_path(symbol: str, event_date: str) -> str:
    return f"symbol={_path_component(symbol)}/event_date={event_date}/part-00000.parquet"


def _safe_relative_path(value: str) -> str:
    text = _required_text(value)
    path = PurePosixPath(text)
    if path.is_absolute() or ".." in path.parts or "\\" in text:
        raise ValueError("artifact path must be a safe relative POSIX path")
    return path.as_posix()


def _partition_byte_digest(
    partitions: Iterable[ReconstructionProductPartitionV1],
) -> str:
    payload: list[dict[str, JSONValue]] = [
        {
            "relative_path": item.relative_path,
            "byte_sha256": item.byte_sha256,
            "size_bytes": item.size_bytes,
        }
        for item in sorted(partitions, key=lambda value: value.relative_path)
    ]
    return _content_sha256(
        {
            "algorithm": RECONSTRUCTION_BYTE_HASH_ALGORITHM,
            "partitions": payload,
        }
    )


def _observed_content_sha256(events: Iterable[SyntheticEventV1]) -> str:
    digest = hashlib.sha256(_OBSERVED_HASH_HEADER)
    for event in sorted(events, key=_event_order_key):
        _update_event_digest(digest, event)
    return digest.hexdigest()


def _text_sequence_sha256(values: Iterable[str]) -> str:
    digest = hashlib.sha256(b"histdatacom-text-sequence-v1\n")
    for value in sorted(_required_text(item) for item in values):
        digest.update(value.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _lineage_assignments_sha256(
    events: Iterable[SyntheticEventV1],
    field_name: str,
) -> str:
    digest = hashlib.sha256(
        f"histdatacom-{field_name}-assignments-v1\n".encode("ascii")
    )
    for event in sorted(events, key=_event_order_key):
        value = getattr(event, field_name)
        digest.update(event.event_id.encode("utf-8"))
        digest.update(b"=")
        digest.update(str(value or "").encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _update_event_digest(digest: Any, event: SyntheticEventV1) -> None:
    if not isinstance(event, SyntheticEventV1):
        raise TypeError("logical reconstruction hashes require event rows")
    digest.update(canonical_contract_json(event.to_dict()).encode("utf-8"))
    digest.update(b"\n")


def _event_order_key(event: SyntheticEventV1) -> tuple[str, int, int, str]:
    return (
        event.symbol,
        event.event_time_ns,
        event.event_sequence,
        event.event_id,
    )


def _broker_streams_content_sha256(
    streams: Iterable[SyntheticEventStreamV1],
) -> str:
    return _content_sha256(
        [
            item.to_dict()
            for item in sorted(streams, key=lambda value: value.symbol)
        ]
    )


def _content_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    return f"{prefix}:sha256:{_content_sha256(payload)}"


def _artifact_ref_for_manifest(
    path: Path,
    manifest: (
        ReconstructionProductManifestV1
        | ReconstructionProductManifestV2
        | ReconstructionProductManifestV3
    ),
) -> ArtifactRef:
    payload = path.read_bytes()
    return ArtifactRef(
        kind=RECONSTRUCTION_MANIFEST_ARTIFACT_KIND,
        path=str(path),
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        metadata={
            "schema_version": manifest.schema_version,
            "publication_id": manifest.publication_id,
            "manifest_id": manifest.manifest_id,
            "event_count": manifest.event_count,
            "logical_content_sha256": (manifest.replay.logical_content_sha256),
            "immutable_anchor_content_sha256": (
                manifest.source.observed_content_sha256
            ),
        },
    )


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary_name = tempfile.mkstemp(
        prefix=path.name + ".tmp-", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(handle, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if temporary.exists():
            temporary.unlink()


def _fsync_file(path: Path) -> None:
    # Windows maps fsync to the writable-handle-only CRT commit operation.
    # Reopen completed Parquet artifacts without truncation but with write
    # access so the durability boundary has the same semantics on every OS.
    with path.open("rb+") as stream:
        os.fsync(stream.fileno())


def _fsync_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _remove_scratch_entry(path: Path, root: Path) -> None:
    root_resolved = root.resolve()
    if path.is_symlink():
        path.unlink(missing_ok=True)
        return
    resolved = path.resolve()
    try:
        resolved.relative_to(root_resolved / RECONSTRUCTION_PRODUCT_DIRECTORY)
    except ValueError as err:
        raise ReconstructionPersistenceError(
            "scratch cleanup refused a path outside the product root"
        ) from err
    if ".scratch" not in resolved.parts or not path.name.startswith(
        "publication.tmp-"
    ):
        raise ReconstructionPersistenceError(
            "scratch cleanup refused a non-transaction path"
        )
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024**2):
            digest.update(chunk)
    return digest.hexdigest()


def _verify_cached_exact_file(
    path: Path,
    *,
    expected_size: int,
    expected_sha256: str,
    label: str,
) -> None:
    """Hash immutable large artifacts once per unchanged filesystem identity."""
    if not path.is_file():
        raise ReconstructionPersistenceError(f"{label} is missing")
    stat = path.stat()
    if stat.st_size != expected_size:
        raise ReconstructionPersistenceError(f"{label} byte size differs")
    actual = _cached_file_sha256(
        str(path.resolve()),
        stat.st_size,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
    )
    if actual != expected_sha256:
        raise ReconstructionPersistenceError(f"{label} hash differs")


@lru_cache(maxsize=2_048)
def _cached_file_sha256(
    path: str,
    size_bytes: int,
    modified_ns: int,
    changed_ns: int,
) -> str:
    """Return a hash keyed by path and mutation-sensitive stat evidence."""
    del size_bytes, modified_ns, changed_ns
    return _file_sha256(Path(path))


@lru_cache(maxsize=2_048)
def _source_arrow_batch_row_index(
    path: str,
    size_bytes: int,
    modified_ns: int,
    changed_ns: int,
    artifact_sha256: str,
) -> tuple[tuple[int, int, int], ...]:
    """Cache one-based row offsets so delta replay opens only used batches."""
    del size_bytes, modified_ns, changed_ns, artifact_sha256
    try:
        import pyarrow as pa
        from pyarrow import ipc
    except ImportError as err:  # pragma: no cover - package dependency
        raise RuntimeError("synthetic-delta replay requires pyarrow") from err
    indexed: list[tuple[int, int, int]] = []
    row_start = 1
    with pa.memory_map(path, "r") as source:
        reader = ipc.open_file(source)
        for batch_ordinal in range(reader.num_record_batches):
            row_count = reader.get_batch(batch_ordinal).num_rows
            if row_count:
                indexed.append((batch_ordinal, row_start, row_count))
            row_start += row_count
    return tuple(indexed)


def _event_date(event_time_ns: int) -> str:
    seconds, remainder = divmod(
        _strict_int(event_time_ns, "event_time_ns"), 10**9
    )
    del remainder
    return datetime.fromtimestamp(seconds, tz=timezone.utc).date().isoformat()


def _path_component(value: str) -> str:
    return quote(_required_text(value), safe="-_.")


def _estimated_event_bytes(
    count: int, bytes_per_event: int, ratio: float
) -> int:
    return math.ceil(count * bytes_per_event * ratio)


def _arrow_modules() -> tuple[Any, Any]:
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError as err:
        raise RuntimeError(
            "reconstruction persistence requires histdatacom[arrow]"
        ) from err
    return pa, pq


def _arrow_dataset_modules() -> tuple[Any, Any, Any]:
    pa, pq = _arrow_modules()
    try:
        import pyarrow.dataset as ds
    except ImportError as err:
        raise RuntimeError(
            "reconstruction scans require pyarrow.dataset"
        ) from err
    return pa, pq, ds


def _polars_module() -> Any:
    try:
        import polars as pl
    except ImportError as err:
        raise RuntimeError("reconstruction scans require polars") from err
    return pl


def _required_text(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > MAX_RECONSTRUCTION_TEXT:
        raise ValueError("required reconstruction text is empty or unbounded")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return _required_text(normalized)


def _normalized_symbol(value: str) -> str:
    normalized = _required_text(value).lower()
    if not normalized.isalnum():
        raise ValueError("reconstruction symbols must be alphanumeric")
    return normalized


def _normalized_text_tuple(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({_required_text(value) for value in values}))


def _required_sha256(value: Any, name: str) -> str:
    normalized = str(value or "").strip().lower()
    if not _SHA256_RE.fullmatch(normalized):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return normalized


def _required_event_date(value: str) -> str:
    normalized = _required_text(value)
    if not _EVENT_DATE_RE.fullmatch(normalized):
        raise ValueError("event_date must be ISO YYYY-MM-DD")
    try:
        if datetime.fromisoformat(normalized).date().isoformat() != normalized:
            raise ValueError
    except ValueError as err:
        raise ValueError("event_date is invalid") from err
    return normalized


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer")
    return value


def _optional_int(value: Any, name: str) -> int | None:
    if value is None:
        return None
    return _strict_int(value, name)


def _strict_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    return _finite_float(float(value), name)


def _finite_float(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as err:
        raise ValueError(f"{name} must be numeric") from err
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number


def _nonnegative_int(value: Any, name: str) -> int:
    number = _strict_int(value, name)
    if number < 0:
        raise ValueError(f"{name} must be non-negative")
    return number


def _positive_int(value: Any, name: str) -> int:
    number = _strict_int(value, name)
    if number < 1:
        raise ValueError(f"{name} must be positive")
    return number


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError(f"{name} must be an object")
    return cast(Mapping[str, Any], value)


def _bounded_json_value_mapping(
    value: Mapping[str, Any],
    name: str,
    *,
    maximum_bytes: int,
) -> dict[str, JSONValue]:
    """Normalize a compact JSON mapping and enforce its wire-size bound."""
    encoded = canonical_contract_json(dict(value)).encode("utf-8")
    if len(encoded) > maximum_bytes:
        raise ValueError(f"{name} exceeds its bounded payload limit")
    restored = json.loads(encoded)
    if not isinstance(restored, dict):  # pragma: no cover - mapping encoded
        raise TypeError(f"{name} must encode as an object")
    return cast(dict[str, JSONValue], restored)


def _mapping_sequence(value: Any, name: str) -> tuple[Mapping[str, Any], ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{name} must be a sequence")
    if not all(isinstance(item, Mapping) for item in value):
        raise ValueError(f"{name} must contain objects")
    return tuple(cast(Mapping[str, Any], item) for item in value)


def _string_tuple(value: Any, name: str) -> tuple[str, ...]:
    if isinstance(value, (str, bytes, bytearray)) or not isinstance(
        value, Sequence
    ):
        raise TypeError(f"{name} must be a sequence")
    return tuple(str(item) for item in value)


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as err:
        raise ReconstructionPersistenceError(
            "reconstruction manifest is invalid JSON"
        ) from err
    if not isinstance(value, Mapping):
        raise ReconstructionPersistenceError(
            "reconstruction manifest must contain an object"
        )
    return cast(Mapping[str, Any], value)


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    _require_version(str(data.get("schema_version", "")), expected, "schema")


def _require_version(value: str, expected: str, name: str) -> None:
    if value != expected:
        raise ValueError(f"unsupported {name} version")


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ValueError(f"derived field {name} differs")


__all__ = [
    "DEFAULT_ESTIMATED_BYTES_PER_EVENT",
    "DEFAULT_ESTIMATED_COMPRESSION_RATIO",
    "DEFAULT_RECONSTRUCTION_ROW_GROUP_SIZE",
    "RECONSTRUCTION_BYTE_HASH_ALGORITHM",
    "RECONSTRUCTION_COMPRESSION",
    "RECONSTRUCTION_CONSTRAINT_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_DELIVERY_QUALITY_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_ENSEMBLE_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_LOGICAL_HASH_ALGORITHM",
    "RECONSTRUCTION_MANIFEST_ARTIFACT_KIND",
    "RECONSTRUCTION_MANIFEST_FILENAME",
    "RECONSTRUCTION_OBSERVED_ANCHOR_SEGMENT_SCHEMA_VERSION",
    "RECONSTRUCTION_PARTITION_SCHEMA_VERSION",
    "RECONSTRUCTION_PRODUCT_DIRECTORY",
    "RECONSTRUCTION_PRODUCT_SCHEMA_VERSION",
    "RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION",
    "RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION",
    "RECONSTRUCTION_QUALITY_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_REPLAY_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_REPLAY_V2_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_RETENTION_PLAN_SCHEMA_VERSION",
    "RECONSTRUCTION_SOURCE_MANIFEST_SCHEMA_VERSION",
    "RECONSTRUCTION_SYNTHETIC_DELTA_WRITER_ID",
    "RECONSTRUCTION_WRITER_ID",
    "PublishedReconstructionV1",
    "PublishedReconstructionV2",
    "PublishedReconstructionV3",
    "ReconstructionConstraintManifestV1",
    "ReconstructionDeliveryQualityManifestV1",
    "ReconstructionEnsembleManifestV1",
    "ReconstructionObservedAnchorSegmentV1",
    "ReconstructionPersistenceError",
    "ReconstructionProductManifestV1",
    "ReconstructionProductManifestV2",
    "ReconstructionProductManifestV3",
    "ReconstructionProductPartitionV1",
    "ReconstructionQualityManifestV1",
    "ReconstructionReplayManifestV1",
    "ReconstructionReplayManifestV2",
    "ReconstructionRetentionPlanV1",
    "ReconstructionSourceManifestV1",
    "ReconstructionStoragePreflightError",
    "StagedReconstructionPublicationV1",
    "StagedReconstructionPublicationV2",
    "StagedReconstructionPublicationV3",
    "cleanup_reconstruction_scratch",
    "commit_delivery_reconstruction_publication",
    "commit_reconstruction_publication",
    "discover_reconstruction_manifests",
    "estimate_reconstruction_retention",
    "iter_reconstruction_event_batches",
    "load_reconstruction_manifest",
    "publish_reconstruction_group",
    "read_reconstruction_streams",
    "reconstruction_logical_content_sha256",
    "reconstruction_parquet_paths",
    "scan_reconstruction_events_polars",
    "stage_delivery_reconstruction_publication",
    "stage_reconstruction_publication",
    "verify_reconstruction_publication",
]
