"""Catalog-bound, leakage-safe HistData reconstruction experiments.

The contracts in this module compose existing authoritative dataset, evidence,
benchmark, and model artifacts.  They do not copy tick rows or replace the
specialized manifests that own those domains.  Scientific identity excludes
local materialization paths while retaining strong content references.

The v1 executable policy is intentionally narrow: HistData.com ASCII tick
partitions, timeframe ``T``, and observed evidence only.  Provider-neutral
dataset identities remain the architectural seam for later milestones; this
module does not admit alternate providers, broker feeds, or OANDA data.
"""

from __future__ import annotations

import hashlib
import json
import os
import platform
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Any

from histdatacom.datasets import (
    DATASET_CATALOG_SCHEMA_VERSION,
    DatasetAliasV1,
    DatasetCatalog,
    DatasetContractError,
    DatasetDescriptorV1,
    DatasetOrigin,
    DatasetQueryScopeV1,
    DatasetResolutionV1,
    DatasetVersionManifestV1,
    HistDataProviderAdapter,
    build_observed_dataset_version,
)
from histdatacom.orchestration.reconstruction import (
    artifact_ref_for_file,
    verify_artifact_ref,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

RECONSTRUCTION_EXPERIMENT_IMPLEMENTATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-implementation.v1"
)
RECONSTRUCTION_EXPERIMENT_SELECTION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-selection.v1"
)
RECONSTRUCTION_EXPERIMENT_SPLIT_POLICY_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-split-policy.v1"
)
RECONSTRUCTION_EXPERIMENT_SPLIT_UNIT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-split-unit.v1"
)
RECONSTRUCTION_EXPERIMENT_LEAKAGE_AUDIT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-leakage-audit.v1"
)
RECONSTRUCTION_EXPERIMENT_ARTIFACT_BINDING_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-artifact-binding.v1"
)
RECONSTRUCTION_EXPERIMENT_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-manifest.v1"
)
RECONSTRUCTION_EXPERIMENT_VERIFICATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-experiment-verification.v1"
)

RECONSTRUCTION_EXPERIMENT_ARTIFACT_KIND = (
    "reconstruction_experiment_manifest_v1"
)
RECONSTRUCTION_EXPERIMENT_RESOLUTION_ARTIFACT_KIND = (
    "dataset_resolution_receipt_v1"
)
RECONSTRUCTION_EXPERIMENT_CATALOG_ARTIFACT_KIND = "dataset_catalog_v1"

CURRENT_EXPERIMENT_PROVIDER_ID = "histdata.com"
CURRENT_EXPERIMENT_SOURCE_FORMAT = "ascii"
CURRENT_EXPERIMENT_TIMEFRAME = "T"
CURRENT_EXPERIMENT_DATASET_ID = "histdata.ascii-t"
CURRENT_EXPERIMENT_ALIAS = "reconstruction-selected"

MAX_EXPERIMENT_SELECTIONS = 32
MAX_EXPERIMENT_SPLIT_UNITS = 4096
MAX_EXPERIMENT_BINDINGS = 256
MAX_EXPERIMENT_ITEMS = 4096
MAX_EXPERIMENT_BYTES = 16 * 1024 * 1024
MAX_BOUND_EXPERIMENT_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_EXPERIMENT_STRING = 4096

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PERIOD_RE = re.compile(r"^\d{6}$")
_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9._:-]{0,255}$")
_JSON_FIELD_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)
_ALLOWED_EXPORT_FORMATS = frozenset({"arrow", "parquet"})
_ALLOWED_TIMESTAMP_MASKING = frozenset(
    {"none", "event_time_masked_row_identity_retained"}
)
_TRAINING_ROLES = frozenset(
    {
        "modern_reference_training",
        "tuning",
        "calibration",
    }
)
_EVALUATION_ROLES = frozenset(
    {"protected_holdout", "negative_control", "product_input"}
)


class ReconstructionExperimentError(ValueError):
    """A frozen experiment, selection, split, or binding failed closed."""


class ReconstructionExperimentRole(str, Enum):
    """Explicit dataset use roles kept separate from provider and origin."""

    HISTORICAL_ANCHOR = "historical_anchor"
    MODERN_REFERENCE_TRAINING = "modern_reference_training"
    TUNING = "tuning"
    CALIBRATION = "calibration"
    PROTECTED_HOLDOUT = "protected_holdout"
    NEGATIVE_CONTROL = "negative_control"
    PRODUCT_INPUT = "product_input"

    @classmethod
    def from_value(
        cls, value: str | ReconstructionExperimentRole
    ) -> ReconstructionExperimentRole:
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ReconstructionExperimentError(
                f"unsupported reconstruction experiment role {value!r}"
            ) from err


class ReconstructionExperimentSplitStrategy(str, Enum):
    """Supported whole-unit split strategies."""

    CHRONOLOGICAL = "chronological"
    WHOLE_PERIOD = "whole_period"
    REGIME = "regime"
    EVENT = "event"
    SYMBOL = "symbol"
    COMPOSITE = "composite"


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentImplementationV1:
    """Bounded package, dependency, runtime, and source-code identity."""

    package_version: str
    python_implementation: str
    python_version: str
    dependency_versions: Mapping[str, str]
    module_sha256: Mapping[str, str]
    implementation_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_EXPERIMENT_IMPLEMENTATION_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_IMPLEMENTATION_SCHEMA_VERSION,
        )
        for name in (
            "package_version",
            "python_implementation",
            "python_version",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        dependencies = _text_mapping(
            self.dependency_versions, "dependency_versions"
        )
        modules = {
            _required_text(name): _sha256(value)
            for name, value in self.module_sha256.items()
        }
        if not modules:
            raise ReconstructionExperimentError(
                "experiment implementation requires module hashes"
            )
        object.__setattr__(self, "dependency_versions", dependencies)
        object.__setattr__(self, "module_sha256", dict(sorted(modules.items())))
        _bind_id(
            self,
            "implementation_id",
            "reconstruction-experiment-implementation",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "package_version": self.package_version,
            "python_implementation": self.python_implementation,
            "python_version": self.python_version,
            "dependency_versions": dict(self.dependency_versions),
            "module_sha256": dict(self.module_sha256),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "implementation_id": self.implementation_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExperimentImplementationV1:
        return cls(
            package_version=str(data.get("package_version", "")),
            python_implementation=str(data.get("python_implementation", "")),
            python_version=str(data.get("python_version", "")),
            dependency_versions={
                str(key): str(value)
                for key, value in _mapping(
                    data.get("dependency_versions")
                ).items()
            },
            module_sha256={
                str(key): str(value)
                for key, value in _mapping(data.get("module_sha256")).items()
            },
            implementation_id=str(data.get("implementation_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentSelectionV1:
    """One exact catalog resolution and its separate semantic use roles."""

    roles: tuple[ReconstructionExperimentRole, ...]
    catalog_id: str
    catalog_ref: ArtifactRef
    resolution: DatasetResolutionV1
    resolution_ref: ArtifactRef
    source_provider_ids: tuple[str, ...]
    dataset_origin: DatasetOrigin
    delivery_profile_id: str | None
    source_format: str
    timeframe: str
    partition_ids: tuple[str, ...]
    row_count: int
    coverage_start_ns: int
    coverage_end_ns: int
    local_materialization_root: str
    selection_id: str = ""
    schema_version: str = RECONSTRUCTION_EXPERIMENT_SELECTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_SELECTION_SCHEMA_VERSION,
        )
        roles = _roles(self.roles)
        if not roles:
            raise ReconstructionExperimentError(
                "experiment selection requires at least one role"
            )
        catalog_id = _content_id(self.catalog_id, "dataset-catalog")
        catalog_ref = _strong_ref(self.catalog_ref)
        resolution_ref = _strong_ref(self.resolution_ref)
        if catalog_ref.kind != RECONSTRUCTION_EXPERIMENT_CATALOG_ARTIFACT_KIND:
            raise ReconstructionExperimentError(
                "experiment selection requires a dataset catalog artifact"
            )
        if (
            resolution_ref.kind
            != RECONSTRUCTION_EXPERIMENT_RESOLUTION_ARTIFACT_KIND
        ):
            raise ReconstructionExperimentError(
                "experiment selection requires a resolution receipt artifact"
            )
        if not isinstance(self.resolution, DatasetResolutionV1):
            raise TypeError("resolution must use DatasetResolutionV1")
        providers = _text_tuple(self.source_provider_ids)
        if providers != (CURRENT_EXPERIMENT_PROVIDER_ID,):
            raise ReconstructionExperimentError(
                "v2.4 experiments admit only HistData.com source partitions"
            )
        origin = DatasetOrigin.from_value(self.dataset_origin)
        if origin is not DatasetOrigin.OBSERVED:
            raise ReconstructionExperimentError(
                "v2.4 experiment inputs must be observed HistData evidence"
            )
        source_format = str(self.source_format).strip().lower()
        timeframe = str(self.timeframe).strip()
        if (
            source_format != CURRENT_EXPERIMENT_SOURCE_FORMAT
            or timeframe != CURRENT_EXPERIMENT_TIMEFRAME
        ):
            raise ReconstructionExperimentError(
                "v2.4 experiments require HistData ASCII/T tick partitions"
            )
        partitions = _text_tuple(self.partition_ids)
        if not partitions:
            raise ReconstructionExperimentError(
                "experiment selection has no immutable partitions"
            )
        row_count = _positive_int(self.row_count, "row_count")
        start = _int64(self.coverage_start_ns, "coverage_start_ns")
        end = _int64(self.coverage_end_ns, "coverage_end_ns")
        if end <= start:
            raise ReconstructionExperimentError(
                "experiment selection coverage is empty"
            )
        root = str(
            Path(_required_text(self.local_materialization_root))
            .expanduser()
            .resolve()
        )
        object.__setattr__(self, "roles", roles)
        object.__setattr__(self, "catalog_id", catalog_id)
        object.__setattr__(self, "catalog_ref", catalog_ref)
        object.__setattr__(self, "resolution_ref", resolution_ref)
        object.__setattr__(self, "source_provider_ids", providers)
        object.__setattr__(self, "dataset_origin", origin)
        object.__setattr__(
            self,
            "delivery_profile_id",
            _optional_text(self.delivery_profile_id),
        )
        object.__setattr__(self, "source_format", source_format)
        object.__setattr__(self, "timeframe", timeframe)
        object.__setattr__(self, "partition_ids", partitions)
        object.__setattr__(self, "row_count", row_count)
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        object.__setattr__(self, "local_materialization_root", root)
        _bind_id(
            self,
            "selection_id",
            "reconstruction-experiment-selection",
            self.identity_payload(),
        )

    @property
    def dataset_id(self) -> str:
        return str(self.resolution.dataset_id)

    @property
    def dataset_version_id(self) -> str:
        return str(self.resolution.dataset_version_id)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "roles": [item.value for item in self.roles],
            "catalog_id": self.catalog_id,
            "resolution": self.resolution.to_dict(),
            "catalog_schema_version": DATASET_CATALOG_SCHEMA_VERSION,
            "resolution_id": self.resolution.resolution_id,
            "dataset_id": self.dataset_id,
            "dataset_version_id": self.dataset_version_id,
            "source_provider_ids": list(self.source_provider_ids),
            "dataset_origin": self.dataset_origin.value,
            "delivery_profile_id": self.delivery_profile_id,
            "source_format": self.source_format,
            "timeframe": self.timeframe,
            "partition_ids": list(self.partition_ids),
            "row_count": self.row_count,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "local_materialization_is_not_scientific_identity": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "catalog_ref": self.catalog_ref.to_dict(),
            "resolution_ref": self.resolution_ref.to_dict(),
            "local_materialization_root": self.local_materialization_root,
            "selection_id": self.selection_id,
        }

    def publication_summary(self) -> dict[str, JSONValue]:
        """Return selection identity without local paths."""
        return {
            **self.identity_payload(),
            "selection_id": self.selection_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExperimentSelectionV1:
        resolution = DatasetResolutionV1.from_dict(
            _mapping(data.get("resolution"))
        )
        _require_derived(
            data, "catalog_schema_version", DATASET_CATALOG_SCHEMA_VERSION
        )
        _require_derived(data, "resolution_id", resolution.resolution_id)
        return cls(
            roles=tuple(
                ReconstructionExperimentRole.from_value(str(value))
                for value in _sequence(data.get("roles"))
            ),
            catalog_id=str(data.get("catalog_id", "")),
            catalog_ref=ArtifactRef.from_dict(
                _mapping(data.get("catalog_ref"))
            ),
            resolution=resolution,
            resolution_ref=ArtifactRef.from_dict(
                _mapping(data.get("resolution_ref"))
            ),
            source_provider_ids=_text_tuple(data.get("source_provider_ids")),
            dataset_origin=DatasetOrigin.from_value(
                str(data.get("dataset_origin", ""))
            ),
            delivery_profile_id=_optional_text(data.get("delivery_profile_id")),
            source_format=str(data.get("source_format", "")),
            timeframe=str(data.get("timeframe", "")),
            partition_ids=_text_tuple(data.get("partition_ids")),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            coverage_start_ns=_strict_int(
                data.get("coverage_start_ns"), "coverage_start_ns"
            ),
            coverage_end_ns=_strict_int(
                data.get("coverage_end_ns"), "coverage_end_ns"
            ),
            local_materialization_root=str(
                data.get("local_materialization_root", "")
            ),
            selection_id=str(data.get("selection_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentSplitPolicyV1:
    """Fail-closed rules that keep dependent observations in one split."""

    strategy: ReconstructionExperimentSplitStrategy = (
        ReconstructionExperimentSplitStrategy.COMPOSITE
    )
    neighbor_guard_ns: int = 0
    keep_adjacent_ticks_together: bool = True
    keep_duplicate_timestamps_together: bool = True
    keep_overlapping_windows_together: bool = True
    keep_context_events_together: bool = True
    keep_anchor_neighborhoods_together: bool = True
    preserve_row_identity_when_timestamp_masked: bool = True
    assignments_frozen_before_candidate_results: bool = True
    policy_id: str = ""
    schema_version: str = RECONSTRUCTION_EXPERIMENT_SPLIT_POLICY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_SPLIT_POLICY_SCHEMA_VERSION,
        )
        object.__setattr__(
            self,
            "strategy",
            ReconstructionExperimentSplitStrategy(self.strategy),
        )
        object.__setattr__(
            self,
            "neighbor_guard_ns",
            _nonnegative_int(self.neighbor_guard_ns, "neighbor_guard_ns"),
        )
        for name in (
            "keep_adjacent_ticks_together",
            "keep_duplicate_timestamps_together",
            "keep_overlapping_windows_together",
            "keep_context_events_together",
            "keep_anchor_neighborhoods_together",
            "preserve_row_identity_when_timestamp_masked",
            "assignments_frozen_before_candidate_results",
        ):
            if getattr(self, name) is not True:
                raise ReconstructionExperimentError(
                    f"v1 split policy requires {name}=true"
                )
        _bind_id(
            self,
            "policy_id",
            "reconstruction-experiment-split-policy",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "strategy": self.strategy.value,
            "neighbor_guard_ns": self.neighbor_guard_ns,
            "keep_adjacent_ticks_together": True,
            "keep_duplicate_timestamps_together": True,
            "keep_overlapping_windows_together": True,
            "keep_context_events_together": True,
            "keep_anchor_neighborhoods_together": True,
            "preserve_row_identity_when_timestamp_masked": True,
            "assignments_frozen_before_candidate_results": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "policy_id": self.policy_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExperimentSplitPolicyV1:
        return cls(
            strategy=ReconstructionExperimentSplitStrategy(
                str(data.get("strategy", ""))
            ),
            neighbor_guard_ns=_strict_int(
                data.get("neighbor_guard_ns"), "neighbor_guard_ns"
            ),
            keep_adjacent_ticks_together=_strict_bool(
                data.get("keep_adjacent_ticks_together"),
                "keep_adjacent_ticks_together",
            ),
            keep_duplicate_timestamps_together=_strict_bool(
                data.get("keep_duplicate_timestamps_together"),
                "keep_duplicate_timestamps_together",
            ),
            keep_overlapping_windows_together=_strict_bool(
                data.get("keep_overlapping_windows_together"),
                "keep_overlapping_windows_together",
            ),
            keep_context_events_together=_strict_bool(
                data.get("keep_context_events_together"),
                "keep_context_events_together",
            ),
            keep_anchor_neighborhoods_together=_strict_bool(
                data.get("keep_anchor_neighborhoods_together"),
                "keep_anchor_neighborhoods_together",
            ),
            preserve_row_identity_when_timestamp_masked=_strict_bool(
                data.get("preserve_row_identity_when_timestamp_masked"),
                "preserve_row_identity_when_timestamp_masked",
            ),
            assignments_frozen_before_candidate_results=_strict_bool(
                data.get("assignments_frozen_before_candidate_results"),
                "assignments_frozen_before_candidate_results",
            ),
            policy_id=str(data.get("policy_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentSplitUnitV1:
    """One indivisible set of partitions/windows/events assigned together."""

    selection_id: str
    roles: tuple[ReconstructionExperimentRole, ...]
    partition_ids: tuple[str, ...]
    symbols: tuple[str, ...]
    periods: tuple[str, ...]
    start_ns: int
    end_ns: int
    cohesion_group_ids: tuple[str, ...]
    row_identity_policy_ids: tuple[str, ...]
    selected_fields: tuple[str, ...]
    label_fields: tuple[str, ...] = ()
    timestamp_masking: str = "none"
    unit_id: str = ""
    schema_version: str = RECONSTRUCTION_EXPERIMENT_SPLIT_UNIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_SPLIT_UNIT_SCHEMA_VERSION,
        )
        object.__setattr__(
            self, "selection_id", _required_text(self.selection_id)
        )
        roles = _roles(self.roles)
        partitions = _text_tuple(self.partition_ids)
        symbols = tuple(sorted({_symbol(value) for value in self.symbols}))
        periods = tuple(sorted({_period(value) for value in self.periods}))
        if not roles or not partitions or not symbols or not periods:
            raise ReconstructionExperimentError(
                "split unit requires roles, partitions, symbols, and periods"
            )
        start = _int64(self.start_ns, "start_ns")
        end = _int64(self.end_ns, "end_ns")
        if end <= start:
            raise ReconstructionExperimentError("split unit interval is empty")
        cohesion = _text_tuple(self.cohesion_group_ids)
        if not cohesion:
            raise ReconstructionExperimentError(
                "split unit requires declared cohesion groups"
            )
        row_policies = _text_tuple(self.row_identity_policy_ids)
        if not row_policies:
            raise ReconstructionExperimentError(
                "split unit requires row identity policy"
            )
        selected = _field_tuple(self.selected_fields)
        labels = _field_tuple(self.label_fields)
        if not selected:
            raise ReconstructionExperimentError(
                "split unit requires selected source fields"
            )
        masking = str(self.timestamp_masking).strip().lower()
        if masking not in _ALLOWED_TIMESTAMP_MASKING:
            raise ReconstructionExperimentError(
                "unsupported experiment timestamp masking policy"
            )
        object.__setattr__(self, "roles", roles)
        object.__setattr__(self, "partition_ids", partitions)
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "periods", periods)
        object.__setattr__(self, "start_ns", start)
        object.__setattr__(self, "end_ns", end)
        object.__setattr__(self, "cohesion_group_ids", cohesion)
        object.__setattr__(self, "row_identity_policy_ids", row_policies)
        object.__setattr__(self, "selected_fields", selected)
        object.__setattr__(self, "label_fields", labels)
        object.__setattr__(self, "timestamp_masking", masking)
        _bind_id(
            self,
            "unit_id",
            "reconstruction-experiment-split-unit",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "selection_id": self.selection_id,
            "roles": [item.value for item in self.roles],
            "partition_ids": list(self.partition_ids),
            "symbols": list(self.symbols),
            "periods": list(self.periods),
            "start_ns": self.start_ns,
            "end_ns": self.end_ns,
            "interval": "[start_ns,end_ns)",
            "cohesion_group_ids": list(self.cohesion_group_ids),
            "row_identity_policy_ids": list(self.row_identity_policy_ids),
            "selected_fields": list(self.selected_fields),
            "label_fields": list(self.label_fields),
            "timestamp_masking": self.timestamp_masking,
            "full_tick_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "unit_id": self.unit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExperimentSplitUnitV1:
        _require_derived(data, "interval", "[start_ns,end_ns)")
        _require_derived(data, "full_tick_rows_embedded", False)
        return cls(
            selection_id=str(data.get("selection_id", "")),
            roles=tuple(
                ReconstructionExperimentRole.from_value(str(value))
                for value in _sequence(data.get("roles"))
            ),
            partition_ids=_text_tuple(data.get("partition_ids")),
            symbols=_text_tuple(data.get("symbols")),
            periods=_text_tuple(data.get("periods")),
            start_ns=_strict_int(data.get("start_ns"), "start_ns"),
            end_ns=_strict_int(data.get("end_ns"), "end_ns"),
            cohesion_group_ids=_text_tuple(data.get("cohesion_group_ids")),
            row_identity_policy_ids=_text_tuple(
                data.get("row_identity_policy_ids")
            ),
            selected_fields=_text_tuple(data.get("selected_fields")),
            label_fields=_text_tuple(data.get("label_fields")),
            timestamp_masking=str(data.get("timestamp_masking", "")),
            unit_id=str(data.get("unit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentLeakageAuditV1:
    """Recomputable partition, overlap, adjacency, and cohesion audit."""

    split_policy_id: str
    split_unit_ids: tuple[str, ...]
    finding_codes: tuple[str, ...]
    shared_partition_count: int
    shared_cohesion_group_count: int
    overlap_count: int
    neighbor_guard_violation_count: int
    accepted: bool
    audit_id: str = ""
    schema_version: str = RECONSTRUCTION_EXPERIMENT_LEAKAGE_AUDIT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_LEAKAGE_AUDIT_SCHEMA_VERSION,
        )
        object.__setattr__(
            self, "split_policy_id", _required_text(self.split_policy_id)
        )
        units = _text_tuple(self.split_unit_ids)
        findings = _text_tuple(self.finding_codes)
        object.__setattr__(self, "split_unit_ids", units)
        object.__setattr__(self, "finding_codes", findings)
        counts = []
        for name in (
            "shared_partition_count",
            "shared_cohesion_group_count",
            "overlap_count",
            "neighbor_guard_violation_count",
        ):
            value = _nonnegative_int(getattr(self, name), name)
            object.__setattr__(self, name, value)
            counts.append(value)
        expected_accepted = not findings and not any(counts)
        if self.accepted is not expected_accepted:
            raise ReconstructionExperimentError(
                "experiment leakage audit status differs from findings"
            )
        _bind_id(
            self,
            "audit_id",
            "reconstruction-experiment-leakage-audit",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "split_policy_id": self.split_policy_id,
            "split_unit_ids": list(self.split_unit_ids),
            "finding_codes": list(self.finding_codes),
            "shared_partition_count": self.shared_partition_count,
            "shared_cohesion_group_count": (self.shared_cohesion_group_count),
            "overlap_count": self.overlap_count,
            "neighbor_guard_violation_count": (
                self.neighbor_guard_violation_count
            ),
            "accepted": self.accepted,
            "assignments_frozen_before_candidate_results": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "audit_id": self.audit_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExperimentLeakageAuditV1:
        _require_derived(
            data, "assignments_frozen_before_candidate_results", True
        )
        return cls(
            split_policy_id=str(data.get("split_policy_id", "")),
            split_unit_ids=_text_tuple(data.get("split_unit_ids")),
            finding_codes=_text_tuple(data.get("finding_codes")),
            shared_partition_count=_strict_int(
                data.get("shared_partition_count"), "shared_partition_count"
            ),
            shared_cohesion_group_count=_strict_int(
                data.get("shared_cohesion_group_count"),
                "shared_cohesion_group_count",
            ),
            overlap_count=_strict_int(
                data.get("overlap_count"), "overlap_count"
            ),
            neighbor_guard_violation_count=_strict_int(
                data.get("neighbor_guard_violation_count"),
                "neighbor_guard_violation_count",
            ),
            accepted=_strict_bool(data.get("accepted"), "accepted"),
            audit_id=str(data.get("audit_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentArtifactBindingV1:
    """Strong reference to one existing authoritative domain manifest."""

    name: str
    domain: str
    artifact: ArtifactRef
    artifact_id: str
    artifact_identity_field: str
    dataset_roles: tuple[ReconstructionExperimentRole, ...]
    split_unit_ids: tuple[str, ...] = ()
    schema_versions: tuple[str, ...] = ()
    available_at_ns: int | None = None
    limitations: tuple[str, ...] = ()
    binding_id: str = ""
    schema_version: str = (
        RECONSTRUCTION_EXPERIMENT_ARTIFACT_BINDING_SCHEMA_VERSION
    )

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_ARTIFACT_BINDING_SCHEMA_VERSION,
        )
        name = _identifier(self.name, "binding name")
        domain = _identifier(self.domain, "binding domain")
        if domain.startswith(("broker", "oanda", "alternate-provider")):
            raise ReconstructionExperimentError(
                "broker/OANDA/alternate-provider bindings are outside v2.4"
            )
        artifact = _strong_ref(self.artifact)
        artifact_id = _required_text(self.artifact_id)
        identity_field = _json_field_name(self.artifact_identity_field)
        roles = _roles(self.dataset_roles)
        if not roles:
            raise ReconstructionExperimentError(
                "artifact binding requires dataset roles"
            )
        split_units = _text_tuple(self.split_unit_ids)
        versions = _text_tuple(self.schema_versions)
        available = (
            None
            if self.available_at_ns is None
            else _int64(self.available_at_ns, "available_at_ns")
        )
        limitations = _bounded_text_tuple(self.limitations)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "domain", domain)
        object.__setattr__(self, "artifact", artifact)
        object.__setattr__(self, "artifact_id", artifact_id)
        object.__setattr__(self, "artifact_identity_field", identity_field)
        object.__setattr__(self, "dataset_roles", roles)
        object.__setattr__(self, "split_unit_ids", split_units)
        object.__setattr__(self, "schema_versions", versions)
        object.__setattr__(self, "available_at_ns", available)
        object.__setattr__(self, "limitations", limitations)
        _bind_id(
            self,
            "binding_id",
            "reconstruction-experiment-artifact-binding",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "name": self.name,
            "domain": self.domain,
            "artifact": _ref_identity(self.artifact),
            "artifact_id": self.artifact_id,
            "artifact_identity_field": self.artifact_identity_field,
            "dataset_roles": [item.value for item in self.dataset_roles],
            "split_unit_ids": list(self.split_unit_ids),
            "schema_versions": list(self.schema_versions),
            "available_at_ns": self.available_at_ns,
            "limitations": list(self.limitations),
            "large_rows_embedded": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "artifact": self.artifact.to_dict(),
            "binding_id": self.binding_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExperimentArtifactBindingV1:
        _require_derived(data, "large_rows_embedded", False)
        return cls(
            name=str(data.get("name", "")),
            domain=str(data.get("domain", "")),
            artifact=ArtifactRef.from_dict(_mapping(data.get("artifact"))),
            artifact_id=str(data.get("artifact_id", "")),
            artifact_identity_field=str(
                data.get("artifact_identity_field", "")
            ),
            dataset_roles=tuple(
                ReconstructionExperimentRole.from_value(str(value))
                for value in _sequence(data.get("dataset_roles"))
            ),
            split_unit_ids=_text_tuple(data.get("split_unit_ids")),
            schema_versions=_text_tuple(data.get("schema_versions")),
            available_at_ns=_optional_int(data.get("available_at_ns")),
            limitations=_text_tuple(data.get("limitations")),
            binding_id=str(data.get("binding_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentManifestV1:
    """One immutable identity composing exact data, splits, and artifacts."""

    selections: tuple[ReconstructionExperimentSelectionV1, ...]
    split_policy: ReconstructionExperimentSplitPolicyV1
    split_units: tuple[ReconstructionExperimentSplitUnitV1, ...]
    leakage_audit: ReconstructionExperimentLeakageAuditV1
    artifact_bindings: tuple[ReconstructionExperimentArtifactBindingV1, ...]
    evidence_policy_ids: tuple[str, ...]
    preprocessing_ids: tuple[str, ...]
    feature_schema_versions: tuple[str, ...]
    benchmark_gate_ids: tuple[str, ...]
    implementation: ReconstructionExperimentImplementationV1
    limitations: tuple[str, ...]
    export_formats: tuple[str, ...] = ()
    experiment_id: str = ""
    schema_version: str = RECONSTRUCTION_EXPERIMENT_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_MANIFEST_SCHEMA_VERSION,
        )
        selections = tuple(
            sorted(self.selections, key=lambda item: item.selection_id)
        )
        if not selections or len(selections) > MAX_EXPERIMENT_SELECTIONS:
            raise ReconstructionExperimentError(
                "experiment selection count is outside limits"
            )
        _unique_ids(
            (item.selection_id for item in selections),
            "experiment selection",
        )
        if not isinstance(
            self.split_policy, ReconstructionExperimentSplitPolicyV1
        ):
            raise TypeError("split_policy must use the v1 contract")
        units = tuple(sorted(self.split_units, key=lambda item: item.unit_id))
        if not units or len(units) > MAX_EXPERIMENT_SPLIT_UNITS:
            raise ReconstructionExperimentError(
                "experiment split-unit count is outside limits"
            )
        _unique_ids((item.unit_id for item in units), "experiment split unit")
        selection_map = {item.selection_id: item for item in selections}
        for unit in units:
            selection = selection_map.get(unit.selection_id)
            if selection is None:
                raise ReconstructionExperimentError(
                    "split unit references an unknown experiment selection"
                )
            if not set(unit.partition_ids).issubset(selection.partition_ids):
                raise ReconstructionExperimentError(
                    "split unit references partitions outside its selection"
                )
            if not set(unit.roles).issubset(selection.roles):
                raise ReconstructionExperimentError(
                    "split-unit role is outside its selection roles"
                )
            if (
                unit.start_ns < selection.coverage_start_ns
                or unit.end_ns > selection.coverage_end_ns
            ):
                raise ReconstructionExperimentError(
                    "split-unit interval is outside its selection coverage"
                )
            scope = selection.resolution.query_scope
            if scope.symbols and not set(unit.symbols).issubset(
                {value.upper() for value in scope.symbols}
            ):
                raise ReconstructionExperimentError(
                    "split-unit symbols are outside its catalog resolution"
                )
            if scope.periods and not set(unit.periods).issubset(scope.periods):
                raise ReconstructionExperimentError(
                    "split-unit periods are outside its catalog resolution"
                )
        for selection in selections:
            assigned = tuple(
                item
                for item in units
                if item.selection_id == selection.selection_id
            )
            if not assigned:
                raise ReconstructionExperimentError(
                    "experiment selection has no frozen split assignment"
                )
            assigned_roles = {role for item in assigned for role in item.roles}
            if assigned_roles != set(selection.roles):
                raise ReconstructionExperimentError(
                    "experiment selection roles are not completely assigned"
                )
            partition_counts = Counter(
                partition_id
                for item in assigned
                for partition_id in item.partition_ids
            )
            if set(partition_counts) != set(selection.partition_ids) or any(
                count != 1 for count in partition_counts.values()
            ):
                raise ReconstructionExperimentError(
                    "experiment selection partitions require one frozen split unit"
                )
        expected_audit = audit_reconstruction_experiment_splits(
            self.split_policy, units
        )
        if self.leakage_audit != expected_audit:
            raise ReconstructionExperimentError(
                "experiment leakage audit differs from recomputed evidence"
            )
        if not self.leakage_audit.accepted:
            raise ReconstructionExperimentError(
                "experiment split assignments fail leakage policy"
            )
        bindings = tuple(
            sorted(self.artifact_bindings, key=lambda item: item.name)
        )
        if not bindings or len(bindings) > MAX_EXPERIMENT_BINDINGS:
            raise ReconstructionExperimentError(
                "experiment requires bounded authoritative artifact bindings"
            )
        _unique_ids((item.name for item in bindings), "artifact binding name")
        unit_ids = {item.unit_id for item in units}
        experiment_roles = {
            role for selection in selections for role in selection.roles
        }
        for binding in bindings:
            if not set(binding.dataset_roles).issubset(experiment_roles):
                raise ReconstructionExperimentError(
                    "artifact binding declares a role absent from the experiment"
                )
            if not set(binding.split_unit_ids).issubset(unit_ids):
                raise ReconstructionExperimentError(
                    "artifact binding references an unknown split unit"
                )
        for name in (
            "evidence_policy_ids",
            "preprocessing_ids",
            "feature_schema_versions",
            "benchmark_gate_ids",
        ):
            values = _text_tuple(getattr(self, name))
            if not values:
                raise ReconstructionExperimentError(
                    f"experiment requires {name}"
                )
            object.__setattr__(self, name, values)
        if not isinstance(
            self.implementation, ReconstructionExperimentImplementationV1
        ):
            raise TypeError("implementation must use the v1 contract")
        limitations = _bounded_text_tuple(self.limitations)
        exports = tuple(
            sorted(
                {str(value).strip().lower() for value in self.export_formats}
            )
        )
        if not set(exports).issubset(_ALLOWED_EXPORT_FORMATS):
            raise ReconstructionExperimentError(
                "experiment export format must be Arrow or Parquet"
            )
        object.__setattr__(self, "selections", selections)
        object.__setattr__(self, "split_units", units)
        object.__setattr__(self, "artifact_bindings", bindings)
        object.__setattr__(self, "limitations", limitations)
        object.__setattr__(self, "export_formats", exports)
        _bind_id(
            self,
            "experiment_id",
            "reconstruction-experiment",
            self.identity_payload(),
        )
        if len(self.to_json().encode("utf-8")) > MAX_EXPERIMENT_BYTES:
            raise ReconstructionExperimentError(
                "reconstruction experiment manifest exceeds byte limit"
            )

    @property
    def dataset_version_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted({item.dataset_version_id for item in self.selections})
        )

    @property
    def catalog_ids(self) -> tuple[str, ...]:
        return tuple(sorted({item.catalog_id for item in self.selections}))

    @property
    def roles(self) -> tuple[ReconstructionExperimentRole, ...]:
        return tuple(
            sorted(
                {role for item in self.selections for role in item.roles},
                key=lambda item: item.value,
            )
        )

    def selection_for_role(
        self, role: ReconstructionExperimentRole
    ) -> ReconstructionExperimentSelectionV1:
        selected = ReconstructionExperimentRole.from_value(role)
        matches = tuple(
            item for item in self.selections if selected in item.roles
        )
        if len(matches) != 1:
            raise ReconstructionExperimentError(
                f"experiment requires exactly one {selected.value} selection"
            )
        return matches[0]

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "selections": [item.identity_payload() for item in self.selections],
            "split_policy": self.split_policy.to_dict(),
            "split_units": [item.to_dict() for item in self.split_units],
            "leakage_audit": self.leakage_audit.to_dict(),
            "artifact_bindings": [
                item.identity_payload() for item in self.artifact_bindings
            ],
            "evidence_policy_ids": list(self.evidence_policy_ids),
            "preprocessing_ids": list(self.preprocessing_ids),
            "feature_schema_versions": list(self.feature_schema_versions),
            "benchmark_gate_ids": list(self.benchmark_gate_ids),
            "implementation": self.implementation.to_dict(),
            "limitations": list(self.limitations),
            "export_formats": list(self.export_formats),
            "export_metadata_emitted": bool(self.export_formats),
            "dataset_version_ids": list(self.dataset_version_ids),
            "catalog_ids": list(self.catalog_ids),
            "roles": [item.value for item in self.roles],
            "specialized_manifests_remain_authoritative": True,
            "full_tick_rows_embedded": False,
            "assignments_frozen_before_candidate_results": True,
            "provider_neutral_identity_histdata_only_execution": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "selections": [item.to_dict() for item in self.selections],
            "artifact_bindings": [
                item.to_dict() for item in self.artifact_bindings
            ],
            "experiment_id": self.experiment_id,
        }

    def publication_summary(self) -> dict[str, JSONValue]:
        """Return bounded discovery metadata with every local path removed."""
        return {
            **self.identity_payload(),
            "selections": [
                item.publication_summary() for item in self.selections
            ],
            "artifact_bindings": [
                {
                    **item.identity_payload(),
                    "binding_id": item.binding_id,
                }
                for item in self.artifact_bindings
            ],
            "experiment_id": self.experiment_id,
        }

    def to_json(self) -> str:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> ReconstructionExperimentManifestV1:
        for name, expected in (
            ("specialized_manifests_remain_authoritative", True),
            ("full_tick_rows_embedded", False),
            ("assignments_frozen_before_candidate_results", True),
            ("provider_neutral_identity_histdata_only_execution", True),
        ):
            _require_derived(data, name, expected)
        return cls(
            selections=tuple(
                ReconstructionExperimentSelectionV1.from_dict(_mapping(value))
                for value in _sequence(data.get("selections"))
            ),
            split_policy=ReconstructionExperimentSplitPolicyV1.from_dict(
                _mapping(data.get("split_policy"))
            ),
            split_units=tuple(
                ReconstructionExperimentSplitUnitV1.from_dict(_mapping(value))
                for value in _sequence(data.get("split_units"))
            ),
            leakage_audit=ReconstructionExperimentLeakageAuditV1.from_dict(
                _mapping(data.get("leakage_audit"))
            ),
            artifact_bindings=tuple(
                ReconstructionExperimentArtifactBindingV1.from_dict(
                    _mapping(value)
                )
                for value in _sequence(data.get("artifact_bindings"))
            ),
            evidence_policy_ids=_text_tuple(data.get("evidence_policy_ids")),
            preprocessing_ids=_text_tuple(data.get("preprocessing_ids")),
            feature_schema_versions=_text_tuple(
                data.get("feature_schema_versions")
            ),
            benchmark_gate_ids=_text_tuple(data.get("benchmark_gate_ids")),
            implementation=ReconstructionExperimentImplementationV1.from_dict(
                _mapping(data.get("implementation"))
            ),
            limitations=_text_tuple(data.get("limitations")),
            export_formats=_text_tuple(data.get("export_formats")),
            experiment_id=str(data.get("experiment_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> ReconstructionExperimentManifestV1:
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionExperimentVerificationV1:
    """Bounded verification result without local paths or row payloads."""

    experiment_id: str
    catalog_ids: tuple[str, ...]
    dataset_version_ids: tuple[str, ...]
    verified_partition_count: int
    verified_binding_count: int
    split_unit_count: int
    leakage_audit_id: str
    implementation_id: str
    status: str
    finding_codes: tuple[str, ...] = ()
    verification_id: str = ""
    schema_version: str = RECONSTRUCTION_EXPERIMENT_VERIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version,
            RECONSTRUCTION_EXPERIMENT_VERIFICATION_SCHEMA_VERSION,
        )
        for name in (
            "experiment_id",
            "leakage_audit_id",
            "implementation_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(self, "catalog_ids", _text_tuple(self.catalog_ids))
        object.__setattr__(
            self, "dataset_version_ids", _text_tuple(self.dataset_version_ids)
        )
        for name in (
            "verified_partition_count",
            "verified_binding_count",
            "split_unit_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        findings = _text_tuple(self.finding_codes)
        status = str(self.status).strip().lower()
        expected = "verified" if not findings else "failed"
        if status != expected:
            raise ReconstructionExperimentError(
                "experiment verification status differs from findings"
            )
        object.__setattr__(self, "finding_codes", findings)
        object.__setattr__(self, "status", status)
        _bind_id(
            self,
            "verification_id",
            "reconstruction-experiment-verification",
            self.identity_payload(),
        )

    @property
    def verified(self) -> bool:
        return self.status == "verified"

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "experiment_id": self.experiment_id,
            "catalog_ids": list(self.catalog_ids),
            "dataset_version_ids": list(self.dataset_version_ids),
            "verified_partition_count": self.verified_partition_count,
            "verified_binding_count": self.verified_binding_count,
            "split_unit_count": self.split_unit_count,
            "leakage_audit_id": self.leakage_audit_id,
            "implementation_id": self.implementation_id,
            "status": self.status,
            "finding_codes": list(self.finding_codes),
            "publication_safe": True,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "verification_id": self.verification_id,
        }


def audit_reconstruction_experiment_splits(
    policy: ReconstructionExperimentSplitPolicyV1,
    units: Sequence[ReconstructionExperimentSplitUnitV1],
) -> ReconstructionExperimentLeakageAuditV1:
    """Recompute split isolation without reading tick rows."""
    if not isinstance(policy, ReconstructionExperimentSplitPolicyV1):
        raise TypeError("policy must use ReconstructionExperimentSplitPolicyV1")
    ordered = tuple(sorted(units, key=lambda item: item.unit_id))
    if not ordered:
        raise ReconstructionExperimentError("split audit requires units")
    shared_partitions: set[str] = set()
    shared_groups: set[str] = set()
    overlap_pairs: set[str] = set()
    guard_pairs: set[str] = set()
    for index, left in enumerate(ordered):
        for right in ordered[index + 1 :]:
            if _compatible_role_sets(left.roles, right.roles):
                continue
            shared_partitions.update(
                set(left.partition_ids) & set(right.partition_ids)
            )
            shared_groups.update(
                set(left.cohesion_group_ids) & set(right.cohesion_group_ids)
            )
            if not set(left.symbols).intersection(right.symbols):
                continue
            pair_id = f"{left.unit_id}|{right.unit_id}"
            if left.start_ns < right.end_ns and right.start_ns < left.end_ns:
                overlap_pairs.add(pair_id)
                continue
            if not _training_evaluation_pair(left.roles, right.roles):
                continue
            gap = max(left.start_ns, right.start_ns) - min(
                left.end_ns, right.end_ns
            )
            if gap < policy.neighbor_guard_ns:
                guard_pairs.add(pair_id)
    findings: set[str] = set()
    if shared_partitions:
        findings.add("partition_cross_split_reuse")
    if shared_groups:
        findings.add("cohesion_group_cross_split_reuse")
    if overlap_pairs:
        findings.add("temporal_overlap_cross_split")
    if guard_pairs:
        findings.add("neighbor_guard_cross_split")
    return ReconstructionExperimentLeakageAuditV1(
        split_policy_id=policy.policy_id,
        split_unit_ids=tuple(item.unit_id for item in ordered),
        finding_codes=tuple(sorted(findings)),
        shared_partition_count=len(shared_partitions),
        shared_cohesion_group_count=len(shared_groups),
        overlap_count=len(overlap_pairs),
        neighbor_guard_violation_count=len(guard_pairs),
        accepted=not findings,
    )


def current_reconstruction_experiment_implementation(
    *,
    module_paths: Mapping[str, str | Path] | None = None,
) -> ReconstructionExperimentImplementationV1:
    """Resolve deterministic installed code/dependency identity."""
    selected_modules = dict(module_paths or _default_module_paths())
    hashes = {
        str(name): _file_sha256(Path(path).expanduser().resolve())
        for name, path in sorted(selected_modules.items())
    }
    dependencies: dict[str, str] = {}
    for distribution in ("numpy", "polars", "pyarrow", "scipy", "pydantic"):
        try:
            dependencies[distribution] = importlib_metadata.version(
                distribution
            )
        except importlib_metadata.PackageNotFoundError:
            dependencies[distribution] = "unavailable"
    try:
        package_version = importlib_metadata.version("histdatacom")
    except importlib_metadata.PackageNotFoundError:
        package_version = "source-tree"
    return ReconstructionExperimentImplementationV1(
        package_version=package_version,
        python_implementation=platform.python_implementation(),
        python_version=platform.python_version(),
        dependency_versions=dependencies,
        module_sha256=hashes,
    )


def build_legacy_histdata_catalog(
    source_root: str | Path,
    *,
    symbols: Iterable[str],
    periods: Iterable[str],
    qualification_evidence: Iterable[ArtifactRef],
    path: str | Path,
) -> tuple[DatasetCatalog, Path, DatasetVersionManifestV1]:
    """Translate the v2.3 raw path into an exact local catalog once."""
    adapter = HistDataProviderAdapter()
    descriptor = DatasetDescriptorV1(
        dataset_id=CURRENT_EXPERIMENT_DATASET_ID,
        display_name="HistData ASCII tick evidence",
        description=(
            "Local immutable HistData.com ASCII/T cache partitions selected "
            "for reconstruction."
        ),
        allowed_origins=(DatasetOrigin.OBSERVED,),
    )
    version = build_observed_dataset_version(
        adapter,
        source_root,
        descriptor,
        symbols=symbols,
        periods=periods,
        qualification_evidence=qualification_evidence,
    )
    catalog = DatasetCatalog(
        providers=(adapter.provider,),
        adapters=(adapter.descriptor,),
        datasets=(descriptor,),
        versions=(version,),
        aliases=(
            DatasetAliasV1(
                alias=CURRENT_EXPERIMENT_ALIAS,
                dataset_id=descriptor.dataset_id,
                dataset_version_id=version.dataset_version_id,
                revision=1,
            ),
        ),
    )
    target = catalog.write(path)
    return catalog, target, version


def freeze_histdata_reconstruction_experiment(
    *,
    catalog_path: str | Path,
    dataset_reference: str,
    query_scope: DatasetQueryScopeV1,
    roles: Iterable[ReconstructionExperimentRole],
    output_directory: str | Path,
    artifact_bindings: Iterable[ReconstructionExperimentArtifactBindingV1],
    evidence_policy_ids: Iterable[str],
    preprocessing_ids: Iterable[str],
    feature_schema_versions: Iterable[str],
    benchmark_gate_ids: Iterable[str],
    split_policy: ReconstructionExperimentSplitPolicyV1 | None = None,
    split_units: Iterable[ReconstructionExperimentSplitUnitV1] | None = None,
    implementation: ReconstructionExperimentImplementationV1 | None = None,
    limitations: Iterable[str] = (),
    export_formats: Iterable[str] = (),
    resolution: DatasetResolutionV1 | None = None,
) -> tuple[ReconstructionExperimentManifestV1, ArtifactRef]:
    """Freeze and persist one exact HistData experiment graph."""
    catalog_path_resolved = Path(catalog_path).expanduser().resolve()
    catalog = DatasetCatalog.read(catalog_path_resolved)
    exact = resolution or catalog.resolve(
        dataset_reference, query_scope=query_scope
    )
    exact = catalog.replay(exact)
    catalog.verify(exact)
    version = _catalog_version(catalog, exact.dataset_version_id)
    _require_current_histdata_version(version)
    selected_partitions = _selected_partitions(version, exact.query_scope)
    root = _histdata_materialization_root(selected_partitions)
    output = Path(output_directory).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    catalog_ref = artifact_ref_for_file(
        catalog_path_resolved,
        kind=RECONSTRUCTION_EXPERIMENT_CATALOG_ARTIFACT_KIND,
        metadata={
            "catalog_id": catalog.catalog_id,
            "schema_version": DATASET_CATALOG_SCHEMA_VERSION,
        },
    )
    resolution_path = output / (
        "dataset-resolution-" + _id_digest(exact.resolution_id) + ".json"
    )
    _atomic_write(resolution_path, exact.to_json())
    resolution_ref = artifact_ref_for_file(
        resolution_path,
        kind=RECONSTRUCTION_EXPERIMENT_RESOLUTION_ARTIFACT_KIND,
        metadata={
            "resolution_id": exact.resolution_id,
            "dataset_version_id": exact.dataset_version_id,
        },
    )
    selected_roles = _roles(tuple(roles))
    selection = ReconstructionExperimentSelectionV1(
        roles=selected_roles,
        catalog_id=catalog.catalog_id,
        catalog_ref=catalog_ref,
        resolution=exact,
        resolution_ref=resolution_ref,
        source_provider_ids=version.source_provider_ids,
        dataset_origin=version.origin,
        delivery_profile_id=version.delivery_profile_id,
        source_format=CURRENT_EXPERIMENT_SOURCE_FORMAT,
        timeframe=CURRENT_EXPERIMENT_TIMEFRAME,
        partition_ids=tuple(item.partition_id for item in selected_partitions),
        row_count=sum(item.row_count for item in selected_partitions),
        coverage_start_ns=min(
            item.coverage_start_ns for item in selected_partitions
        ),
        coverage_end_ns=max(
            item.coverage_end_ns for item in selected_partitions
        ),
        local_materialization_root=str(root),
    )
    policy = split_policy or ReconstructionExperimentSplitPolicyV1()
    selected_units = tuple(split_units or ())
    if not selected_units:
        selected_units = (_default_split_unit(selection, selected_partitions),)
    audit = audit_reconstruction_experiment_splits(policy, selected_units)
    manifest = ReconstructionExperimentManifestV1(
        selections=(selection,),
        split_policy=policy,
        split_units=selected_units,
        leakage_audit=audit,
        artifact_bindings=tuple(artifact_bindings),
        evidence_policy_ids=_text_tuple(tuple(evidence_policy_ids)),
        preprocessing_ids=_text_tuple(tuple(preprocessing_ids)),
        feature_schema_versions=_text_tuple(tuple(feature_schema_versions)),
        benchmark_gate_ids=_text_tuple(tuple(benchmark_gate_ids)),
        implementation=(
            implementation or current_reconstruction_experiment_implementation()
        ),
        limitations=tuple(limitations),
        export_formats=tuple(export_formats),
    )
    manifest_path = write_reconstruction_experiment(manifest, output)
    ref = artifact_ref_for_file(
        manifest_path,
        kind=RECONSTRUCTION_EXPERIMENT_ARTIFACT_KIND,
        metadata={
            "experiment_id": manifest.experiment_id,
            "dataset_version_ids": list(manifest.dataset_version_ids),
            "catalog_ids": list(manifest.catalog_ids),
            "provider": CURRENT_EXPERIMENT_PROVIDER_ID,
            "publication_safe_summary": True,
        },
    )
    return manifest, ref


def verify_reconstruction_experiment(
    manifest: ReconstructionExperimentManifestV1,
    *,
    require_current_implementation: bool = True,
) -> ReconstructionExperimentVerificationV1:
    """Hash-verify catalogs, receipts, partitions, bindings, and code identity."""
    if not isinstance(manifest, ReconstructionExperimentManifestV1):
        raise TypeError("manifest must use ReconstructionExperimentManifestV1")
    findings: set[str] = set()
    partition_count = 0
    binding_count = 0
    for selection in manifest.selections:
        try:
            verify_artifact_ref(selection.catalog_ref)
            verify_artifact_ref(selection.resolution_ref)
            catalog = DatasetCatalog.read(selection.catalog_ref.path)
            if catalog.catalog_id != selection.catalog_id:
                raise ReconstructionExperimentError("catalog identity changed")
            receipt = DatasetResolutionV1.from_json(
                Path(selection.resolution_ref.path).read_text(encoding="utf-8")
            )
            if receipt != selection.resolution:
                raise ReconstructionExperimentError(
                    "resolution receipt differs from frozen selection"
                )
            exact = catalog.replay(receipt)
            verification = catalog.verify(exact)
            version = _catalog_version(catalog, exact.dataset_version_id)
            selected = _selected_partitions(version, exact.query_scope)
            if (
                tuple(sorted(item.partition_id for item in selected))
                != selection.partition_ids
                or sum(item.row_count for item in selected)
                != selection.row_count
                or verification.dataset_version_id
                != selection.dataset_version_id
            ):
                raise ReconstructionExperimentError(
                    "catalog selection differs from experiment"
                )
            partition_count += len(selected)
        except (OSError, TypeError, ValueError, DatasetContractError):
            findings.add("dataset_selection_verification_failed")
    for binding in manifest.artifact_bindings:
        try:
            verify_artifact_ref(binding.artifact)
            payload = _bounded_artifact_mapping(binding.artifact)
            if (
                _json_path_value(payload, binding.artifact_identity_field)
                != binding.artifact_id
            ):
                raise ReconstructionExperimentError(
                    f"artifact binding {binding.name} identity differs"
                )
            payload_schema = str(payload.get("schema_version", ""))
            if (
                binding.schema_versions
                and payload_schema not in binding.schema_versions
            ):
                raise ReconstructionExperimentError(
                    f"artifact binding {binding.name} schema differs"
                )
            binding_count += 1
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            findings.add(f"artifact_binding_verification_failed:{binding.name}")
    expected_audit = audit_reconstruction_experiment_splits(
        manifest.split_policy, manifest.split_units
    )
    if expected_audit != manifest.leakage_audit:
        findings.add("split_leakage_audit_changed")
    if require_current_implementation:
        current = current_reconstruction_experiment_implementation()
        if current != manifest.implementation:
            findings.add("implementation_identity_changed")
    return ReconstructionExperimentVerificationV1(
        experiment_id=manifest.experiment_id,
        catalog_ids=manifest.catalog_ids,
        dataset_version_ids=manifest.dataset_version_ids,
        verified_partition_count=partition_count,
        verified_binding_count=binding_count,
        split_unit_count=len(manifest.split_units),
        leakage_audit_id=manifest.leakage_audit.audit_id,
        implementation_id=manifest.implementation.implementation_id,
        status="verified" if not findings else "failed",
        finding_codes=tuple(sorted(findings)),
    )


def write_reconstruction_experiment(
    manifest: ReconstructionExperimentManifestV1, directory: str | Path
) -> Path:
    """Atomically persist one content-addressed experiment manifest."""
    root = Path(directory).expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True)
    target = root / (
        "reconstruction-experiment-"
        + _id_digest(manifest.experiment_id)
        + ".json"
    )
    encoded = manifest.to_json().encode("utf-8") + b"\n"
    if len(encoded) > MAX_EXPERIMENT_BYTES:
        raise ReconstructionExperimentError(
            "reconstruction experiment manifest exceeds byte limit"
        )
    if target.exists():
        if target.read_bytes() != encoded:
            raise ReconstructionExperimentError(
                "content-addressed experiment path collision"
            )
        return target
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, target)
    return target


def read_reconstruction_experiment(
    path: str | Path,
) -> ReconstructionExperimentManifestV1:
    """Read one bounded experiment manifest and verify its identity."""
    target = Path(path).expanduser().resolve()
    content = target.read_bytes()
    if len(content) > MAX_EXPERIMENT_BYTES:
        raise ReconstructionExperimentError(
            "reconstruction experiment manifest exceeds byte limit"
        )
    return ReconstructionExperimentManifestV1.from_json(content.decode("utf-8"))


def discover_reconstruction_experiments(
    root: str | Path,
) -> tuple[Path, ...]:
    """Discover bounded content-addressed experiment manifests."""
    selected = Path(root).expanduser().resolve()
    paths = tuple(sorted(selected.rglob("reconstruction-experiment-*.json")))
    if len(paths) > MAX_EXPERIMENT_ITEMS:
        raise ReconstructionExperimentError(
            "experiment discovery exceeds path limit"
        )
    return paths


def _default_split_unit(
    selection: ReconstructionExperimentSelectionV1,
    partitions: Sequence[Any],
) -> ReconstructionExperimentSplitUnitV1:
    periods = tuple(sorted({str(item.period) for item in partitions}))
    symbols = tuple(sorted({str(item.symbol) for item in partitions}))
    cohesion = tuple(
        sorted(
            {
                *(f"partition:{item.partition_id}" for item in partitions),
                *(f"period:{period}" for period in periods),
                *(f"triangle-period:{period}" for period in periods),
            }
        )
    )
    return ReconstructionExperimentSplitUnitV1(
        selection_id=selection.selection_id,
        roles=selection.roles,
        partition_ids=selection.partition_ids,
        symbols=symbols,
        periods=periods,
        start_ns=selection.coverage_start_ns,
        end_ns=selection.coverage_end_ns,
        cohesion_group_ids=cohesion,
        row_identity_policy_ids=tuple(
            sorted({str(item.row_identity_policy_id) for item in partitions})
        ),
        selected_fields=("datetime", "bid", "ask", "vol"),
    )


def _catalog_version(
    catalog: DatasetCatalog, dataset_version_id: str
) -> DatasetVersionManifestV1:
    match = next(
        (
            item
            for item in catalog.versions
            if item.dataset_version_id == dataset_version_id
        ),
        None,
    )
    if match is None:
        raise ReconstructionExperimentError(
            "resolved dataset version is absent from catalog"
        )
    return match


def _selected_partitions(
    version: DatasetVersionManifestV1, scope: DatasetQueryScopeV1
) -> tuple[Any, ...]:
    selected = tuple(
        item
        for item in version.partitions
        if (not scope.symbols or item.symbol in scope.symbols)
        and (not scope.periods or item.period in scope.periods)
    )
    if not selected:
        raise ReconstructionExperimentError(
            "catalog selection resolved no partitions"
        )
    if scope.symbols and scope.periods:
        expected = {
            (symbol, period)
            for symbol in scope.symbols
            for period in scope.periods
        }
        if {(item.symbol, item.period) for item in selected} != expected:
            raise ReconstructionExperimentError(
                "catalog selection is not a complete symbol/period matrix"
            )
    return selected


def _require_current_histdata_version(
    version: DatasetVersionManifestV1,
) -> None:
    if (
        version.origin is not DatasetOrigin.OBSERVED
        or version.source_provider_ids != (CURRENT_EXPERIMENT_PROVIDER_ID,)
        or not version.partitions
    ):
        raise ReconstructionExperimentError(
            "v2.4 experiments require observed HistData.com partitions"
        )
    for partition in version.partitions:
        if (
            partition.format != CURRENT_EXPERIMENT_SOURCE_FORMAT
            or partition.granularity != CURRENT_EXPERIMENT_TIMEFRAME
        ):
            raise ReconstructionExperimentError(
                "v2.4 experiments reject M1 and non-ASCII/T selections"
            )


def _histdata_materialization_root(partitions: Sequence[Any]) -> Path:
    roots: set[Path] = set()
    for partition in partitions:
        path = Path(partition.artifact.path).expanduser().resolve()
        try:
            root = path.parents[3]
        except IndexError as err:
            raise ReconstructionExperimentError(
                "HistData partition path is outside ASCII/T layout"
            ) from err
        if (
            path.name != ".data"
            or root.name.upper() != "T"
            or root.parent.name.upper() != "ASCII"
        ):
            raise ReconstructionExperimentError(
                "HistData partition path is outside ASCII/T layout"
            )
        roots.add(root)
    if len(roots) != 1:
        raise ReconstructionExperimentError(
            "experiment selection spans multiple local materialization roots"
        )
    return next(iter(roots))


def _bounded_artifact_mapping(ref: ArtifactRef) -> Mapping[str, Any]:
    path = Path(ref.path).expanduser().resolve()
    if path.stat().st_size > MAX_BOUND_EXPERIMENT_ARTIFACT_BYTES:
        raise ReconstructionExperimentError(
            "bound experiment artifact exceeds JSON inspection limit"
        )
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ReconstructionExperimentError(
            "bound experiment artifact must be a JSON object"
        )
    return payload


def _json_path_value(payload: Mapping[str, Any], path: str) -> Any:
    value: Any = payload
    for field in path.split("."):
        if not isinstance(value, Mapping) or field not in value:
            raise ReconstructionExperimentError(
                f"bound experiment artifact lacks identity path {path}"
            )
        value = value[field]
    return value


def _compatible_role_sets(
    left: Sequence[ReconstructionExperimentRole],
    right: Sequence[ReconstructionExperimentRole],
) -> bool:
    return tuple(left) == tuple(right)


def _training_evaluation_pair(
    left: Sequence[ReconstructionExperimentRole],
    right: Sequence[ReconstructionExperimentRole],
) -> bool:
    left_values = {item.value for item in left}
    right_values = {item.value for item in right}
    return bool(
        (left_values & _TRAINING_ROLES and right_values & _EVALUATION_ROLES)
        or (right_values & _TRAINING_ROLES and left_values & _EVALUATION_ROLES)
    )


def _default_module_paths() -> dict[str, Path]:
    root = Path(__file__).resolve().parent
    return {
        "histdatacom.datasets.adapters": root / "datasets" / "adapters.py",
        "histdatacom.datasets.catalog": root / "datasets" / "catalog.py",
        "histdatacom.datasets.contracts": root / "datasets" / "contracts.py",
        "histdatacom.datasets.projection": root / "datasets" / "projection.py",
        "histdatacom.reconstruction": root / "reconstruction.py",
        "histdatacom.reconstruction_experiment": Path(__file__).resolve(),
        "histdatacom.reconstruction_science": (
            root / "reconstruction_science.py"
        ),
        "histdatacom.reconstruction_schema": root / "reconstruction_schema.py",
        "histdatacom.synthetic.benchmark_corpus": (
            root / "synthetic" / "benchmark_corpus.py"
        ),
        "histdatacom.synthetic.cross_currency": (
            root / "synthetic" / "cross_currency.py"
        ),
        "histdatacom.synthetic.marked_hawkes": (
            root / "synthetic" / "marked_hawkes.py"
        ),
        "histdatacom.synthetic.proposal_engines": (
            root / "synthetic" / "proposal_engines.py"
        ),
        "histdatacom.synthetic.qualification": (
            root / "synthetic" / "qualification.py"
        ),
        "histdatacom.synthetic.reconstruction_handlers": (
            root / "synthetic" / "reconstruction_handlers.py"
        ),
        "histdatacom.synthetic.reconstruction_plan": (
            root / "synthetic" / "reconstruction_plan.py"
        ),
        "histdatacom.synthetic.streaming": root / "synthetic" / "streaming.py",
    }


def _roles(
    values: Iterable[ReconstructionExperimentRole],
) -> tuple[ReconstructionExperimentRole, ...]:
    return tuple(
        sorted(
            {
                ReconstructionExperimentRole.from_value(value)
                for value in values
            },
            key=lambda item: item.value,
        )
    )


def _ref_identity(ref: ArtifactRef) -> dict[str, JSONValue]:
    return {
        "kind": ref.kind,
        "size_bytes": ref.size_bytes,
        "sha256": ref.sha256,
    }


def _strong_ref(value: ArtifactRef) -> ArtifactRef:
    if not isinstance(value, ArtifactRef):
        raise TypeError("artifact must use ArtifactRef")
    if not value.kind or not value.path or value.size_bytes is None:
        raise ReconstructionExperimentError(
            "experiment artifact reference is incomplete"
        )
    if value.size_bytes < 0 or not _SHA256_RE.fullmatch(value.sha256):
        raise ReconstructionExperimentError(
            "experiment artifact reference lacks strong hash/size"
        )
    return value


def _content_id(value: str, prefix: str) -> str:
    text = _required_text(value)
    expected = prefix + ":sha256:"
    if not text.startswith(expected) or not _SHA256_RE.fullmatch(
        text.removeprefix(expected)
    ):
        raise ReconstructionExperimentError(
            f"{prefix} identity is not content addressed"
        )
    return text


def _bind_id(
    instance: Any,
    field_name: str,
    prefix: str,
    payload: Mapping[str, JSONValue],
) -> None:
    expected = _stable_id(prefix, payload)
    supplied = str(getattr(instance, field_name)).strip()
    if supplied and supplied != expected:
        raise ReconstructionExperimentError(
            f"{field_name} differs from immutable content"
        )
    object.__setattr__(instance, field_name, expected)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        _canonical_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _canonical_json(value: Mapping[str, JSONValue]) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    if not isinstance(value, Mapping):
        raise TypeError("experiment JSON must be an object")
    return value


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        return value
    raise TypeError("expected a sequence")


def _text_tuple(value: Any) -> tuple[str, ...]:
    values = value if not isinstance(value, Mapping) else tuple(value)
    if not isinstance(values, Iterable) or isinstance(
        values, (str, bytes, bytearray)
    ):
        raise TypeError("expected a string sequence")
    result = tuple(sorted({_required_text(item) for item in values}))
    if len(result) > MAX_EXPERIMENT_ITEMS:
        raise ReconstructionExperimentError("experiment sequence exceeds limit")
    return result


def _bounded_text_tuple(value: Any) -> tuple[str, ...]:
    result = _text_tuple(value)
    if any(len(item) > MAX_EXPERIMENT_STRING for item in result):
        raise ReconstructionExperimentError("experiment text exceeds limit")
    return result


def _field_tuple(value: Any) -> tuple[str, ...]:
    return tuple(sorted({_identifier(item, "field") for item in value}))


def _text_mapping(value: Mapping[str, str], name: str) -> dict[str, str]:
    if len(value) > MAX_EXPERIMENT_ITEMS:
        raise ReconstructionExperimentError(f"{name} exceeds item limit")
    return dict(
        sorted(
            (
                _required_text(key),
                _required_text(item),
            )
            for key, item in value.items()
        )
    )


def _identifier(value: Any, name: str) -> str:
    text = str(value).strip().lower().replace("_", "-")
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ReconstructionExperimentError(f"invalid {name}")
    return text


def _json_field_name(value: Any) -> str:
    text = str(value).strip()
    if len(text) > 256 or not _JSON_FIELD_RE.fullmatch(text):
        raise ReconstructionExperimentError("invalid artifact identity field")
    return text


def _symbol(value: Any) -> str:
    text = "".join(
        character for character in str(value).upper() if character.isalnum()
    )
    if len(text) != 6 or not text.isalpha():
        raise ReconstructionExperimentError("invalid experiment FX symbol")
    return text


def _period(value: Any) -> str:
    text = str(value).strip()
    if not _PERIOD_RE.fullmatch(text) or not 1 <= int(text[4:]) <= 12:
        raise ReconstructionExperimentError("invalid experiment period")
    return text


def _required_text(value: Any) -> str:
    text = str(value).strip()
    if not text or len(text) > MAX_EXPERIMENT_STRING:
        raise ReconstructionExperimentError(
            "required experiment text is invalid"
        )
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return _required_text(text) if text else None


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    return value


def _optional_int(value: Any) -> int | None:
    return None if value is None else _strict_int(value, "optional int")


def _int64(value: Any, name: str) -> int:
    parsed = _strict_int(value, name)
    if not -(2**63) <= parsed <= 2**63 - 1:
        raise ReconstructionExperimentError(f"{name} is outside int64")
    return parsed


def _positive_int(value: Any, name: str) -> int:
    parsed = _strict_int(value, name)
    if parsed <= 0:
        raise ReconstructionExperimentError(f"{name} must be positive")
    return parsed


def _nonnegative_int(value: Any, name: str) -> int:
    parsed = _strict_int(value, name)
    if parsed < 0:
        raise ReconstructionExperimentError(f"{name} must be nonnegative")
    return parsed


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be bool")
    return value


def _sha256(value: Any) -> str:
    text = str(value).strip().lower()
    if not _SHA256_RE.fullmatch(text):
        raise ReconstructionExperimentError("invalid SHA-256 digest")
    return text


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, text: str) -> None:
    encoded = text.encode("utf-8") + b"\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, path)


def _id_digest(value: str) -> str:
    digest = value.rsplit(":", 1)[-1]
    return _sha256(digest)


def _unique_ids(values: Iterable[str], name: str) -> None:
    selected = tuple(values)
    if len(selected) != len(set(selected)):
        raise ReconstructionExperimentError(f"duplicate {name} identity")


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ReconstructionExperimentError(
            f"unsupported experiment schema {actual!r}; expected {expected!r}"
        )


def _require_derived(data: Mapping[str, Any], name: str, expected: Any) -> None:
    if data.get(name) != expected:
        raise ReconstructionExperimentError(
            f"derived experiment field {name} differs"
        )


__all__ = [
    "CURRENT_EXPERIMENT_ALIAS",
    "CURRENT_EXPERIMENT_DATASET_ID",
    "CURRENT_EXPERIMENT_PROVIDER_ID",
    "CURRENT_EXPERIMENT_SOURCE_FORMAT",
    "CURRENT_EXPERIMENT_TIMEFRAME",
    "RECONSTRUCTION_EXPERIMENT_ARTIFACT_KIND",
    "RECONSTRUCTION_EXPERIMENT_MANIFEST_SCHEMA_VERSION",
    "ReconstructionExperimentArtifactBindingV1",
    "ReconstructionExperimentError",
    "ReconstructionExperimentImplementationV1",
    "ReconstructionExperimentLeakageAuditV1",
    "ReconstructionExperimentManifestV1",
    "ReconstructionExperimentRole",
    "ReconstructionExperimentSelectionV1",
    "ReconstructionExperimentSplitPolicyV1",
    "ReconstructionExperimentSplitStrategy",
    "ReconstructionExperimentSplitUnitV1",
    "ReconstructionExperimentVerificationV1",
    "audit_reconstruction_experiment_splits",
    "build_legacy_histdata_catalog",
    "current_reconstruction_experiment_implementation",
    "discover_reconstruction_experiments",
    "freeze_histdata_reconstruction_experiment",
    "read_reconstruction_experiment",
    "verify_reconstruction_experiment",
    "write_reconstruction_experiment",
]
