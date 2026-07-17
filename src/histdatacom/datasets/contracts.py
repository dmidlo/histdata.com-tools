"""Strict provider-neutral dataset identity and lineage contracts.

The contracts in this module are deliberately metadata-only.  Tick payloads
remain in strong local artifacts, while catalog, resolution, cursor, and replay
objects stay bounded and safe to serialize through CLI and workflow surfaces.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any

from histdatacom.runtime_contracts import ArtifactRef, JSONValue

SOURCE_PROVIDER_SCHEMA_VERSION = "histdatacom.source-provider.v1"
PROVIDER_ADAPTER_SCHEMA_VERSION = "histdatacom.provider-adapter.v1"
OBSERVED_PARTITION_SCHEMA_VERSION = "histdatacom.observed-partition.v2"
DATASET_DESCRIPTOR_SCHEMA_VERSION = "histdatacom.dataset-descriptor.v1"
DATASET_PARENT_SCHEMA_VERSION = "histdatacom.dataset-parent.v1"
DATASET_VERSION_SCHEMA_VERSION = "histdatacom.dataset-version.v1"
DATASET_ALIAS_SCHEMA_VERSION = "histdatacom.dataset-alias.v1"
DATASET_QUERY_SCOPE_SCHEMA_VERSION = "histdatacom.dataset-query-scope.v1"
DATASET_RESOLUTION_SCHEMA_VERSION = "histdatacom.dataset-resolution.v1"
DATASET_CURSOR_SCHEMA_VERSION = "histdatacom.dataset-cursor.v1"
DATASET_VERIFICATION_SCHEMA_VERSION = "histdatacom.dataset-verification.v1"
DATASET_EVENT_LINEAGE_SCHEMA_VERSION = "histdatacom.dataset-event-lineage.v2"
PROVIDER_SOURCE_INVENTORY_SCHEMA_VERSION = (
    "histdatacom.provider-source-inventory.v2"
)

MAX_CATALOG_ITEMS = 4096
MAX_LINEAGE_PARENTS = 64
MAX_PARTITIONS_PER_VERSION = 4096
MAX_QUALIFICATION_EVIDENCE = 64

_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")
_SEMVER_RE = re.compile(
    r"^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)"
    r"(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$"
)
_PERIOD_RE = re.compile(r"^\d{6}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FORBIDDEN_SECRET_KEYS = frozenset(
    {
        "access_token",
        "api_key",
        "authorization",
        "client_secret",
        "credential",
        "credentials",
        "password",
        "refresh_token",
        "secret",
        "token",
    }
)


class DatasetFailureCode(str, Enum):
    """Stable fail-closed provider and catalog failure categories."""

    UNSUPPORTED_FORMAT = "unsupported_format"
    UNSUPPORTED_TIMEFRAME = "unsupported_timeframe"
    MALFORMED_QUOTE = "malformed_quote"
    AMBIGUOUS_CLOCK = "ambiguous_clock"
    INVALID_SYMBOL = "invalid_symbol"
    INVALID_PERIOD = "invalid_period"
    MISSING_HASH = "missing_hash"
    STALE_ALIAS = "stale_alias"
    AMBIGUOUS_REFERENCE = "ambiguous_reference"
    UNKNOWN_REFERENCE = "unknown_reference"
    UNSUPPORTED_LICENSING_POLICY = "unsupported_licensing_policy"
    INCONSISTENT_COVERAGE = "inconsistent_coverage"
    ARTIFACT_MISSING = "artifact_missing"
    ARTIFACT_HASH_MISMATCH = "artifact_hash_mismatch"
    ARTIFACT_SIZE_MISMATCH = "artifact_size_mismatch"
    UNQUALIFIED_DATASET = "unqualified_dataset"
    IDENTITY_MISMATCH = "identity_mismatch"
    CURSOR_SCOPE_MISMATCH = "cursor_scope_mismatch"
    SECRET_MATERIAL = "secret_material"
    UNSUPPORTED_ORIGIN = "unsupported_origin"


class DatasetContractError(ValueError):
    """A provider, manifest, resolution, or replay contract failed closed."""

    def __init__(self, code: DatasetFailureCode, message: str) -> None:
        self.code = code
        super().__init__(f"{code.value}: {message}")


class DatasetOrigin(str, Enum):
    """Whether a dataset contains evidence or a declared derivative."""

    OBSERVED = "observed"
    SYNTHETIC = "synthetic"
    DERIVED = "derived"
    COMPOSED = "composed"

    @classmethod
    def from_value(cls, value: str | "DatasetOrigin") -> "DatasetOrigin":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_ORIGIN,
                f"unsupported dataset origin {value!r}",
            ) from err


class DatasetQualificationStatus(str, Enum):
    """Qualification state bound into an immutable dataset version."""

    QUALIFIED = "qualified"
    UNQUALIFIED = "unqualified"
    RETIRED = "retired"

    @classmethod
    def from_value(
        cls, value: str | "DatasetQualificationStatus"
    ) -> "DatasetQualificationStatus":
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError(
                "unsupported dataset qualification status"
            ) from err


class DatasetLicensingPolicy(str, Enum):
    """Bounded redistribution policy; unknown policy is never executable."""

    PUBLIC = "public"
    ATTRIBUTION = "attribution"
    LOCAL_ONLY = "local-only"
    RESTRICTED = "restricted"
    UNKNOWN = "unknown"

    @classmethod
    def from_value(
        cls, value: str | "DatasetLicensingPolicy"
    ) -> "DatasetLicensingPolicy":
        if isinstance(value, cls):
            return value
        normalized = str(value).strip().lower().replace("_", "-")
        try:
            return cls(normalized)
        except ValueError as err:
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_LICENSING_POLICY,
                f"unsupported licensing policy {value!r}",
            ) from err


def canonical_contract_json(value: Mapping[str, JSONValue]) -> str:
    """Return the single deterministic JSON representation for identities."""
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    )


def normalize_provider_id(value: str) -> str:
    """Normalize a provider ID without conflating distinct organizations."""
    return _identifier(value, "source_provider_id")


def normalize_dataset_id(value: str) -> str:
    """Normalize a stable logical dataset name."""
    return _identifier(value, "dataset_id")


def normalize_dataset_alias(value: str) -> str:
    """Normalize a mutable dataset alias."""
    return _identifier(value, "dataset_alias")


def normalize_symbol(value: str) -> str:
    """Return a strict six-letter uppercase FX symbol."""
    normalized = "".join(
        character for character in str(value).upper() if character.isalnum()
    )
    if re.fullmatch(r"[A-Z]{6}", normalized) is None:
        raise DatasetContractError(
            DatasetFailureCode.INVALID_SYMBOL,
            "FX symbol must contain exactly six letters",
        )
    return normalized


def normalize_period(value: str) -> str:
    """Return a strict calendar month in YYYYMM form."""
    normalized = str(value).strip()
    if _PERIOD_RE.fullmatch(normalized) is None:
        raise DatasetContractError(
            DatasetFailureCode.INVALID_PERIOD, "period must use YYYYMM"
        )
    month = int(normalized[4:])
    if not 1 <= month <= 12:
        raise DatasetContractError(
            DatasetFailureCode.INVALID_PERIOD,
            "period month must be between 01 and 12",
        )
    return normalized


@dataclass(frozen=True, slots=True)
class SourceProviderDescriptorV1:
    """Normalized identity, attribution, and licensing for one provider."""

    source_provider_id: str
    display_name: str
    attribution: str
    licensing_policy: DatasetLicensingPolicy
    redistribution_allowed: bool
    descriptor_id: str = ""
    schema_version: str = SOURCE_PROVIDER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, SOURCE_PROVIDER_SCHEMA_VERSION)
        object.__setattr__(
            self,
            "source_provider_id",
            normalize_provider_id(self.source_provider_id),
        )
        object.__setattr__(
            self, "display_name", _required_text(self.display_name)
        )
        object.__setattr__(
            self, "attribution", _required_text(self.attribution)
        )
        policy = DatasetLicensingPolicy.from_value(self.licensing_policy)
        if policy is DatasetLicensingPolicy.UNKNOWN:
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_LICENSING_POLICY,
                "provider licensing policy cannot be unknown",
            )
        object.__setattr__(self, "licensing_policy", policy)
        if not isinstance(self.redistribution_allowed, bool):
            raise TypeError("redistribution_allowed must be bool")
        if self.redistribution_allowed and policy in {
            DatasetLicensingPolicy.LOCAL_ONLY,
            DatasetLicensingPolicy.RESTRICTED,
        }:
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_LICENSING_POLICY,
                "restricted/local-only provider cannot allow redistribution",
            )
        _bind_id(
            self, "descriptor_id", "source-provider", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_provider_id": self.source_provider_id,
            "display_name": self.display_name,
            "attribution": self.attribution,
            "licensing_policy": self.licensing_policy.value,
            "redistribution_allowed": self.redistribution_allowed,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "descriptor_id": self.descriptor_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SourceProviderDescriptorV1":
        return cls(
            source_provider_id=str(data.get("source_provider_id", "")),
            display_name=str(data.get("display_name", "")),
            attribution=str(data.get("attribution", "")),
            licensing_policy=DatasetLicensingPolicy.from_value(
                str(data.get("licensing_policy", ""))
            ),
            redistribution_allowed=_strict_bool(
                data.get("redistribution_allowed"), "redistribution_allowed"
            ),
            descriptor_id=str(data.get("descriptor_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProviderAdapterDescriptorV1:
    """Versioned transformation policy implemented by a provider adapter."""

    adapter_id: str
    adapter_version: str
    source_provider_id: str
    formats: tuple[str, ...]
    granularities: tuple[str, ...]
    clock_policy_id: str
    partition_policy_id: str
    row_identity_policy_id: str
    projection_schema_version: str
    descriptor_id: str = ""
    schema_version: str = PROVIDER_ADAPTER_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, PROVIDER_ADAPTER_SCHEMA_VERSION)
        object.__setattr__(
            self, "adapter_id", _identifier(self.adapter_id, "adapter_id")
        )
        version = str(self.adapter_version).strip()
        if _SEMVER_RE.fullmatch(version) is None:
            raise ValueError("adapter_version must use SemVer")
        object.__setattr__(self, "adapter_version", version)
        object.__setattr__(
            self,
            "source_provider_id",
            normalize_provider_id(self.source_provider_id),
        )
        formats = tuple(
            sorted({_identifier(value, "format") for value in self.formats})
        )
        granularities = tuple(
            sorted({_granularity(value) for value in self.granularities})
        )
        if not formats:
            raise ValueError("provider adapter requires at least one format")
        if not granularities:
            raise ValueError(
                "provider adapter requires at least one granularity"
            )
        object.__setattr__(self, "formats", formats)
        object.__setattr__(self, "granularities", granularities)
        for name in (
            "clock_policy_id",
            "partition_policy_id",
            "row_identity_policy_id",
            "projection_schema_version",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        _bind_id(
            self, "descriptor_id", "provider-adapter", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "source_provider_id": self.source_provider_id,
            "formats": list(self.formats),
            "granularities": list(self.granularities),
            "clock_policy_id": self.clock_policy_id,
            "partition_policy_id": self.partition_policy_id,
            "row_identity_policy_id": self.row_identity_policy_id,
            "projection_schema_version": self.projection_schema_version,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "descriptor_id": self.descriptor_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ProviderAdapterDescriptorV1":
        return cls(
            adapter_id=str(data.get("adapter_id", "")),
            adapter_version=str(data.get("adapter_version", "")),
            source_provider_id=str(data.get("source_provider_id", "")),
            formats=_string_tuple(data.get("formats")),
            granularities=_string_tuple(data.get("granularities")),
            clock_policy_id=str(data.get("clock_policy_id", "")),
            partition_policy_id=str(data.get("partition_policy_id", "")),
            row_identity_policy_id=str(data.get("row_identity_policy_id", "")),
            projection_schema_version=str(
                data.get("projection_schema_version", "")
            ),
            descriptor_id=str(data.get("descriptor_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class CanonicalObservedPartitionV2:
    """One immutable provider partition projected to canonical ASCII/T ticks."""

    source_provider_id: str
    adapter_id: str
    adapter_version: str
    symbol: str
    period: str
    artifact: ArtifactRef
    source_artifact_sha256: str
    row_count: int
    coverage_start_ns: int
    coverage_end_ns: int
    clock_policy_id: str
    partition_policy_id: str
    row_identity_policy_id: str
    licensing_policy: DatasetLicensingPolicy
    native_partition_id: str = ""
    series_id: str = ""
    partition_id: str = ""
    format: str = "ascii"
    granularity: str = "T"
    schema_version: str = OBSERVED_PARTITION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, OBSERVED_PARTITION_SCHEMA_VERSION)
        provider = normalize_provider_id(self.source_provider_id)
        adapter = _identifier(self.adapter_id, "adapter_id")
        version = str(self.adapter_version).strip()
        if _SEMVER_RE.fullmatch(version) is None:
            raise ValueError("adapter_version must use SemVer")
        symbol = normalize_symbol(self.symbol)
        period = normalize_period(self.period)
        data_format = _identifier(self.format, "format")
        if data_format != "ascii":
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_FORMAT,
                "canonical observed partitions require ASCII",
            )
        granularity = _granularity(self.granularity)
        if granularity != "T":
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_TIMEFRAME,
                "canonical observed partitions require tick granularity",
            )
        artifact = strong_artifact_ref(self.artifact)
        source_hash = _sha256(self.source_artifact_sha256)
        rows = _positive_int(self.row_count, "row_count")
        start = _int64(self.coverage_start_ns, "coverage_start_ns")
        end = _int64(self.coverage_end_ns, "coverage_end_ns")
        if end <= start:
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "partition coverage is empty",
            )
        policy = DatasetLicensingPolicy.from_value(self.licensing_policy)
        if policy is DatasetLicensingPolicy.UNKNOWN:
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_LICENSING_POLICY,
                "partition licensing policy cannot be unknown",
            )
        for name, value in (
            ("clock_policy_id", self.clock_policy_id),
            ("partition_policy_id", self.partition_policy_id),
            ("row_identity_policy_id", self.row_identity_policy_id),
        ):
            object.__setattr__(self, name, _required_text(value))
        native = str(self.native_partition_id).strip()
        series = self.series_id or f"ascii:T:{symbol}:{provider}"
        if _required_text(series) != series:
            raise ValueError("series_id contains surrounding whitespace")
        object.__setattr__(self, "source_provider_id", provider)
        object.__setattr__(self, "adapter_id", adapter)
        object.__setattr__(self, "adapter_version", version)
        object.__setattr__(self, "symbol", symbol)
        object.__setattr__(self, "period", period)
        object.__setattr__(self, "artifact", artifact)
        object.__setattr__(self, "source_artifact_sha256", source_hash)
        object.__setattr__(self, "row_count", rows)
        object.__setattr__(self, "coverage_start_ns", start)
        object.__setattr__(self, "coverage_end_ns", end)
        object.__setattr__(self, "licensing_policy", policy)
        object.__setattr__(self, "native_partition_id", native)
        object.__setattr__(self, "series_id", series)
        object.__setattr__(self, "format", data_format)
        object.__setattr__(self, "granularity", granularity)
        _bind_id(
            self, "partition_id", "observed-partition", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "source_provider_id": self.source_provider_id,
            "adapter_id": self.adapter_id,
            "adapter_version": self.adapter_version,
            "symbol": self.symbol,
            "period": self.period,
            "format": self.format,
            "granularity": self.granularity,
            "artifact_sha256": self.artifact.sha256,
            "artifact_size_bytes": self.artifact.size_bytes,
            "source_artifact_sha256": self.source_artifact_sha256,
            "row_count": self.row_count,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
            "clock_policy_id": self.clock_policy_id,
            "partition_policy_id": self.partition_policy_id,
            "row_identity_policy_id": self.row_identity_policy_id,
            "licensing_policy": self.licensing_policy.value,
            "native_partition_id": self.native_partition_id,
            "series_id": self.series_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "artifact": self.artifact.to_dict(),
            "partition_id": self.partition_id,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CanonicalObservedPartitionV2":
        return cls(
            source_provider_id=str(data.get("source_provider_id", "")),
            adapter_id=str(data.get("adapter_id", "")),
            adapter_version=str(data.get("adapter_version", "")),
            symbol=str(data.get("symbol", "")),
            period=str(data.get("period", "")),
            artifact=ArtifactRef.from_dict(_mapping(data.get("artifact"))),
            source_artifact_sha256=str(data.get("source_artifact_sha256", "")),
            row_count=_strict_int(data.get("row_count"), "row_count"),
            coverage_start_ns=_strict_int(
                data.get("coverage_start_ns"), "coverage_start_ns"
            ),
            coverage_end_ns=_strict_int(
                data.get("coverage_end_ns"), "coverage_end_ns"
            ),
            clock_policy_id=str(data.get("clock_policy_id", "")),
            partition_policy_id=str(data.get("partition_policy_id", "")),
            row_identity_policy_id=str(data.get("row_identity_policy_id", "")),
            licensing_policy=DatasetLicensingPolicy.from_value(
                str(data.get("licensing_policy", ""))
            ),
            native_partition_id=str(data.get("native_partition_id", "")),
            series_id=str(data.get("series_id", "")),
            partition_id=str(data.get("partition_id", "")),
            format=str(data.get("format", "")),
            granularity=str(data.get("granularity", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetDescriptorV1:
    """Stable user-selectable logical dataset identity."""

    dataset_id: str
    display_name: str
    description: str
    allowed_origins: tuple[DatasetOrigin, ...]
    descriptor_id: str = ""
    schema_version: str = DATASET_DESCRIPTOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, DATASET_DESCRIPTOR_SCHEMA_VERSION)
        object.__setattr__(
            self, "dataset_id", normalize_dataset_id(self.dataset_id)
        )
        object.__setattr__(
            self, "display_name", _required_text(self.display_name)
        )
        object.__setattr__(
            self, "description", _required_text(self.description)
        )
        origins = tuple(
            sorted(
                {
                    DatasetOrigin.from_value(value)
                    for value in self.allowed_origins
                },
                key=lambda item: item.value,
            )
        )
        if not origins:
            raise ValueError("dataset descriptor requires an allowed origin")
        object.__setattr__(self, "allowed_origins", origins)
        _bind_id(
            self, "descriptor_id", "dataset-descriptor", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "display_name": self.display_name,
            "description": self.description,
            "allowed_origins": [item.value for item in self.allowed_origins],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "descriptor_id": self.descriptor_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetDescriptorV1":
        return cls(
            dataset_id=str(data.get("dataset_id", "")),
            display_name=str(data.get("display_name", "")),
            description=str(data.get("description", "")),
            allowed_origins=tuple(
                DatasetOrigin.from_value(str(value))
                for value in _sequence(data.get("allowed_origins"))
            ),
            descriptor_id=str(data.get("descriptor_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetParentV1:
    """One exact parent version in derived/composed dataset lineage."""

    parent_dataset_version_id: str
    role: str
    ordinal: int
    lineage_id: str = ""
    schema_version: str = DATASET_PARENT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, DATASET_PARENT_SCHEMA_VERSION)
        object.__setattr__(
            self,
            "parent_dataset_version_id",
            _version_id(self.parent_dataset_version_id),
        )
        object.__setattr__(self, "role", _identifier(self.role, "parent role"))
        ordinal = _nonnegative_int(self.ordinal, "ordinal")
        object.__setattr__(self, "ordinal", ordinal)
        _bind_id(self, "lineage_id", "dataset-parent", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "parent_dataset_version_id": self.parent_dataset_version_id,
            "role": self.role,
            "ordinal": self.ordinal,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetParentV1":
        return cls(
            parent_dataset_version_id=str(
                data.get("parent_dataset_version_id", "")
            ),
            role=str(data.get("role", "")),
            ordinal=_strict_int(data.get("ordinal"), "ordinal"),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetVersionManifestV1:
    """Immutable content-bound observed, synthetic, or composed dataset."""

    dataset_id: str
    origin: DatasetOrigin
    normalization_policy_id: str
    qualification_status: DatasetQualificationStatus
    partitions: tuple[CanonicalObservedPartitionV2, ...] = ()
    parents: tuple[DatasetParentV1, ...] = ()
    qualification_evidence: tuple[ArtifactRef, ...] = ()
    delivery_profile_id: str | None = None
    dataset_version_id: str = ""
    manifest_sha256: str = ""
    schema_version: str = DATASET_VERSION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, DATASET_VERSION_SCHEMA_VERSION)
        dataset_id = normalize_dataset_id(self.dataset_id)
        origin = DatasetOrigin.from_value(self.origin)
        partitions = tuple(
            sorted(self.partitions, key=lambda item: (item.period, item.symbol))
        )
        parents = tuple(
            sorted(self.parents, key=lambda item: (item.ordinal, item.role))
        )
        evidence = tuple(
            sorted(
                (
                    strong_artifact_ref(item)
                    for item in self.qualification_evidence
                ),
                key=lambda item: (item.kind, item.sha256, item.path),
            )
        )
        if len(partitions) > MAX_PARTITIONS_PER_VERSION:
            raise ValueError("dataset partition count exceeds catalog limit")
        if len(parents) > MAX_LINEAGE_PARENTS:
            raise ValueError("dataset parent count exceeds catalog limit")
        if len(evidence) > MAX_QUALIFICATION_EVIDENCE:
            raise ValueError(
                "qualification evidence count exceeds catalog limit"
            )
        if len({item.partition_id for item in partitions}) != len(partitions):
            raise ValueError("dataset contains duplicate partitions")
        if len({(item.symbol, item.period) for item in partitions}) != len(
            partitions
        ):
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "dataset contains duplicate symbol/period coverage",
            )
        if len({item.ordinal for item in parents}) != len(parents):
            raise ValueError("dataset parent ordinals must be unique")
        status = DatasetQualificationStatus.from_value(
            self.qualification_status
        )
        if status is DatasetQualificationStatus.QUALIFIED and not evidence:
            raise DatasetContractError(
                DatasetFailureCode.UNQUALIFIED_DATASET,
                "qualified dataset version requires strong evidence",
            )
        if origin is DatasetOrigin.OBSERVED:
            if not partitions or parents:
                raise DatasetContractError(
                    DatasetFailureCode.INCONSISTENT_COVERAGE,
                    "observed dataset requires partitions and no parents",
                )
        else:
            if partitions:
                raise DatasetContractError(
                    DatasetFailureCode.UNSUPPORTED_ORIGIN,
                    "derived datasets cannot masquerade as observed partitions",
                )
            if not parents:
                raise DatasetContractError(
                    DatasetFailureCode.INCONSISTENT_COVERAGE,
                    "derived dataset requires exact parent versions",
                )
            if origin is DatasetOrigin.COMPOSED and len(parents) < 2:
                raise DatasetContractError(
                    DatasetFailureCode.INCONSISTENT_COVERAGE,
                    "composed dataset requires at least two parents",
                )
        delivery = _optional_text(self.delivery_profile_id)
        if origin is DatasetOrigin.OBSERVED and delivery is not None:
            # Delivery is permitted but remains orthogonal and explicit; it is
            # never used to derive provider identity.
            delivery = _identifier(delivery, "delivery_profile_id")
        elif delivery is not None:
            delivery = _identifier(delivery, "delivery_profile_id")
        object.__setattr__(self, "dataset_id", dataset_id)
        object.__setattr__(self, "origin", origin)
        object.__setattr__(
            self,
            "normalization_policy_id",
            _required_text(self.normalization_policy_id),
        )
        object.__setattr__(self, "qualification_status", status)
        object.__setattr__(self, "partitions", partitions)
        object.__setattr__(self, "parents", parents)
        object.__setattr__(self, "qualification_evidence", evidence)
        object.__setattr__(self, "delivery_profile_id", delivery)
        payload = self.identity_payload()
        digest = hashlib.sha256(
            canonical_contract_json(payload).encode()
        ).hexdigest()
        expected_version = f"dataset-version:sha256:{digest}"
        supplied_version = str(self.dataset_version_id).strip()
        if supplied_version and supplied_version != expected_version:
            raise DatasetContractError(
                DatasetFailureCode.IDENTITY_MISMATCH,
                "dataset_version_id differs from immutable manifest",
            )
        supplied_hash = str(self.manifest_sha256).strip().lower()
        if supplied_hash and supplied_hash != digest:
            raise DatasetContractError(
                DatasetFailureCode.IDENTITY_MISMATCH,
                "manifest_sha256 differs from immutable manifest",
            )
        object.__setattr__(self, "dataset_version_id", expected_version)
        object.__setattr__(self, "manifest_sha256", digest)

    @property
    def source_provider_ids(self) -> tuple[str, ...]:
        """Return observed providers without treating synthetic as a provider."""
        return tuple(
            sorted({item.source_provider_id for item in self.partitions})
        )

    @property
    def row_count(self) -> int:
        return sum(item.row_count for item in self.partitions)

    @property
    def coverage_start_ns(self) -> int | None:
        return min(
            (item.coverage_start_ns for item in self.partitions), default=None
        )

    @property
    def coverage_end_ns(self) -> int | None:
        return max(
            (item.coverage_end_ns for item in self.partitions), default=None
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "origin": self.origin.value,
            "normalization_policy_id": self.normalization_policy_id,
            "qualification_status": self.qualification_status.value,
            "partitions": [
                {**item.identity_payload(), "partition_id": item.partition_id}
                for item in self.partitions
            ],
            "parents": [item.to_dict() for item in self.parents],
            "qualification_evidence": [
                _artifact_identity_payload(item)
                for item in self.qualification_evidence
            ],
            "delivery_profile_id": self.delivery_profile_id,
            "source_provider_ids": list(self.source_provider_ids),
            "row_count": self.row_count,
            "coverage_start_ns": self.coverage_start_ns,
            "coverage_end_ns": self.coverage_end_ns,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "partitions": [item.to_dict() for item in self.partitions],
            "qualification_evidence": [
                item.to_dict() for item in self.qualification_evidence
            ],
            "dataset_version_id": self.dataset_version_id,
            "manifest_sha256": self.manifest_sha256,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetVersionManifestV1":
        return cls(
            dataset_id=str(data.get("dataset_id", "")),
            origin=DatasetOrigin.from_value(str(data.get("origin", ""))),
            normalization_policy_id=str(
                data.get("normalization_policy_id", "")
            ),
            qualification_status=DatasetQualificationStatus.from_value(
                str(data.get("qualification_status", ""))
            ),
            partitions=tuple(
                CanonicalObservedPartitionV2.from_dict(_mapping(value))
                for value in _sequence(data.get("partitions"))
            ),
            parents=tuple(
                DatasetParentV1.from_dict(_mapping(value))
                for value in _sequence(data.get("parents"))
            ),
            qualification_evidence=tuple(
                ArtifactRef.from_dict(_mapping(value))
                for value in _sequence(data.get("qualification_evidence"))
            ),
            delivery_profile_id=_optional_text(data.get("delivery_profile_id")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            manifest_sha256=str(data.get("manifest_sha256", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetAliasV1:
    """One mutable name that resolves once to an immutable version."""

    alias: str
    dataset_id: str
    dataset_version_id: str
    revision: int
    require_qualified: bool = True
    alias_id: str = ""
    schema_version: str = DATASET_ALIAS_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, DATASET_ALIAS_SCHEMA_VERSION)
        object.__setattr__(self, "alias", normalize_dataset_alias(self.alias))
        object.__setattr__(
            self, "dataset_id", normalize_dataset_id(self.dataset_id)
        )
        object.__setattr__(
            self, "dataset_version_id", _version_id(self.dataset_version_id)
        )
        object.__setattr__(
            self, "revision", _positive_int(self.revision, "revision")
        )
        if not isinstance(self.require_qualified, bool):
            raise TypeError("require_qualified must be bool")
        _bind_id(self, "alias_id", "dataset-alias", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "alias": self.alias,
            "dataset_id": self.dataset_id,
            "dataset_version_id": self.dataset_version_id,
            "revision": self.revision,
            "require_qualified": self.require_qualified,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "alias_id": self.alias_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetAliasV1":
        return cls(
            alias=str(data.get("alias", "")),
            dataset_id=str(data.get("dataset_id", "")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            revision=_strict_int(data.get("revision"), "revision"),
            require_qualified=_strict_bool(
                data.get("require_qualified"), "require_qualified"
            ),
            alias_id=str(data.get("alias_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetQueryScopeV1:
    """Dataset-independent query identity bound into receipts and cursors."""

    symbols: tuple[str, ...] = ()
    periods: tuple[str, ...] = ()
    origin: DatasetOrigin | None = None
    ensemble_member_id: str | None = None
    scope_id: str = ""
    schema_version: str = DATASET_QUERY_SCOPE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, DATASET_QUERY_SCOPE_SCHEMA_VERSION)
        symbols = tuple(
            sorted({normalize_symbol(value) for value in self.symbols})
        )
        periods = tuple(
            sorted({normalize_period(value) for value in self.periods})
        )
        origin = (
            None
            if self.origin is None
            else DatasetOrigin.from_value(self.origin)
        )
        ensemble = _optional_text(self.ensemble_member_id)
        object.__setattr__(self, "symbols", symbols)
        object.__setattr__(self, "periods", periods)
        object.__setattr__(self, "origin", origin)
        object.__setattr__(self, "ensemble_member_id", ensemble)
        _bind_id(
            self, "scope_id", "dataset-query-scope", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "symbols": list(self.symbols),
            "periods": list(self.periods),
            "origin": None if self.origin is None else self.origin.value,
            "ensemble_member_id": self.ensemble_member_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "scope_id": self.scope_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetQueryScopeV1":
        raw_origin = data.get("origin")
        return cls(
            symbols=_string_tuple(data.get("symbols")),
            periods=_string_tuple(data.get("periods")),
            origin=(
                None
                if raw_origin in (None, "")
                else DatasetOrigin.from_value(str(raw_origin))
            ),
            ensemble_member_id=_optional_text(data.get("ensemble_member_id")),
            scope_id=str(data.get("scope_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetResolutionV1:
    """Immutable result of resolving a dataset version or mutable alias."""

    reference: str
    dataset_id: str
    dataset_version_id: str
    manifest_sha256: str
    query_scope: DatasetQueryScopeV1
    alias: str | None = None
    alias_revision: int | None = None
    alias_id: str | None = None
    resolution_id: str = ""
    schema_version: str = DATASET_RESOLUTION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, DATASET_RESOLUTION_SCHEMA_VERSION)
        object.__setattr__(self, "reference", _required_text(self.reference))
        object.__setattr__(
            self, "dataset_id", normalize_dataset_id(self.dataset_id)
        )
        object.__setattr__(
            self, "dataset_version_id", _version_id(self.dataset_version_id)
        )
        object.__setattr__(
            self, "manifest_sha256", _sha256(self.manifest_sha256)
        )
        if not isinstance(self.query_scope, DatasetQueryScopeV1):
            raise TypeError("query_scope must be DatasetQueryScopeV1")
        alias = (
            None if self.alias is None else normalize_dataset_alias(self.alias)
        )
        if alias is None:
            if self.alias_revision is not None or self.alias_id is not None:
                raise ValueError(
                    "non-alias resolution cannot carry alias identity"
                )
        else:
            if self.alias_revision is None or self.alias_id is None:
                raise ValueError(
                    "alias resolution requires revision and alias_id"
                )
            object.__setattr__(
                self,
                "alias_revision",
                _positive_int(self.alias_revision, "alias_revision"),
            )
            object.__setattr__(self, "alias_id", _required_text(self.alias_id))
        object.__setattr__(self, "alias", alias)
        _bind_id(
            self, "resolution_id", "dataset-resolution", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "reference": self.reference,
            "dataset_id": self.dataset_id,
            "dataset_version_id": self.dataset_version_id,
            "manifest_sha256": self.manifest_sha256,
            "query_scope": self.query_scope.to_dict(),
            "alias": self.alias,
            "alias_revision": self.alias_revision,
            "alias_id": self.alias_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "resolution_id": self.resolution_id}

    def to_json(self) -> str:
        return canonical_contract_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetResolutionV1":
        return cls(
            reference=str(data.get("reference", "")),
            dataset_id=str(data.get("dataset_id", "")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            manifest_sha256=str(data.get("manifest_sha256", "")),
            query_scope=DatasetQueryScopeV1.from_dict(
                _mapping(data.get("query_scope"))
            ),
            alias=_optional_text(data.get("alias")),
            alias_revision=_optional_int(data.get("alias_revision")),
            alias_id=_optional_text(data.get("alias_id")),
            resolution_id=str(data.get("resolution_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "DatasetResolutionV1":
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class DatasetCursorV1:
    """Pagination identity that cannot cross version or query boundaries."""

    resolution_id: str
    dataset_version_id: str
    query_scope_id: str
    origin: DatasetOrigin
    series_id: str | None = None
    period: str | None = None
    row_id: int | None = None
    ensemble_member_id: str | None = None
    event_id: str | None = None
    cursor_id: str = ""
    schema_version: str = DATASET_CURSOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(self.schema_version, DATASET_CURSOR_SCHEMA_VERSION)
        object.__setattr__(
            self, "resolution_id", _required_text(self.resolution_id)
        )
        object.__setattr__(
            self, "dataset_version_id", _version_id(self.dataset_version_id)
        )
        object.__setattr__(
            self, "query_scope_id", _required_text(self.query_scope_id)
        )
        origin = DatasetOrigin.from_value(self.origin)
        object.__setattr__(self, "origin", origin)
        if origin is DatasetOrigin.OBSERVED:
            if (
                self.series_id is None
                or self.period is None
                or self.row_id is None
            ):
                raise ValueError(
                    "observed cursor requires series, period, and row_id"
                )
            object.__setattr__(
                self, "series_id", _required_text(self.series_id)
            )
            object.__setattr__(self, "period", normalize_period(self.period))
            object.__setattr__(
                self, "row_id", _positive_int(self.row_id, "row_id")
            )
            if self.ensemble_member_id is not None or self.event_id is not None:
                raise ValueError(
                    "observed cursor cannot carry synthetic identity"
                )
        else:
            if self.ensemble_member_id is None or self.event_id is None:
                raise ValueError(
                    "derived cursor requires ensemble member and event"
                )
            object.__setattr__(
                self,
                "ensemble_member_id",
                _required_text(self.ensemble_member_id),
            )
            object.__setattr__(self, "event_id", _required_text(self.event_id))
            if (
                self.series_id is not None
                or self.period is not None
                or self.row_id is not None
            ):
                raise ValueError(
                    "derived cursor cannot carry observed row identity"
                )
        _bind_id(self, "cursor_id", "dataset-cursor", self.identity_payload())

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "resolution_id": self.resolution_id,
            "dataset_version_id": self.dataset_version_id,
            "query_scope_id": self.query_scope_id,
            "origin": self.origin.value,
            "series_id": self.series_id,
            "period": self.period,
            "row_id": self.row_id,
            "ensemble_member_id": self.ensemble_member_id,
            "event_id": self.event_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "cursor_id": self.cursor_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetCursorV1":
        return cls(
            resolution_id=str(data.get("resolution_id", "")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            query_scope_id=str(data.get("query_scope_id", "")),
            origin=DatasetOrigin.from_value(str(data.get("origin", ""))),
            series_id=_optional_text(data.get("series_id")),
            period=_optional_text(data.get("period")),
            row_id=_optional_int(data.get("row_id")),
            ensemble_member_id=_optional_text(data.get("ensemble_member_id")),
            event_id=_optional_text(data.get("event_id")),
            cursor_id=str(data.get("cursor_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetVerificationV1:
    """Deterministic bounded evidence that a version's artifacts reconcile."""

    dataset_version_id: str
    manifest_sha256: str
    partition_count: int
    evidence_count: int
    verified_artifact_sha256: tuple[str, ...]
    verification_id: str = ""
    schema_version: str = DATASET_VERIFICATION_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, DATASET_VERIFICATION_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "dataset_version_id", _version_id(self.dataset_version_id)
        )
        object.__setattr__(
            self, "manifest_sha256", _sha256(self.manifest_sha256)
        )
        object.__setattr__(
            self,
            "partition_count",
            _nonnegative_int(self.partition_count, "partition_count"),
        )
        object.__setattr__(
            self,
            "evidence_count",
            _nonnegative_int(self.evidence_count, "evidence_count"),
        )
        hashes = tuple(
            sorted(_sha256(value) for value in self.verified_artifact_sha256)
        )
        if len(hashes) != self.partition_count + self.evidence_count:
            raise ValueError("verification artifact counts do not reconcile")
        object.__setattr__(self, "verified_artifact_sha256", hashes)
        _bind_id(
            self,
            "verification_id",
            "dataset-verification",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_version_id": self.dataset_version_id,
            "manifest_sha256": self.manifest_sha256,
            "partition_count": self.partition_count,
            "evidence_count": self.evidence_count,
            "verified_artifact_sha256": list(self.verified_artifact_sha256),
            "status": "verified",
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "verification_id": self.verification_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetVerificationV1":
        return cls(
            dataset_version_id=str(data.get("dataset_version_id", "")),
            manifest_sha256=str(data.get("manifest_sha256", "")),
            partition_count=_strict_int(
                data.get("partition_count"), "partition_count"
            ),
            evidence_count=_strict_int(
                data.get("evidence_count"), "evidence_count"
            ),
            verified_artifact_sha256=_string_tuple(
                data.get("verified_artifact_sha256")
            ),
            verification_id=str(data.get("verification_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class DatasetEventLineageV2:
    """Companion lineage for observed or synthetic rows without changing V1."""

    dataset_id: str
    dataset_version_id: str
    origin: DatasetOrigin
    source_provider_id: str | None = None
    parent_dataset_version_ids: tuple[str, ...] = ()
    delivery_profile_id: str | None = None
    source_series_id: str | None = None
    source_period: str | None = None
    source_row_id: int | None = None
    ensemble_member_id: str | None = None
    event_id: str | None = None
    anchor_interval_id: str | None = None
    generator_id: str | None = None
    lineage_id: str = ""
    schema_version: str = DATASET_EVENT_LINEAGE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, DATASET_EVENT_LINEAGE_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "dataset_id", normalize_dataset_id(self.dataset_id)
        )
        object.__setattr__(
            self, "dataset_version_id", _version_id(self.dataset_version_id)
        )
        origin = DatasetOrigin.from_value(self.origin)
        object.__setattr__(self, "origin", origin)
        parents = tuple(
            sorted(
                {
                    _version_id(value)
                    for value in self.parent_dataset_version_ids
                }
            )
        )
        object.__setattr__(self, "parent_dataset_version_ids", parents)
        delivery = _optional_text(self.delivery_profile_id)
        if delivery is not None:
            delivery = _identifier(delivery, "delivery_profile_id")
        object.__setattr__(self, "delivery_profile_id", delivery)
        if origin is DatasetOrigin.OBSERVED:
            if self.source_provider_id is None:
                raise ValueError("observed lineage requires source_provider_id")
            object.__setattr__(
                self,
                "source_provider_id",
                normalize_provider_id(self.source_provider_id),
            )
            if parents:
                raise ValueError(
                    "observed lineage cannot claim parent datasets"
                )
            if (
                self.source_series_id is None
                or self.source_period is None
                or self.source_row_id is None
            ):
                raise ValueError(
                    "observed lineage requires exact source row identity"
                )
            object.__setattr__(
                self, "source_series_id", _required_text(self.source_series_id)
            )
            object.__setattr__(
                self, "source_period", normalize_period(self.source_period)
            )
            object.__setattr__(
                self,
                "source_row_id",
                _positive_int(self.source_row_id, "source_row_id"),
            )
            if any(
                value is not None
                for value in (
                    self.ensemble_member_id,
                    self.event_id,
                    self.anchor_interval_id,
                    self.generator_id,
                )
            ):
                raise ValueError(
                    "observed lineage cannot carry synthetic identity"
                )
        else:
            if self.source_provider_id is not None:
                raise ValueError(
                    "synthetic/derived lineage cannot claim a source provider"
                )
            if not parents:
                raise ValueError(
                    "synthetic/derived lineage requires exact parents"
                )
            for name in (
                "ensemble_member_id",
                "event_id",
                "anchor_interval_id",
                "generator_id",
            ):
                object.__setattr__(
                    self, name, _required_text(getattr(self, name))
                )
            if any(
                value is not None
                for value in (
                    self.source_series_id,
                    self.source_period,
                    self.source_row_id,
                )
            ):
                raise ValueError(
                    "synthetic/derived lineage cannot claim observed row "
                    "identity"
                )
        _bind_id(
            self, "lineage_id", "dataset-event-lineage", self.identity_payload()
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "dataset_version_id": self.dataset_version_id,
            "origin": self.origin.value,
            "source_provider_id": self.source_provider_id,
            "parent_dataset_version_ids": list(self.parent_dataset_version_ids),
            "delivery_profile_id": self.delivery_profile_id,
            "source_series_id": self.source_series_id,
            "source_period": self.source_period,
            "source_row_id": self.source_row_id,
            "ensemble_member_id": self.ensemble_member_id,
            "event_id": self.event_id,
            "anchor_interval_id": self.anchor_interval_id,
            "generator_id": self.generator_id,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "lineage_id": self.lineage_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetEventLineageV2":
        return cls(
            dataset_id=str(data.get("dataset_id", "")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            origin=DatasetOrigin.from_value(str(data.get("origin", ""))),
            source_provider_id=_optional_text(data.get("source_provider_id")),
            parent_dataset_version_ids=_string_tuple(
                data.get("parent_dataset_version_ids")
            ),
            delivery_profile_id=_optional_text(data.get("delivery_profile_id")),
            source_series_id=_optional_text(data.get("source_series_id")),
            source_period=_optional_text(data.get("source_period")),
            source_row_id=_optional_int(data.get("source_row_id")),
            ensemble_member_id=_optional_text(data.get("ensemble_member_id")),
            event_id=_optional_text(data.get("event_id")),
            anchor_interval_id=_optional_text(data.get("anchor_interval_id")),
            generator_id=_optional_text(data.get("generator_id")),
            lineage_id=str(data.get("lineage_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ProviderSourceInventoryV2:
    """Provider-neutral, dataset-bound reconstruction source inventory."""

    dataset_id: str
    dataset_version_id: str
    manifest_sha256: str
    requested_start_ns: int
    requested_end_ns: int
    partitions: tuple[CanonicalObservedPartitionV2, ...]
    inventory_id: str = ""
    schema_version: str = PROVIDER_SOURCE_INVENTORY_SCHEMA_VERSION

    def __post_init__(self) -> None:
        _require_schema(
            self.schema_version, PROVIDER_SOURCE_INVENTORY_SCHEMA_VERSION
        )
        object.__setattr__(
            self, "dataset_id", normalize_dataset_id(self.dataset_id)
        )
        object.__setattr__(
            self, "dataset_version_id", _version_id(self.dataset_version_id)
        )
        object.__setattr__(
            self, "manifest_sha256", _sha256(self.manifest_sha256)
        )
        start = _int64(self.requested_start_ns, "requested_start_ns")
        end = _int64(self.requested_end_ns, "requested_end_ns")
        if end <= start:
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "requested source interval is empty",
            )
        partitions = tuple(
            sorted(self.partitions, key=lambda item: (item.period, item.symbol))
        )
        if not partitions:
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "provider-neutral inventory is empty",
            )
        if any(
            item.coverage_end_ns <= start or item.coverage_start_ns >= end
            for item in partitions
        ):
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "inventory contains a partition outside the requested interval",
            )
        if not _series_cover_interval(partitions, start=start, end=end):
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "inventory does not continuously cover the requested interval",
            )
        object.__setattr__(self, "requested_start_ns", start)
        object.__setattr__(self, "requested_end_ns", end)
        object.__setattr__(self, "partitions", partitions)
        _bind_id(
            self,
            "inventory_id",
            "provider-source-inventory",
            self.identity_payload(),
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "dataset_id": self.dataset_id,
            "dataset_version_id": self.dataset_version_id,
            "manifest_sha256": self.manifest_sha256,
            "requested_start_ns": self.requested_start_ns,
            "requested_end_ns": self.requested_end_ns,
            "partitions": [
                {**item.identity_payload(), "partition_id": item.partition_id}
                for item in self.partitions
            ],
            "source_provider_ids": [
                item
                for item in sorted(
                    {
                        partition.source_provider_id
                        for partition in self.partitions
                    }
                )
            ],
            "adapter_ids": [
                item
                for item in sorted(
                    {partition.adapter_id for partition in self.partitions}
                )
            ],
            "row_count": sum(item.row_count for item in self.partitions),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "partitions": [item.to_dict() for item in self.partitions],
            "inventory_id": self.inventory_id,
        }

    def to_json(self) -> str:
        return canonical_contract_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ProviderSourceInventoryV2":
        return cls(
            dataset_id=str(data.get("dataset_id", "")),
            dataset_version_id=str(data.get("dataset_version_id", "")),
            manifest_sha256=str(data.get("manifest_sha256", "")),
            requested_start_ns=_strict_int(
                data.get("requested_start_ns"), "requested_start_ns"
            ),
            requested_end_ns=_strict_int(
                data.get("requested_end_ns"), "requested_end_ns"
            ),
            partitions=tuple(
                CanonicalObservedPartitionV2.from_dict(_mapping(value))
                for value in _sequence(data.get("partitions"))
            ),
            inventory_id=str(data.get("inventory_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ProviderSourceInventoryV2":
        return cls.from_dict(_json_mapping(text))


def strong_artifact_ref(value: ArtifactRef) -> ArtifactRef:
    """Normalize a local strong reference and reject embedded secret material."""
    if not isinstance(value, ArtifactRef):
        raise TypeError("artifact must be ArtifactRef")
    kind = _required_text(value.kind)
    path = str(Path(_required_text(value.path)).expanduser().resolve())
    if value.size_bytes is None:
        raise ValueError("artifact requires size_bytes")
    size = _nonnegative_int(value.size_bytes, "artifact.size_bytes")
    digest = _sha256(value.sha256)
    metadata = _json_mapping_without_secrets(value.metadata)
    return ArtifactRef(
        kind=kind,
        path=path,
        size_bytes=size,
        sha256=digest,
        metadata=metadata,
    )


def _artifact_identity_payload(value: ArtifactRef) -> dict[str, JSONValue]:
    """Bind strong artifact content and provenance without its local path."""
    artifact = strong_artifact_ref(value)
    return {
        "kind": artifact.kind,
        "size_bytes": artifact.size_bytes,
        "sha256": artifact.sha256,
        "metadata": dict(artifact.metadata),
    }


def _series_cover_interval(
    partitions: tuple[CanonicalObservedPartitionV2, ...],
    *,
    start: int,
    end: int,
) -> bool:
    """Return whether every selected series covers the complete interval."""
    by_series: dict[str, list[tuple[int, int]]] = {}
    for partition in partitions:
        by_series.setdefault(partition.series_id, []).append(
            (partition.coverage_start_ns, partition.coverage_end_ns)
        )
    for intervals in by_series.values():
        covered_until = start
        for interval_start, interval_end in sorted(intervals):
            if interval_end <= covered_until:
                continue
            if interval_start > covered_until:
                return False
            covered_until = interval_end
            if covered_until >= end:
                break
        if covered_until < end:
            return False
    return bool(by_series)


def _json_mapping_without_secrets(
    value: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    result: dict[str, JSONValue] = {}
    for raw_key, raw_value in value.items():
        key = str(raw_key)
        normalized = key.strip().lower().replace("-", "_")
        if normalized in _FORBIDDEN_SECRET_KEYS:
            raise DatasetContractError(
                DatasetFailureCode.SECRET_MATERIAL,
                f"secret-like metadata key is forbidden: {key}",
            )
        result[key] = _clean_json_value(raw_value, path=key)
    return result


def _clean_json_value(value: JSONValue, *, path: str) -> JSONValue:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"non-finite catalog metadata at {path}")
        return value
    if isinstance(value, list):
        return [
            _clean_json_value(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, dict):
        return _json_mapping_without_secrets(value)
    raise TypeError(f"non-JSON catalog metadata at {path}")


def _bind_id(
    instance: Any,
    field_name: str,
    prefix: str,
    payload: Mapping[str, JSONValue],
) -> None:
    expected = _stable_id(prefix, payload)
    supplied = str(getattr(instance, field_name)).strip()
    if supplied and supplied != expected:
        raise DatasetContractError(
            DatasetFailureCode.IDENTITY_MISMATCH,
            f"{field_name} differs from immutable content",
        )
    object.__setattr__(instance, field_name, expected)


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _version_id(value: str) -> str:
    normalized = str(value).strip().lower()
    if not normalized.startswith("dataset-version:sha256:"):
        raise DatasetContractError(
            DatasetFailureCode.IDENTITY_MISMATCH,
            "dataset version must be content-addressed",
        )
    _sha256(normalized.removeprefix("dataset-version:sha256:"))
    return normalized


def _sha256(value: str) -> str:
    normalized = str(value).strip().lower().removeprefix("sha256:")
    if _SHA256_RE.fullmatch(normalized) is None:
        raise DatasetContractError(
            DatasetFailureCode.MISSING_HASH,
            "a lowercase SHA-256 digest is required",
        )
    return normalized


def _identifier(value: str, name: str) -> str:
    normalized = re.sub(
        r"[-_.]+",
        lambda match: match.group(0)[0],
        str(value).strip().lower().replace(" ", "-"),
    )
    if _IDENTIFIER_RE.fullmatch(normalized) is None:
        raise ValueError(
            f"{name} must use normalized lowercase identifier syntax"
        )
    return normalized


def _granularity(value: str) -> str:
    normalized = str(value).strip().upper()
    if normalized != "T":
        raise DatasetContractError(
            DatasetFailureCode.UNSUPPORTED_TIMEFRAME,
            "only canonical ASCII/T tick data is supported",
        )
    return normalized


def _require_schema(actual: str, expected: str) -> None:
    if actual != expected:
        raise ValueError(f"unsupported schema version: {actual!r}")


def _required_text(value: Any) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise ValueError("value must be non-empty text")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _strict_bool(value: Any, name: str) -> bool:
    if not isinstance(value, bool):
        raise TypeError(f"{name} must be bool")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be int")
    return value


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return _strict_int(value, "optional integer")


def _nonnegative_int(value: Any, name: str) -> int:
    parsed = _strict_int(value, name)
    if parsed < 0:
        raise ValueError(f"{name} must be non-negative")
    return parsed


def _positive_int(value: Any, name: str) -> int:
    parsed = _strict_int(value, name)
    if parsed < 1:
        raise ValueError(f"{name} must be positive")
    return parsed


def _int64(value: Any, name: str) -> int:
    parsed = _strict_int(value, name)
    if not -(2**63) <= parsed <= 2**63 - 1:
        raise ValueError(f"{name} is outside int64")
    return parsed


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("value must be a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if isinstance(value, (str, bytes)) or not isinstance(value, Sequence):
        raise TypeError("value must be a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as err:
        raise ValueError("invalid JSON") from err
    return _mapping(value)


__all__ = [
    "CanonicalObservedPartitionV2",
    "DatasetAliasV1",
    "DatasetContractError",
    "DatasetCursorV1",
    "DatasetDescriptorV1",
    "DatasetEventLineageV2",
    "DatasetFailureCode",
    "DatasetLicensingPolicy",
    "DatasetOrigin",
    "DatasetParentV1",
    "DatasetQualificationStatus",
    "DatasetQueryScopeV1",
    "DatasetResolutionV1",
    "DatasetVerificationV1",
    "DatasetVersionManifestV1",
    "ProviderAdapterDescriptorV1",
    "ProviderSourceInventoryV2",
    "SourceProviderDescriptorV1",
    "canonical_contract_json",
    "normalize_dataset_alias",
    "normalize_dataset_id",
    "normalize_period",
    "normalize_provider_id",
    "normalize_symbol",
    "strong_artifact_ref",
]
