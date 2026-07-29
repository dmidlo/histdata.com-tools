"""Versioned local dataset catalog, alias resolver, replay, and verification."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from histdatacom.datasets.contracts import (
    MAX_CATALOG_ITEMS,
    DatasetAliasV1,
    DatasetContractError,
    DatasetCursorV1,
    DatasetDescriptorV1,
    DatasetFailureCode,
    DatasetOrigin,
    DatasetQualificationStatus,
    DatasetQueryScopeV1,
    DatasetResolutionV1,
    DatasetVerificationV1,
    DatasetVersionManifestV1,
    ProviderAdapterDescriptorV1,
    ProviderSourceInventoryV2,
    SourceProviderDescriptorV1,
    canonical_contract_json,
    normalize_dataset_alias,
    normalize_dataset_id,
    normalize_period,
    normalize_symbol,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue

DATASET_CATALOG_SCHEMA_VERSION = "histdatacom.dataset-catalog.v1"
MAX_DATASET_CATALOG_BYTES = 16 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class DatasetCatalog:
    """Bounded typed catalog for immutable versions and mutable aliases."""

    providers: tuple[SourceProviderDescriptorV1, ...]
    adapters: tuple[ProviderAdapterDescriptorV1, ...]
    datasets: tuple[DatasetDescriptorV1, ...]
    versions: tuple[DatasetVersionManifestV1, ...]
    aliases: tuple[DatasetAliasV1, ...] = ()
    catalog_id: str = ""
    schema_version: str = DATASET_CATALOG_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != DATASET_CATALOG_SCHEMA_VERSION:
            raise ValueError("unsupported dataset catalog schema")
        providers = tuple(
            sorted(self.providers, key=lambda item: item.source_provider_id)
        )
        adapters = tuple(
            sorted(self.adapters, key=lambda item: item.adapter_id)
        )
        datasets = tuple(
            sorted(self.datasets, key=lambda item: item.dataset_id)
        )
        versions = tuple(
            sorted(self.versions, key=lambda item: item.dataset_version_id)
        )
        aliases = tuple(sorted(self.aliases, key=lambda item: item.alias))
        for name, values in (
            ("provider", providers),
            ("adapter", adapters),
            ("dataset", datasets),
            ("version", versions),
            ("alias", aliases),
        ):
            if len(values) > MAX_CATALOG_ITEMS:
                raise ValueError(f"catalog {name} count exceeds limit")
        _unique(providers, lambda item: item.source_provider_id, "provider")
        _unique(adapters, lambda item: item.adapter_id, "adapter")
        _unique(datasets, lambda item: item.dataset_id, "dataset")
        _unique(versions, lambda item: item.dataset_version_id, "version")
        _unique(aliases, lambda item: item.alias, "alias")
        provider_map = {item.source_provider_id: item for item in providers}
        adapter_map = {item.adapter_id: item for item in adapters}
        dataset_map = {item.dataset_id: item for item in datasets}
        version_map = {item.dataset_version_id: item for item in versions}
        for adapter in adapters:
            if adapter.source_provider_id not in provider_map:
                raise DatasetContractError(
                    DatasetFailureCode.UNKNOWN_REFERENCE,
                    "adapter references unknown provider "
                    f"{adapter.source_provider_id}",
                )
        for version in versions:
            descriptor = dataset_map.get(version.dataset_id)
            if descriptor is None:
                raise DatasetContractError(
                    DatasetFailureCode.UNKNOWN_REFERENCE,
                    f"version references unknown dataset {version.dataset_id}",
                )
            if version.origin not in descriptor.allowed_origins:
                raise DatasetContractError(
                    DatasetFailureCode.UNSUPPORTED_ORIGIN,
                    f"dataset {version.dataset_id} does not allow "
                    f"{version.origin.value}",
                )
            for partition in version.partitions:
                provider = provider_map.get(partition.source_provider_id)
                partition_adapter = adapter_map.get(partition.adapter_id)
                if provider is None or partition_adapter is None:
                    raise DatasetContractError(
                        DatasetFailureCode.UNKNOWN_REFERENCE,
                        "partition references an unknown provider or adapter",
                    )
                if (
                    partition_adapter.source_provider_id
                    != partition.source_provider_id
                    or partition_adapter.adapter_version
                    != partition.adapter_version
                    or partition_adapter.clock_policy_id
                    != partition.clock_policy_id
                    or partition_adapter.partition_policy_id
                    != partition.partition_policy_id
                    or partition_adapter.row_identity_policy_id
                    != partition.row_identity_policy_id
                    or provider.licensing_policy != partition.licensing_policy
                ):
                    raise DatasetContractError(
                        DatasetFailureCode.IDENTITY_MISMATCH,
                        "partition policy differs from provider/adapter "
                        "descriptors",
                    )
            for parent in version.parents:
                if parent.parent_dataset_version_id not in version_map:
                    raise DatasetContractError(
                        DatasetFailureCode.UNKNOWN_REFERENCE,
                        "version references unknown parent "
                        f"{parent.parent_dataset_version_id}",
                    )
        for alias in aliases:
            alias_target = version_map.get(alias.dataset_version_id)
            if (
                alias_target is None
                or alias_target.dataset_id != alias.dataset_id
            ):
                raise DatasetContractError(
                    DatasetFailureCode.UNKNOWN_REFERENCE,
                    f"alias {alias.alias} target is absent or cross-dataset",
                )
            if (
                alias.require_qualified
                and alias_target.qualification_status
                is not DatasetQualificationStatus.QUALIFIED
            ):
                raise DatasetContractError(
                    DatasetFailureCode.UNQUALIFIED_DATASET,
                    f"alias {alias.alias} requires a qualified target",
                )
        object.__setattr__(self, "providers", providers)
        object.__setattr__(self, "adapters", adapters)
        object.__setattr__(self, "datasets", datasets)
        object.__setattr__(self, "versions", versions)
        object.__setattr__(self, "aliases", aliases)
        expected = _stable_id("dataset-catalog", self.identity_payload())
        if self.catalog_id and self.catalog_id != expected:
            raise DatasetContractError(
                DatasetFailureCode.IDENTITY_MISMATCH,
                "catalog_id differs from immutable catalog content",
            )
        object.__setattr__(self, "catalog_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "providers": [item.to_dict() for item in self.providers],
            "adapters": [item.to_dict() for item in self.adapters],
            "datasets": [item.to_dict() for item in self.datasets],
            "versions": [item.to_dict() for item in self.versions],
            "aliases": [item.to_dict() for item in self.aliases],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {**self.identity_payload(), "catalog_id": self.catalog_id}

    def to_json(self) -> str:
        return canonical_contract_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DatasetCatalog":
        return cls(
            providers=tuple(
                SourceProviderDescriptorV1.from_dict(_mapping(value))
                for value in _sequence(data.get("providers"))
            ),
            adapters=tuple(
                ProviderAdapterDescriptorV1.from_dict(_mapping(value))
                for value in _sequence(data.get("adapters"))
            ),
            datasets=tuple(
                DatasetDescriptorV1.from_dict(_mapping(value))
                for value in _sequence(data.get("datasets"))
            ),
            versions=tuple(
                DatasetVersionManifestV1.from_dict(_mapping(value))
                for value in _sequence(data.get("versions"))
            ),
            aliases=tuple(
                DatasetAliasV1.from_dict(_mapping(value))
                for value in _sequence(data.get("aliases"))
            ),
            catalog_id=str(data.get("catalog_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "DatasetCatalog":
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as err:
            raise ValueError("dataset catalog is invalid JSON") from err
        return cls.from_dict(_mapping(payload))

    @classmethod
    def read(cls, path: str | Path) -> "DatasetCatalog":
        """Read one bounded catalog file."""
        target = Path(path).expanduser().resolve()
        try:
            content = target.read_bytes()
        except OSError as err:
            raise DatasetContractError(
                DatasetFailureCode.ARTIFACT_MISSING,
                f"dataset catalog cannot be read: {target}",
            ) from err
        if len(content) > MAX_DATASET_CATALOG_BYTES:
            raise ValueError("dataset catalog exceeds byte limit")
        try:
            return cls.from_json(content.decode("utf-8"))
        except UnicodeDecodeError as err:
            raise ValueError("dataset catalog is not UTF-8") from err

    def write(self, path: str | Path) -> Path:
        """Atomically write canonical catalog JSON without tick payloads."""
        target = Path(path).expanduser().resolve()
        target.parent.mkdir(parents=True, exist_ok=True)
        encoded = self.to_json().encode("utf-8") + b"\n"
        if len(encoded) > MAX_DATASET_CATALOG_BYTES:
            raise ValueError("dataset catalog exceeds byte limit")
        temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
        temporary.write_bytes(encoded)
        os.replace(temporary, target)
        return target

    def list_entries(self) -> tuple[dict[str, JSONValue], ...]:
        """Return a bounded list surface without artifact paths or evidence."""
        aliases_by_dataset: dict[str, list[str]] = {}
        for alias in self.aliases:
            aliases_by_dataset.setdefault(alias.dataset_id, []).append(
                alias.alias
            )
        versions_by_dataset: dict[str, list[DatasetVersionManifestV1]] = {}
        for version in self.versions:
            versions_by_dataset.setdefault(version.dataset_id, []).append(
                version
            )
        return tuple(
            {
                "dataset_id": descriptor.dataset_id,
                "display_name": descriptor.display_name,
                "allowed_origins": [
                    item.value for item in descriptor.allowed_origins
                ],
                "aliases": [
                    item
                    for item in sorted(
                        aliases_by_dataset.get(descriptor.dataset_id, [])
                    )
                ],
                "versions": [
                    {
                        "dataset_version_id": version.dataset_version_id,
                        "origin": version.origin.value,
                        "qualification_status": (
                            version.qualification_status.value
                        ),
                        "source_provider_ids": list(
                            version.source_provider_ids
                        ),
                        "row_count": version.row_count,
                    }
                    for version in versions_by_dataset.get(
                        descriptor.dataset_id, []
                    )
                ],
            }
            for descriptor in self.datasets
        )

    def describe(self, reference: str) -> dict[str, JSONValue]:
        """Describe a logical dataset, alias, or immutable version."""
        text = str(reference).strip()
        version = self._version_map().get(text)
        if version is not None:
            return {"kind": "dataset_version", "value": version.to_dict()}
        alias = self._alias_map().get(_safe_alias(text))
        if alias is not None:
            target = self._version_map()[alias.dataset_version_id]
            return {
                "kind": "dataset_alias",
                "value": alias.to_dict(),
                "resolved_version": target.to_dict(),
            }
        descriptor = self._dataset_map().get(_safe_dataset_id(text))
        if descriptor is not None:
            return {
                "kind": "dataset",
                "value": descriptor.to_dict(),
                "versions": [
                    item.to_dict()
                    for item in self.versions
                    if item.dataset_id == descriptor.dataset_id
                ],
                "aliases": [
                    item.to_dict()
                    for item in self.aliases
                    if item.dataset_id == descriptor.dataset_id
                ],
            }
        raise DatasetContractError(
            DatasetFailureCode.UNKNOWN_REFERENCE,
            f"unknown dataset reference {reference!r}",
        )

    def resolve(
        self,
        reference: str,
        *,
        query_scope: DatasetQueryScopeV1 | None = None,
    ) -> DatasetResolutionV1:
        """Resolve once and return an immutable receipt for all later work."""
        text = str(reference).strip()
        scope = query_scope or DatasetQueryScopeV1()
        version = self._version_map().get(text)
        alias: DatasetAliasV1 | None = None
        if version is None:
            alias = self._alias_map().get(_safe_alias(text))
            if alias is not None:
                version = self._version_map()[alias.dataset_version_id]
            else:
                dataset_id = _safe_dataset_id(text)
                matches = tuple(
                    item
                    for item in self.versions
                    if item.dataset_id == dataset_id
                    and item.qualification_status
                    is DatasetQualificationStatus.QUALIFIED
                )
                if len(matches) > 1:
                    raise DatasetContractError(
                        DatasetFailureCode.AMBIGUOUS_REFERENCE,
                        "logical dataset has multiple versions; use an alias "
                        "or immutable version",
                    )
                if len(matches) == 1:
                    version = matches[0]
        if version is None:
            raise DatasetContractError(
                DatasetFailureCode.UNKNOWN_REFERENCE,
                f"unknown dataset reference {reference!r}",
            )
        if (
            version.qualification_status
            is not DatasetQualificationStatus.QUALIFIED
        ):
            raise DatasetContractError(
                DatasetFailureCode.UNQUALIFIED_DATASET,
                "dataset version is not qualified: "
                f"{version.dataset_version_id}",
            )
        _scope_matches_version(scope, version)
        return DatasetResolutionV1(
            reference=text,
            dataset_id=version.dataset_id,
            dataset_version_id=version.dataset_version_id,
            manifest_sha256=version.manifest_sha256,
            query_scope=scope,
            alias=None if alias is None else alias.alias,
            alias_revision=None if alias is None else alias.revision,
            alias_id=None if alias is None else alias.alias_id,
        )

    def replay(self, receipt: DatasetResolutionV1) -> DatasetResolutionV1:
        """Replay the exact resolved version without consulting a moved alias."""
        if not isinstance(receipt, DatasetResolutionV1):
            raise TypeError("replay requires DatasetResolutionV1")
        version = self._version_map().get(receipt.dataset_version_id)
        if (
            version is None
            or version.dataset_id != receipt.dataset_id
            or version.manifest_sha256 != receipt.manifest_sha256
        ):
            raise DatasetContractError(
                DatasetFailureCode.STALE_ALIAS,
                "replay receipt target is absent or no longer byte-identical",
            )
        _scope_matches_version(receipt.query_scope, version)
        # Reconstruction verifies the deterministic receipt identity, ensuring
        # callers cannot mutate fields while retaining the old resolution_id.
        return DatasetResolutionV1.from_dict(receipt.to_dict())

    def require_current_alias(self, receipt: DatasetResolutionV1) -> None:
        """Fail when an alias changed after the supplied resolution receipt."""
        if receipt.alias is None:
            return
        current = self._alias_map().get(receipt.alias)
        if (
            current is None
            or current.alias_id != receipt.alias_id
            or current.revision != receipt.alias_revision
            or current.dataset_version_id != receipt.dataset_version_id
        ):
            raise DatasetContractError(
                DatasetFailureCode.STALE_ALIAS,
                f"alias {receipt.alias!r} moved after this receipt resolved",
            )

    def verify(
        self, reference: str | DatasetResolutionV1
    ) -> DatasetVerificationV1:
        """Hash-verify every partition and qualification evidence artifact."""
        if isinstance(reference, DatasetResolutionV1):
            resolution = self.replay(reference)
        else:
            resolution = self.resolve(reference)
        version = self._version_map()[resolution.dataset_version_id]
        hashes: list[str] = []
        for partition in version.partitions:
            _verify_artifact(partition.artifact)
            hashes.append(partition.artifact.sha256)
        for evidence in version.qualification_evidence:
            _verify_artifact(evidence)
            hashes.append(evidence.sha256)
        return DatasetVerificationV1(
            dataset_version_id=version.dataset_version_id,
            manifest_sha256=version.manifest_sha256,
            partition_count=len(version.partitions),
            evidence_count=len(version.qualification_evidence),
            verified_artifact_sha256=tuple(hashes),
        )

    def cursor(
        self,
        resolution: DatasetResolutionV1,
        *,
        origin: DatasetOrigin,
        series_id: str | None = None,
        period: str | None = None,
        row_id: int | None = None,
        ensemble_member_id: str | None = None,
        event_id: str | None = None,
    ) -> DatasetCursorV1:
        """Create a cursor bound to the exact version and query receipt."""
        exact = self.replay(resolution)
        cursor = DatasetCursorV1(
            resolution_id=exact.resolution_id,
            dataset_version_id=exact.dataset_version_id,
            query_scope_id=exact.query_scope.scope_id,
            origin=origin,
            series_id=series_id,
            period=period,
            row_id=row_id,
            ensemble_member_id=ensemble_member_id,
            event_id=event_id,
        )
        self.validate_cursor(cursor, exact)
        return cursor

    def validate_cursor(
        self, cursor: DatasetCursorV1, resolution: DatasetResolutionV1
    ) -> None:
        """Reject cursors that cross version, query, series, or origin scope."""
        exact = self.replay(resolution)
        if (
            cursor.resolution_id != exact.resolution_id
            or cursor.dataset_version_id != exact.dataset_version_id
            or cursor.query_scope_id != exact.query_scope.scope_id
        ):
            raise DatasetContractError(
                DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
                "cursor does not belong to this resolution and query scope",
            )
        scope = exact.query_scope
        if scope.origin is not None and cursor.origin is not scope.origin:
            raise DatasetContractError(
                DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
                "cursor origin differs from query scope",
            )
        version = self._version_map()[exact.dataset_version_id]
        if cursor.origin is not version.origin:
            raise DatasetContractError(
                DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
                "cursor origin differs from resolved dataset version",
            )
        if cursor.origin is DatasetOrigin.OBSERVED:
            partition = next(
                (
                    item
                    for item in version.partitions
                    if item.series_id == cursor.series_id
                    and item.period == cursor.period
                ),
                None,
            )
            if (
                partition is None
                or cursor.row_id is None
                or cursor.row_id > partition.row_count
            ):
                raise DatasetContractError(
                    DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
                    "cursor observed row is outside resolved dataset version",
                )
            if scope.symbols and partition.symbol not in scope.symbols:
                raise DatasetContractError(
                    DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
                    "cursor series is outside query symbol scope",
                )
            if scope.periods and partition.period not in scope.periods:
                raise DatasetContractError(
                    DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
                    "cursor period is outside query period scope",
                )
        elif (
            scope.ensemble_member_id is not None
            and cursor.ensemble_member_id != scope.ensemble_member_id
        ):
            raise DatasetContractError(
                DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
                "cursor ensemble member differs from query scope",
            )

    def reconstruction_inventory(
        self,
        resolution: DatasetResolutionV1,
        *,
        requested_start_ns: int,
        requested_end_ns: int,
        symbols: Iterable[str] = (),
        periods: Iterable[str] = (),
    ) -> ProviderSourceInventoryV2:
        """Build a dataset-bound provider-neutral reconstruction inventory."""
        exact = self.replay(resolution)
        version = self._version_map()[exact.dataset_version_id]
        if version.origin is not DatasetOrigin.OBSERVED:
            raise DatasetContractError(
                DatasetFailureCode.UNSUPPORTED_ORIGIN,
                "reconstruction source inventory requires observed evidence",
            )
        self.verify(exact)
        selected_symbols = (
            tuple(sorted({normalize_symbol(value) for value in symbols}))
            or exact.query_scope.symbols
        )
        selected_periods = (
            tuple(sorted({normalize_period(value) for value in periods}))
            or exact.query_scope.periods
        )
        partitions = tuple(
            item
            for item in version.partitions
            if (not selected_symbols or item.symbol in selected_symbols)
            and (not selected_periods or item.period in selected_periods)
            and item.coverage_end_ns > requested_start_ns
            and item.coverage_start_ns < requested_end_ns
        )
        if selected_symbols and selected_periods:
            expected = {
                (symbol, period)
                for symbol in selected_symbols
                for period in selected_periods
            }
            if {(item.symbol, item.period) for item in partitions} != expected:
                raise DatasetContractError(
                    DatasetFailureCode.INCONSISTENT_COVERAGE,
                    "reconstruction inventory lacks complete selected coverage",
                )
        return ProviderSourceInventoryV2(
            dataset_id=exact.dataset_id,
            dataset_version_id=exact.dataset_version_id,
            manifest_sha256=exact.manifest_sha256,
            requested_start_ns=requested_start_ns,
            requested_end_ns=requested_end_ns,
            partitions=partitions,
        )

    def preflight_reconstruction_inventory(
        self, inventory: ProviderSourceInventoryV2
    ) -> DatasetVerificationV1:
        """Verify a V2 inventory using only the public catalog contract."""
        version = self._version_map().get(inventory.dataset_version_id)
        if (
            version is None
            or version.dataset_id != inventory.dataset_id
            or version.manifest_sha256 != inventory.manifest_sha256
            or version.qualification_status
            is not DatasetQualificationStatus.QUALIFIED
            or version.origin is not DatasetOrigin.OBSERVED
        ):
            raise DatasetContractError(
                DatasetFailureCode.UNQUALIFIED_DATASET,
                "inventory dataset version is absent, changed, or unqualified",
            )
        by_id = {item.partition_id: item for item in version.partitions}
        if any(
            by_id.get(item.partition_id) != item
            for item in inventory.partitions
        ):
            raise DatasetContractError(
                DatasetFailureCode.IDENTITY_MISMATCH,
                "inventory partition differs from immutable dataset version",
            )
        return self.verify(inventory.dataset_version_id)

    def _provider_map(self) -> dict[str, SourceProviderDescriptorV1]:
        return {item.source_provider_id: item for item in self.providers}

    def _adapter_map(self) -> dict[str, ProviderAdapterDescriptorV1]:
        return {item.adapter_id: item for item in self.adapters}

    def _dataset_map(self) -> dict[str, DatasetDescriptorV1]:
        return {item.dataset_id: item for item in self.datasets}

    def _version_map(self) -> dict[str, DatasetVersionManifestV1]:
        return {item.dataset_version_id: item for item in self.versions}

    def _alias_map(self) -> dict[str, DatasetAliasV1]:
        return {item.alias: item for item in self.aliases}


def write_resolution_receipt(
    receipt: DatasetResolutionV1, path: str | Path
) -> Path:
    """Atomically persist one exact alias/version resolution."""
    target = Path(path).expanduser().resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    encoded = receipt.to_json().encode("utf-8") + b"\n"
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    temporary.write_bytes(encoded)
    os.replace(temporary, target)
    return target


def read_resolution_receipt(path: str | Path) -> DatasetResolutionV1:
    """Read a bounded exact resolution receipt."""
    target = Path(path).expanduser().resolve()
    try:
        content = target.read_bytes()
    except OSError as err:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_MISSING,
            f"resolution receipt cannot be read: {target}",
        ) from err
    if len(content) > 1_048_576:
        raise ValueError("resolution receipt exceeds byte limit")
    return DatasetResolutionV1.from_json(content.decode("utf-8"))


def _scope_matches_version(
    scope: DatasetQueryScopeV1, version: DatasetVersionManifestV1
) -> None:
    if scope.origin is not None and scope.origin is not version.origin:
        raise DatasetContractError(
            DatasetFailureCode.CURSOR_SCOPE_MISMATCH,
            "query origin differs from resolved dataset version",
        )
    if version.origin is not DatasetOrigin.OBSERVED:
        return
    available_symbols = {item.symbol for item in version.partitions}
    available_periods = {item.period for item in version.partitions}
    if scope.symbols and not set(scope.symbols).issubset(available_symbols):
        raise DatasetContractError(
            DatasetFailureCode.INCONSISTENT_COVERAGE,
            "query requests symbols absent from dataset version",
        )
    if scope.periods and not set(scope.periods).issubset(available_periods):
        raise DatasetContractError(
            DatasetFailureCode.INCONSISTENT_COVERAGE,
            "query requests periods absent from dataset version",
        )
    if scope.symbols and scope.periods:
        available_cells = {
            (item.symbol, item.period) for item in version.partitions
        }
        requested_cells = {
            (symbol, period)
            for symbol in scope.symbols
            for period in scope.periods
        }
        if not requested_cells.issubset(available_cells):
            raise DatasetContractError(
                DatasetFailureCode.INCONSISTENT_COVERAGE,
                "query requests absent symbol/period coverage cells",
            )


def _verify_artifact(ref: ArtifactRef) -> None:
    path = Path(ref.path)
    if not path.is_file():
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_MISSING,
            f"catalog artifact is missing: {path}",
        )
    if path.stat().st_size != ref.size_bytes:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_SIZE_MISMATCH,
            f"catalog artifact size differs: {path}",
        )
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    if digest.hexdigest() != ref.sha256:
        raise DatasetContractError(
            DatasetFailureCode.ARTIFACT_HASH_MISMATCH,
            f"catalog artifact hash differs: {path}",
        )


def _unique(values: tuple[Any, ...], key: Any, name: str) -> None:
    identities = [key(item) for item in values]
    if len(set(identities)) != len(identities):
        raise ValueError(f"catalog contains duplicate {name} identities")


def _safe_alias(value: str) -> str:
    try:
        return normalize_dataset_alias(value)
    except ValueError:
        return ""


def _safe_dataset_id(value: str) -> str:
    try:
        return normalize_dataset_id(value)
    except ValueError:
        return ""


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode()
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("value must be a mapping")
    return value


def _sequence(value: Any) -> tuple[Any, ...]:
    if isinstance(value, (str, bytes)) or not isinstance(value, list):
        raise TypeError("value must be a JSON array")
    return tuple(value)


__all__ = [
    "DATASET_CATALOG_SCHEMA_VERSION",
    "DatasetCatalog",
    "read_resolution_receipt",
    "write_resolution_receipt",
]
