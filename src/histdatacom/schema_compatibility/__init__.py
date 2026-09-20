"""Public metadata-only compatibility queries; no producer imports or execution."""

from functools import lru_cache
from importlib.resources import files

from .contracts import (
    MAX_REGISTRY_BYTES,
    CompatibilityRegistryV1,
    CompositionV1,
    EvidenceKind,
    EvidenceV1,
    ExemptionV1,
    ImplementationV1,
    MigrationClassification,
    MigrationEdgeV1,
    SchemaV1,
    SupportStatus,
)
from .graph import CompatibilityRefusal

__all__ = [
    "CompatibilityRefusal",
    "CompatibilityRegistryV1",
    "CompositionV1",
    "EvidenceKind",
    "EvidenceV1",
    "ExemptionV1",
    "ImplementationV1",
    "MigrationClassification",
    "MigrationEdgeV1",
    "SchemaV1",
    "SupportStatus",
    "can_migrate",
    "can_read",
    "exact_semantics",
    "migration_path",
    "schema_compatibility_registry",
]


@lru_cache(maxsize=1)
def schema_compatibility_registry() -> CompatibilityRegistryV1:
    """Load the packaged, immutable registry, without importing its producers."""
    with (
        files(__package__)
        .joinpath("assets/registry_v1.json")
        .open("rb") as handle
    ):
        data = handle.read(MAX_REGISTRY_BYTES + 1)
    if len(data) > MAX_REGISTRY_BYTES:
        raise ValueError("packaged compatibility registry exceeds byte bound")
    return CompatibilityRegistryV1.from_json(data.decode("ascii"))


def can_read(
    schema: str, *, reader: str | None = None, reader_version: str | None = None
) -> bool:
    return schema_compatibility_registry().can_read(
        schema, reader=reader, reader_version=reader_version
    )


def can_migrate(source: str, destination: str, *, exact: bool = False) -> bool:
    return schema_compatibility_registry().can_migrate(
        source, destination, exact=exact
    )


def migration_path(
    source: str, destination: str, *, exact: bool = False
) -> tuple[MigrationEdgeV1, ...]:
    return schema_compatibility_registry().migration_path(
        source, destination, exact=exact
    )


def exact_semantics(source: str, destination: str) -> bool:
    return schema_compatibility_registry().exact_semantics(source, destination)
