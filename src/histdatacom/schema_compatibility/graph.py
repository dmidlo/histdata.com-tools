"""Qualified metadata graph; path lookup never imports or executes a producer."""

from __future__ import annotations

import re
from collections import deque
from itertools import pairwise
from typing import TypeVar

from .contracts import (
    CompatibilityRegistryV1,
    EvidenceKind,
    MigrationClassification,
    MigrationEdgeV1,
    SchemaV1,
    SupportStatus,
)

_T = TypeVar("_T")


class CompatibilityRefusal(ValueError):
    """No supported, unambiguous, fully evidenced metadata answer."""


def version_key(
    value: str,
) -> tuple[int, int, int, tuple[tuple[int, int, str], ...]]:
    if value == "unversioned":
        return (-1, -1, -1, ())
    match = re.fullmatch(
        r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)"
        r"(?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?"
        r"(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?",
        value,
    )
    if match is None or len(value) > 128:
        raise ValueError("invalid compatibility SemVer")
    prerelease = match.group(4)
    parts: list[tuple[int, int, str]] = []
    for part in prerelease.split(".") if prerelease else ():
        if part.isdigit():
            if len(part) > 1 and part.startswith("0"):
                raise ValueError("invalid numeric SemVer prerelease")
            parts.append((0, int(part), ""))
        else:
            parts.append((1, 0, part))
    return (
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
        tuple(parts) if prerelease else ((2, 0, ""),),
    )


def _index(items: tuple[_T, ...], field: str) -> dict[str, _T]:
    keys = [getattr(item, field) for item in items]
    if keys != sorted(set(keys)):
        raise ValueError("registry entries must have sorted unique identities")
    return dict(zip(keys, items))


def validate_registry(registry: CompatibilityRegistryV1) -> None:
    schemas = _index(registry.schemas, "schema_id")
    implementations = _index(registry.implementations, "implementation_id")
    evidence = _index(registry.evidence, "evidence_id")
    edges = _index(registry.migrations, "edge_id")
    _index(registry.exemptions, "qualified_name")
    pairs = [item.edge_ids for item in registry.compositions]
    if pairs != sorted(set(pairs)):
        raise ValueError("duplicate or unordered composition")
    indegree = dict.fromkeys(schemas, 0)
    outgoing: dict[str, list[str]] = {key: [] for key in schemas}
    for schema in schemas.values():
        if not set(schema.readers + schema.writers) <= implementations.keys():
            raise ValueError("missing reader or writer implementation")
        if not set(schema.evidence) <= evidence.keys():
            raise ValueError("missing schema evidence")
    for edge in edges.values():
        if edge.source not in schemas or edge.destination not in schemas:
            raise ValueError("unknown migration endpoint")
        if edge.implementation_id is not None and (
            edge.implementation_id not in implementations
        ):
            raise ValueError("missing migration implementation")
        if not set(edge.evidence) <= evidence.keys():
            raise ValueError("missing migration evidence")
        if edge.qualified:
            if any(
                schemas[key].status is SupportStatus.UNSUPPORTED
                for key in (edge.source, edge.destination)
            ):
                raise ValueError("qualified edge has unsupported endpoint")
            kinds = {evidence[key].kind for key in edge.evidence}
            if (
                not {EvidenceKind.GOLDEN_CORPUS, EvidenceKind.INVARIANT_CHECK}
                <= kinds
            ):
                raise ValueError(
                    "migration requires golden and invariant evidence"
                )
            if (
                edge.source_invariants != schemas[edge.source].invariants
                or edge.destination_invariants
                != schemas[edge.destination].invariants
            ):
                raise ValueError("migration invariant coverage differs")
        indegree[edge.destination] += 1
        outgoing[edge.source].append(edge.destination)
    ready = deque(key for key, degree in indegree.items() if degree == 0)
    visited = 0
    while ready:
        node = ready.popleft()
        visited += 1
        for destination in outgoing[node]:
            indegree[destination] -= 1
            if indegree[destination] == 0:
                ready.append(destination)
    if visited != len(schemas):
        raise ValueError("cyclic migration graph")
    for composition in registry.compositions:
        if not set(composition.edge_ids) <= edges.keys():
            raise ValueError("unknown composition edge")
        path = tuple(edges[key] for key in composition.edge_ids)
        if any(not edge.qualified for edge in path) or any(
            left.destination != right.source for left, right in pairwise(path)
        ):
            raise ValueError("invalid qualified composition")
        if not set(composition.evidence) <= evidence.keys() or not any(
            evidence[key].kind is EvidenceKind.COMPOSITION
            for key in composition.evidence
        ):
            raise ValueError("missing composition test evidence")
    # Validate the same selection policy before accepting the graph. No query
    # has to discover an already-known ambiguous best path at runtime.
    endpoints = {
        (path[0].source, path[-1].destination) for path in _paths(registry)
    }
    for source, destination in endpoints:
        for exact in (False, True):
            _select(
                registry, source, destination, exact=exact, allow_missing=True
            )


def _schema(registry: CompatibilityRegistryV1, identity: str) -> SchemaV1:
    # A wire label is convenient only when it names exactly one contract.
    matches = [item for item in registry.schemas if item.schema_id == identity]
    if not matches:
        matches = [
            item for item in registry.schemas if item.wire_schema == identity
        ]
    if len(matches) != 1:
        raise CompatibilityRefusal("unknown or ambiguous schema identity")
    result = matches[0]
    if result.status is SupportStatus.UNSUPPORTED:
        raise CompatibilityRefusal("unsupported schema")
    return result


def can_read(
    registry: CompatibilityRegistryV1,
    schema: str,
    *,
    reader: str | None = None,
    reader_version: str | None = None,
) -> bool:
    try:
        item = _schema(registry, schema)
    except CompatibilityRefusal:
        return False
    implementations = {
        item.implementation_id: item for item in registry.implementations
    }
    return any(
        (reader is None or reader in (key, implementations[key].qualified_name))
        and (
            reader_version is None
            or implementations[key].version == reader_version
        )
        for key in item.readers
    )


def migration_path(
    registry: CompatibilityRegistryV1,
    source: str,
    destination: str,
    *,
    exact: bool = False,
) -> tuple[MigrationEdgeV1, ...]:
    start, end = _schema(registry, source), _schema(registry, destination)
    if start.schema_id == end.schema_id:
        if not start.readers:
            raise CompatibilityRefusal("identity path has no direct reader")
        return ()
    result = _select(registry, start.schema_id, end.schema_id, exact=exact)
    assert result is not None
    return result


def _paths(
    registry: CompatibilityRegistryV1,
) -> list[tuple[MigrationEdgeV1, ...]]:
    # Enumerate ONLY independently qualified single edges and explicitly tested
    # complete compositions. Reachability does not manufacture qualifications.
    edges = {edge.edge_id: edge for edge in registry.migrations}
    paths: list[tuple[MigrationEdgeV1, ...]] = [
        (edge,) for edge in edges.values() if edge.qualified
    ]
    paths.extend(
        tuple(edges[key] for key in composition.edge_ids)
        for composition in registry.compositions
    )
    return paths


def _select(
    registry: CompatibilityRegistryV1,
    source: str,
    destination: str,
    *,
    exact: bool,
    allow_missing: bool = False,
) -> tuple[MigrationEdgeV1, ...] | None:
    paths = _paths(registry)
    admissible = [
        path
        for path in paths
        if path[0].source == source
        and path[-1].destination == destination
        and (
            not exact
            or all(
                edge.classification
                is MigrationClassification.LOSSLESS_REPRESENTATION
                for edge in path
            )
        )
    ]
    if not admissible:
        if allow_missing:
            return None
        raise CompatibilityRefusal("no qualified migration composition")
    # Explicit priority, then declared destination schema versions. Edge IDs
    # are NOT a hidden tie breaker: equally preferred alternatives refuse.
    versions = {
        item.schema_id: version_key(item.version) for item in registry.schemas
    }

    def rank(path: tuple[MigrationEdgeV1, ...]) -> tuple[object, ...]:
        return (
            len(path),
            tuple(edge.priority for edge in path),
            tuple(versions[edge.destination] for edge in path),
        )

    admissible.sort(key=rank)
    if len(admissible) > 1 and rank(admissible[0]) == rank(admissible[1]):
        raise CompatibilityRefusal("ambiguous equal-priority migration paths")
    return admissible[0]
