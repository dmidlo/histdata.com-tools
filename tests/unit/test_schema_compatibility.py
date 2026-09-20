"""Synthetic graph qualifications do not certify production migrations."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from dataclasses import FrozenInstanceError, replace

import pytest

from histdatacom.schema_compatibility import (
    CompatibilityRefusal,
    CompatibilityRegistryV1,
    CompositionV1,
    EvidenceKind,
    EvidenceV1,
    ImplementationV1,
    MigrationClassification,
    MigrationEdgeV1,
    SchemaV1,
    SupportStatus,
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _fixture_identity(value: dict[str, int]) -> dict[str, int]:
    """The retained executable implementation for synthetic graph fixtures."""
    return dict(value)


def _schema(
    name: str, version: str = "1.0.0", *, read: bool = True
) -> SchemaV1:
    return SchemaV1(
        name,
        "fixture." + name,
        version,
        "fixture." + name,
        SupportStatus.SUPPORTED if read else SupportStatus.WRITE_ONLY,
        ("fixture-implementation",) if read else (),
        ("fixture-implementation",),
        ("invariant:" + name,),
        ("fixture-golden",),
        "Synthetic graph fixture, not a shipped migration.",
    )


def _edge(
    name: str,
    source: str,
    destination: str,
    *,
    priority: int = 0,
    classification: MigrationClassification = MigrationClassification.LOSSLESS_REPRESENTATION,
    qualified: bool = True,
) -> MigrationEdgeV1:
    return MigrationEdgeV1(
        name,
        source,
        destination,
        classification,
        priority,
        "fixture-implementation",
        qualified,
        ("invariant:" + source,),
        ("invariant:" + destination,),
        ("fixture-golden", "fixture-invariant"),
    )


def _registry(
    schemas: tuple[SchemaV1, ...],
    edges: tuple[MigrationEdgeV1, ...] = (),
    paths: tuple[tuple[str, ...], ...] = (),
) -> CompatibilityRegistryV1:
    return CompatibilityRegistryV1(
        tuple(sorted(schemas, key=lambda item: item.schema_id)),
        (
            ImplementationV1(
                "fixture-implementation",
                "tests.unit.test_schema_compatibility._fixture_identity",
                "1.0.0",
                hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            ),
        ),
        (
            EvidenceV1(
                "fixture-composition",
                EvidenceKind.COMPOSITION,
                "synthetic:identity-composition",
                _sha("identity-composition"),
            ),
            EvidenceV1(
                "fixture-golden",
                EvidenceKind.GOLDEN_CORPUS,
                "synthetic:identity-golden",
                _sha('{"value":1}'),
            ),
            EvidenceV1(
                "fixture-invariant",
                EvidenceKind.INVARIANT_CHECK,
                "synthetic:value-equality",
                _sha("value-equality"),
            ),
        ),
        tuple(sorted(edges, key=lambda item: item.edge_id)),
        tuple(
            CompositionV1(path, ("fixture-composition",))
            for path in sorted(paths)
        ),
        (),
    )


def test_direct_reader_is_not_migration_reachability() -> None:
    registry = _registry(
        (_schema("a"), _schema("b", read=False)), (_edge("ab", "a", "b"),)
    )
    assert registry.can_read("a")
    assert registry.can_read(
        "a",
        reader="tests.unit.test_schema_compatibility._fixture_identity",
        reader_version="1.0.0",
    )
    assert not registry.can_read("a", reader_version="2.0.0")
    assert not registry.can_read("b")
    assert registry.can_migrate("a", "b")
    assert registry.exact_semantics("a", "b")
    assert registry.migration_path("a", "a") == ()
    assert not registry.can_migrate("b", "b")
    assert not registry.can_migrate("missing", "a")
    assert not registry.can_read("missing")


def test_composition_requires_its_own_retained_evidence() -> None:
    schemas = tuple(_schema(name) for name in "abc")
    edges = (_edge("ab", "a", "b"), _edge("bc", "b", "c"))
    unqualified_path = _registry(schemas, edges)
    assert not unqualified_path.can_migrate("a", "c")
    qualified = _registry(schemas, edges, (("ab", "bc"),))
    assert tuple(
        edge.edge_id for edge in qualified.migration_path("a", "c")
    ) == ("ab", "bc")
    assert qualified.exact_semantics("a", "c")
    assert qualified == CompatibilityRegistryV1.from_json(qualified.to_json())
    # Actual retained synthetic golden bytes and composition, not reachability
    # relabelled as execution evidence. No production artifact is migrated.
    original = {"value": 1}
    value = original
    for _edge_record in qualified.migration_path("a", "c"):
        value = _fixture_identity(value)
    assert value == original and value is not original
    assert _sha(json.dumps(value, separators=(",", ":"))) == next(
        item.sha256
        for item in qualified.evidence
        if item.evidence_id == "fixture-golden"
    )


@pytest.mark.parametrize(
    "classification",
    [
        MigrationClassification.SEMANTIC_SUCCESSOR,
        MigrationClassification.LOSSY_ADVISORY,
    ],
)
def test_semantic_successor_and_lossy_are_not_exact(
    classification: MigrationClassification,
) -> None:
    registry = _registry(
        (_schema("a"), _schema("b")),
        (_edge("ab", "a", "b", classification=classification),),
    )
    assert registry.can_migrate("a", "b")
    assert not registry.exact_semantics("a", "b")
    with pytest.raises(CompatibilityRefusal):
        registry.migration_path("a", "b", exact=True)


def test_unqualified_edge_is_not_a_migration() -> None:
    registry = _registry(
        (_schema("a"), _schema("b")), (_edge("ab", "a", "b", qualified=False),)
    )
    assert not registry.can_migrate("a", "b")


def test_shortest_then_priority_then_numeric_semver() -> None:
    schemas = (
        _schema("a"),
        _schema("b", "1.10.0"),
        _schema("c", "1.2.0"),
        _schema("d"),
    )
    edges = (
        _edge("ab", "a", "b"),
        _edge("bd", "b", "d"),
        _edge("ac", "a", "c"),
        _edge("cd", "c", "d"),
    )
    registry = _registry(schemas, edges, (("ab", "bd"), ("ac", "cd")))
    assert registry.migration_path("a", "d")[0].edge_id == "ac"
    priority = tuple(
        replace(edge, priority=1) if edge.edge_id == "ac" else edge
        for edge in edges
    )
    assert (
        _registry(schemas, priority, (("ab", "bd"), ("ac", "cd")))
        .migration_path("a", "d")[0]
        .edge_id
        == "ab"
    )
    shorter = _registry(
        schemas,
        (*edges, _edge("ad", "a", "d", priority=999)),
        (("ab", "bd"), ("ac", "cd")),
    )
    assert tuple(edge.edge_id for edge in shorter.migration_path("a", "d")) == (
        "ad",
    )


def test_registry_validation_detects_ambiguity_not_only_query() -> None:
    with pytest.raises(CompatibilityRefusal, match="ambiguous"):
        _registry(
            tuple(_schema(name) for name in "abcd"),
            (
                _edge("ab", "a", "b"),
                _edge("bd", "b", "d"),
                _edge("ac", "a", "c"),
                _edge("cd", "c", "d"),
            ),
            (("ab", "bd"), ("ac", "cd")),
        )


def test_cycles_refuse_even_if_edges_not_qualified() -> None:
    with pytest.raises(ValueError, match="cyclic"):
        _registry(
            (_schema("a"), _schema("b")),
            (
                _edge("ab", "a", "b", qualified=False),
                _edge("ba", "b", "a", qualified=False),
            ),
        )


def test_unsupported_intermediate_cannot_qualify_composition() -> None:
    with pytest.raises(ValueError, match="unsupported endpoint"):
        _registry(
            (
                _schema("a"),
                replace(_schema("b"), status=SupportStatus.UNSUPPORTED),
                _schema("c"),
            ),
            (_edge("ab", "a", "b"), _edge("bc", "b", "c")),
            (("ab", "bc"),),
        )


def test_missing_implementation_and_invariant_evidence_refuse() -> None:
    schemas = (_schema("a"), _schema("b"))
    with pytest.raises(ValueError, match="missing migration implementation"):
        _registry(
            schemas,
            (replace(_edge("ab", "a", "b"), implementation_id="absent"),),
        )
    with pytest.raises(ValueError, match="golden and invariant"):
        _registry(
            schemas,
            (replace(_edge("ab", "a", "b"), evidence=("fixture-golden",)),),
        )
    with pytest.raises(ValueError, match="invariant coverage"):
        _registry(
            schemas,
            (replace(_edge("ab", "a", "b"), source_invariants=("invented",)),),
        )


@pytest.mark.parametrize(
    "version", ["1", "1.2", "01.2.3", "1.2.3-01", "not-version"]
)
def test_schema_version_is_explicit_semver_or_unversioned(version: str) -> None:
    with pytest.raises(ValueError, match="SemVer"):
        _schema("a", version)


def test_strict_bounded_canonical_and_immutable_contracts() -> None:
    registry = _registry((_schema("a"),))
    with pytest.raises(FrozenInstanceError):
        registry.schema_version = "bad"  # type: ignore[misc]
    with pytest.raises(ValueError, match="noncanonical"):
        CompatibilityRegistryV1.from_json(json.dumps(registry.to_dict()))
    with pytest.raises(ValueError, match="duplicate"):
        CompatibilityRegistryV1.from_json('{"schemas":[],"schemas":[]}')
    with pytest.raises(ValueError, match="unknown or missing"):
        CompatibilityRegistryV1.from_dict({**registry.to_dict(), "extra": True})
    with pytest.raises(ValueError, match="scalar type"):
        replace(_edge("ab", "a", "b"), priority=True)
    with pytest.raises(ValueError, match="expansion bound"):
        CompatibilityRegistryV1.from_json("[" * 22 + "0" + "]" * 22)
