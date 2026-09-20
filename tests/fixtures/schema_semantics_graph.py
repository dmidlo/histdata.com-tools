"""Actual encoding migrations in a synthetic graph, not historical upgrades."""

from dataclasses import replace

from histdatacom.schema_compatibility import (
    CompatibilityRegistryV1,
    CompositionV1,
    EvidenceKind,
    EvidenceV1,
    MigrationClassification,
    MigrationEdgeV1,
    SchemaV1,
    SupportStatus,
)
from histdatacom.schema_semantics import (
    SemanticCompositionProofV1,
    prove_semantic_migration,
    representation_implementations,
    semantic_profile,
)
from histdatacom.schema_semantics.canonical import sha256
from histdatacom.schema_semantics.executors import (
    CANONICAL_EXECUTOR,
    INDENTED_EXECUTOR,
)


def graph_fixture(source_json: str, profile_name: str = "synthetic-event-v1"):
    profile = semantic_profile(profile_name)
    implementations = representation_implementations()
    schemas = tuple(
        SchemaV1(
            name,
            profile.family,
            "1.0.0",
            profile.wire_schema,
            SupportStatus.SUPPORTED,
            (CANONICAL_EXECUTOR,),
            (CANONICAL_EXECUTOR,),
            ("synthetic-exact-current-envelope",),
            ("golden-0",),
            "Synthetic encoding descriptor; not a new native schema generation.",
        )
        for name in ("encoding-a", "encoding-b", "encoding-c")
    )
    edges = tuple(
        MigrationEdgeV1(
            name,
            source,
            destination,
            MigrationClassification.LOSSLESS_REPRESENTATION,
            0,
            implementation,
            True,
            schemas[0].invariants,
            schemas[0].invariants,
            (f"golden-{index}", f"invariant-{index}"),
        )
        for index, (name, source, destination, implementation) in enumerate(
            (
                ("compact", "encoding-a", "encoding-b", CANONICAL_EXECUTOR),
                ("indent", "encoding-b", "encoding-c", INDENTED_EXECUTOR),
            )
        )
    )

    def evidence(kind, name, digest):
        return EvidenceV1(name, kind, "fixture:" + name, digest)

    evidence_items = (
        evidence(EvidenceKind.COMPOSITION, "composition", "0" * 64),
        evidence(EvidenceKind.GOLDEN_CORPUS, "golden-0", sha256(source_json)),
        evidence(EvidenceKind.GOLDEN_CORPUS, "golden-1", "0" * 64),
        evidence(EvidenceKind.INVARIANT_CHECK, "invariant-0", "0" * 64),
        evidence(EvidenceKind.INVARIANT_CHECK, "invariant-1", "0" * 64),
    )
    registry = CompatibilityRegistryV1(
        schemas,
        implementations,
        evidence_items,
        edges,
        (CompositionV1(("compact", "indent"), ("composition",)),),
        (),
    )
    first = prove_semantic_migration(
        registry, "compact", profile.profile_id, profile.profile_id, source_json
    )
    second = prove_semantic_migration(
        registry,
        "indent",
        profile.profile_id,
        profile.profile_id,
        first.destination_json,
    )
    composition = SemanticCompositionProofV1((first, second))
    hashes = {
        "invariant-0": sha256(first.to_json()),
        "invariant-1": sha256(second.to_json()),
        "composition": sha256(composition.to_json()),
        "golden-1": sha256(second.source_json),
    }
    registry = replace(
        registry,
        evidence=tuple(
            replace(item, sha256=hashes.get(item.evidence_id, item.sha256))
            for item in registry.evidence
        ),
    )
    return registry, (first, second, composition)
