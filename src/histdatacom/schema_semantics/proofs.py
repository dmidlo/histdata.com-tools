"""Re-executed proof qualification, separate from metadata graph queries."""

from __future__ import annotations

from histdatacom.schema_compatibility import (
    CompatibilityRegistryV1,
    EvidenceKind,
    ImplementationV1,
    MigrationClassification,
    MigrationEdgeV1,
)

from .canonical import canonical_json, sha256
from .contracts import (
    SemanticCompositionProofV1,
    SemanticMigrationProofV1,
    SemanticProjectionV1,
    SemanticRegistryValidationV1,
)
from .executors import (
    CANONICAL_EXECUTOR,
    INDENTED_EXECUTOR,
    execute_representation,
)
from .profiles import (
    module_fingerprint,
    project_semantics,
    semantic_profile,
    semantic_profiles,
)

SemanticProofV1 = SemanticMigrationProofV1 | SemanticCompositionProofV1


def representation_implementations() -> tuple[ImplementationV1, ...]:
    """Exact closed executable catalog; these do not upgrade native schemas."""
    fingerprint = module_fingerprint(
        ("schema_semantics/executors.py", "schema_semantics/canonical.py")
    )
    return tuple(
        ImplementationV1(
            name,
            "histdatacom.schema_semantics.executors.execute_representation",
            "1.0.0",
            fingerprint,
        )
        for name in (CANONICAL_EXECUTOR, INDENTED_EXECUTOR)
    )


def edge_subject_id(registry: CompatibilityRegistryV1, edge_id: str) -> str:
    """Acyclic subject includes semantics/implementation, not proof references."""
    edge = next(
        (item for item in registry.migrations if item.edge_id == edge_id), None
    )
    if edge is None:
        raise ValueError("unknown semantic migration edge")
    implementation = next(
        (
            item
            for item in registry.implementations
            if item.implementation_id == edge.implementation_id
        ),
        None,
    )
    if implementation is None:
        raise ValueError("missing semantic migration implementation")
    schemas = {item.schema_id: item for item in registry.schemas}
    profiles = {item.family: item.profile_id for item in semantic_profiles()}
    return "semantic-edge:sha256:" + sha256(
        canonical_json(
            {
                "edge_id": edge.edge_id,
                "source": edge.source,
                "destination": edge.destination,
                "source_version": schemas[edge.source].version,
                "destination_version": schemas[edge.destination].version,
                "source_family": schemas[edge.source].family,
                "destination_family": schemas[edge.destination].family,
                "source_wire_schema": schemas[edge.source].wire_schema,
                "destination_wire_schema": schemas[
                    edge.destination
                ].wire_schema,
                "source_profile": profiles.get(schemas[edge.source].family),
                "destination_profile": profiles.get(
                    schemas[edge.destination].family
                ),
                "classification": edge.classification.value,
                "implementation": implementation.to_dict(),
                "source_invariants": edge.source_invariants,
                "destination_invariants": edge.destination_invariants,
            }
        )
    )


def _admit(
    registry: CompatibilityRegistryV1,
    edge_id: str,
    source_profile: str,
    destination_profile: str,
) -> MigrationEdgeV1:
    edge = next(
        (item for item in registry.migrations if item.edge_id == edge_id), None
    )
    if (
        edge is None
        or not edge.qualified
        or edge.classification
        is not MigrationClassification.LOSSLESS_REPRESENTATION
    ):
        raise ValueError("edge is not declared qualified lossless")
    implementations = {
        item.implementation_id: item
        for item in representation_implementations()
    }
    declared = next(
        (
            item
            for item in registry.implementations
            if item.implementation_id == edge.implementation_id
        ),
        None,
    )
    if (
        declared is None
        or implementations.get(declared.implementation_id) != declared
    ):
        raise ValueError(
            "migration implementation is missing, changed or unreviewed"
        )
    schemas = {item.schema_id: item for item in registry.schemas}
    for endpoint, profile_id in (
        (edge.source, source_profile),
        (edge.destination, destination_profile),
    ):
        profile = semantic_profile(profile_id)
        schema = schemas[endpoint]
        if (
            schema.family != profile.family
            or schema.wire_schema != profile.wire_schema
        ):
            raise ValueError("migration schema does not match reviewed profile")
    return edge


def prove_semantic_migration(
    registry: CompatibilityRegistryV1,
    edge_id: str,
    source_profile_id: str,
    destination_profile_id: str,
    source_json: str,
) -> SemanticMigrationProofV1:
    """Execute admitted code and independently project both actual artifacts."""
    edge = _admit(registry, edge_id, source_profile_id, destination_profile_id)
    assert edge.implementation_id is not None
    before = project_semantics(source_profile_id, source_json)
    destination = execute_representation(edge.implementation_id, source_json)
    after = project_semantics(destination_profile_id, destination)
    implementation = next(
        item
        for item in representation_implementations()
        if item.implementation_id == edge.implementation_id
    )
    return SemanticMigrationProofV1(
        edge_subject_id(registry, edge_id),
        edge.implementation_id,
        implementation.source_sha256,
        before.profile_id,
        after.profile_id,
        source_json,
        destination,
        before,
        after,
    )


def verify_semantic_projection(
    projection: SemanticProjectionV1, input_json: str
) -> SemanticProjectionV1:
    actual = project_semantics(projection.profile_id, input_json)
    if actual != projection:
        raise ValueError("semantic projection does not recompute")
    return actual


def verify_semantic_proof(
    registry: CompatibilityRegistryV1, proof: SemanticProofV1
) -> SemanticProofV1:
    """Re-execute each bound implementation; never trust self-resealed hashes."""
    steps: tuple[SemanticMigrationProofV1, ...]
    if type(proof) is SemanticCompositionProofV1:
        steps = proof.steps
    elif type(proof) is SemanticMigrationProofV1:
        steps = (proof,)
    else:
        raise ValueError("unsupported semantic proof type")
    subjects = {
        edge_subject_id(registry, item.edge_id): item.edge_id
        for item in registry.migrations
        if item.implementation_id is not None
    }
    edge_ids = []
    for step in steps:
        edge_id = subjects.get(step.edge_subject_id)
        if edge_id is None:
            raise ValueError("semantic proof has unknown edge subject")
        expected = prove_semantic_migration(
            registry,
            edge_id,
            step.source_profile_id,
            step.destination_profile_id,
            step.source_json,
        )
        if expected != step:
            raise ValueError("semantic proof execution or projection differs")
        edge_ids.append(edge_id)
    if type(proof) is SemanticCompositionProofV1:
        if tuple(edge_ids) not in {
            item.edge_ids for item in registry.compositions
        }:
            raise ValueError("semantic composition lacks explicit declaration")
        # Reconstruct to enforce adjacency even for forged process-local values.
        SemanticCompositionProofV1(tuple(steps))
    return proof


def validate_lossless_evidence(
    registry: CompatibilityRegistryV1, proofs: tuple[SemanticProofV1, ...] = ()
) -> SemanticRegistryValidationV1:
    """Mandatory offline bundled-registry gate; custom graphs are declarations.

    References are matched by content hash, never opened or imported. Every
    qualified lossless edge and every all-lossless declared composition needs
    its own re-executed proof, not inferred reachability or a parser success.
    """
    if type(proofs) is not tuple or len(proofs) > 512:
        raise ValueError("semantic proof catalog bound")
    catalog_bytes = 0
    for proof in proofs:
        if type(proof) not in (
            SemanticMigrationProofV1,
            SemanticCompositionProofV1,
        ):
            raise ValueError("unsupported semantic proof type")
        catalog_bytes += len(proof.to_json())
        if catalog_bytes > 64 * 1024 * 1024:
            raise ValueError("semantic proof catalog aggregate byte bound")
    catalog = {}
    for proof in proofs:
        verify_semantic_proof(registry, proof)
        digest = sha256(proof.to_json())
        if digest in catalog:
            raise ValueError("duplicate semantic proof")
        catalog[digest] = proof
    evidence = {item.evidence_id: item for item in registry.evidence}
    edges = {item.edge_id: item for item in registry.migrations}
    used: set[str] = set()
    subjects = []
    for edge in registry.migrations:
        if (
            not edge.qualified
            or edge.classification
            is not MigrationClassification.LOSSLESS_REPRESENTATION
        ):
            continue
        subject = edge_subject_id(registry, edge.edge_id)
        subjects.append(subject)
        matches = [
            catalog.get(evidence[name].sha256)
            for name in edge.evidence
            if evidence[name].kind is EvidenceKind.INVARIANT_CHECK
        ]
        verified = [
            proof
            for proof in matches
            if type(proof) is SemanticMigrationProofV1
            and proof.edge_subject_id == subject
        ]
        if not verified:
            raise ValueError("lossless edge lacks re-executed semantic proof")
        golden_hashes = {
            evidence[name].sha256
            for name in edge.evidence
            if evidence[name].kind is EvidenceKind.GOLDEN_CORPUS
        }
        if any(
            sha256(proof.source_json) not in golden_hashes for proof in verified
        ):
            raise ValueError("semantic proof lacks exact input corpus evidence")
        used.update(proof.artifact_id for proof in verified)
    for composition in registry.compositions:
        if not all(
            edges[name].qualified
            and edges[name].classification
            is MigrationClassification.LOSSLESS_REPRESENTATION
            for name in composition.edge_ids
        ):
            continue
        expected = tuple(
            edge_subject_id(registry, name) for name in composition.edge_ids
        )
        matches = [
            catalog.get(evidence[name].sha256)
            for name in composition.evidence
            if evidence[name].kind is EvidenceKind.COMPOSITION
        ]
        verified_compositions = [
            proof
            for proof in matches
            if type(proof) is SemanticCompositionProofV1
            and tuple(step.edge_subject_id for step in proof.steps) == expected
        ]
        if not verified_compositions:
            raise ValueError(
                "lossless composition lacks re-executed semantic proof"
            )
        used.update(proof.artifact_id for proof in verified_compositions)
    if used != {proof.artifact_id for proof in proofs}:
        raise ValueError("unreferenced semantic proof catalog entry")
    return SemanticRegistryValidationV1(
        registry.registry_id, tuple(sorted(set(subjects))), tuple(sorted(used))
    )
