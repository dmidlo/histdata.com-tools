"""Actual migration re-execution, path evidence, forged receipts and storage."""

import json
import os
from dataclasses import replace

import pytest

from histdatacom.schema_compatibility import (
    EvidenceKind,
    EvidenceV1,
    schema_compatibility_registry,
)
from histdatacom.schema_semantics import (
    SemanticCompositionProofV1,
    SemanticMigrationProofV1,
    edge_subject_id,
    prove_semantic_migration,
    read_semantic_proof,
    validate_lossless_evidence,
    verify_semantic_proof,
    write_semantic_proof,
)
from histdatacom.schema_semantics import proofs as proof_module
from histdatacom.schema_semantics.canonical import sha256
from histdatacom.schema_semantics.executors import (
    INDENTED_EXECUTOR,
    execute_representation,
)
from tests.fixtures.schema_semantics_graph import graph_fixture
from histdatacom.synthetic.contracts import SyntheticEventV1


@pytest.fixture
def graph():
    event = SyntheticEventV1.observed(
        symbol="EURUSD",
        event_time_ns=10,
        event_sequence=0,
        bid=1.0,
        ask=1.001,
        run_id="fixture-run",
        ensemble_member_id="member-1",
        source_version_id="source-1",
        source_series_id="series-1",
        source_period="202001",
        source_row_id=1,
    )
    return graph_fixture(json.dumps(event.to_dict(), indent=4))


def test_actual_direct_and_composed_encoding_migrations_recompute(graph):
    registry, (first, second, composition) = graph
    assert (
        first.source_json != first.destination_json != second.destination_json
    )
    assert (
        execute_representation(INDENTED_EXECUTOR, first.source_json)
        == second.destination_json
    )
    assert (
        first.source_projection.semantic_json
        == second.destination_projection.semantic_json
    )
    receipt = validate_lossless_evidence(registry, (first, second, composition))
    assert receipt.registry_id == registry.registry_id
    assert len(receipt.lossless_subject_ids) == 2
    assert len(receipt.proof_ids) == 3
    assert SemanticMigrationProofV1.from_json(first.to_json()) == first
    assert (
        SemanticCompositionProofV1.from_json(composition.to_json())
        == composition
    )


def test_bundled_zero_edges_is_explicit_nonclaim_and_future_edge_requires_proof(
    graph,
):
    bundled = schema_compatibility_registry()
    assert not bundled.migrations
    assert validate_lossless_evidence(bundled).lossless_subject_ids == ()
    with pytest.raises(ValueError, match="lacks re-executed"):
        validate_lossless_evidence(graph[0])


def test_proof_reader_reexecutes_wrong_implementation_even_equivalent_pair(
    graph, monkeypatch
):
    registry, (first, _, _) = graph
    monkeypatch.setattr(
        proof_module,
        "execute_representation",
        lambda implementation_id, value: value,
    )
    with pytest.raises(ValueError, match="execution or projection differs"):
        verify_semantic_proof(registry, first)


def test_equivalent_supplied_destination_does_not_prove_bound_execution(graph):
    registry, (first, _, _) = graph
    from histdatacom.schema_semantics import project_semantics

    destination = json.dumps(json.loads(first.source_json), indent=3)
    forged = replace(
        first,
        destination_json=destination,
        destination_projection=project_semantics(
            first.destination_profile_id, destination
        ),
    )
    with pytest.raises(ValueError, match="execution or projection differs"):
        verify_semantic_proof(registry, forged)


def test_resealed_false_projection_and_changed_implementation_are_refused(
    graph,
):
    registry, (first, _, _) = graph
    false_before = replace(
        first.source_projection, semantic_json='{"false":true}'
    )
    false_after = replace(
        first.destination_projection, semantic_json='{"false":true}'
    )
    forged = replace(
        first,
        source_projection=false_before,
        destination_projection=false_after,
    )
    with pytest.raises(ValueError):
        verify_semantic_proof(
            registry, SemanticMigrationProofV1.from_json(forged.to_json())
        )
    changed = replace(
        registry,
        implementations=tuple(
            replace(item, source_sha256="a" * 64)
            for item in registry.implementations
        ),
    )
    with pytest.raises(ValueError):
        verify_semantic_proof(changed, first)


def test_missing_composition_golden_and_wrong_evidence_hash_refuse(graph):
    registry, evidence = graph
    with pytest.raises(ValueError, match="composition lacks"):
        validate_lossless_evidence(registry, evidence[:2])
    altered = replace(
        registry,
        evidence=tuple(
            (
                replace(item, sha256="f" * 64)
                if item.evidence_id == "golden-0"
                else item
            )
            for item in registry.evidence
        ),
    )
    with pytest.raises(ValueError, match="exact input corpus"):
        validate_lossless_evidence(altered, evidence)
    altered = replace(
        registry,
        evidence=tuple(
            (
                replace(item, sha256="f" * 64)
                if item.evidence_id == "invariant-0"
                else item
            )
            for item in registry.evidence
        ),
    )
    with pytest.raises(ValueError, match="lacks re-executed"):
        validate_lossless_evidence(altered, evidence)


@pytest.mark.parametrize("qualified", [False, True])
def test_parallel_edges_do_not_shadow_qualified_evidence(graph, qualified):
    registry, evidence = graph
    sibling = replace(
        registry.migrations[0],
        edge_id="compact-other",
        priority=1,
        qualified=qualified,
        evidence=("golden-0", "invariant-other"),
    )
    added = EvidenceV1(
        "invariant-other",
        EvidenceKind.INVARIANT_CHECK,
        "fixture:parallel",
        "0" * 64,
    )
    registry = replace(
        registry,
        migrations=tuple(
            sorted(
                (*registry.migrations, sibling), key=lambda item: item.edge_id
            )
        ),
        evidence=tuple(
            sorted(
                (*registry.evidence, added), key=lambda item: item.evidence_id
            )
        ),
    )
    assert edge_subject_id(registry, "compact") != edge_subject_id(
        registry, "compact-other"
    )
    if qualified:
        first = evidence[0]
        extra = prove_semantic_migration(
            registry,
            sibling.edge_id,
            first.source_profile_id,
            first.destination_profile_id,
            first.source_json,
        )
        registry = replace(
            registry,
            evidence=tuple(
                (
                    replace(item, sha256=sha256(extra.to_json()))
                    if item.evidence_id == "invariant-other"
                    else item
                )
                for item in registry.evidence
            ),
        )
        evidence = (*evidence, extra)
    receipt = validate_lossless_evidence(registry, evidence)
    assert len(receipt.lossless_subject_ids) == (3 if qualified else 2)


def test_endpoint_version_and_named_edge_change_subject_without_hash_cycle(
    graph,
):
    registry, evidence = graph
    altered = replace(
        registry,
        schemas=tuple(
            (
                replace(item, version="1.1.0")
                if item.schema_id == "encoding-b"
                else item
            )
            for item in registry.schemas
        ),
    )
    assert edge_subject_id(registry, "compact") != edge_subject_id(
        altered, "compact"
    )
    with pytest.raises(ValueError, match="unknown edge"):
        verify_semantic_proof(altered, evidence[0])
    changed_evidence = replace(
        registry,
        evidence=tuple(
            replace(item, locator="other:metadata")
            for item in registry.evidence
        ),
    )
    assert edge_subject_id(registry, "compact") == edge_subject_id(
        changed_evidence, "compact"
    )


def test_unknown_native_family_and_lossy_edge_cannot_get_exact_proof(graph):
    registry, (first, _, _) = graph
    from histdatacom.schema_compatibility import MigrationClassification

    altered = replace(
        registry,
        migrations=tuple(
            (
                replace(
                    item, classification=MigrationClassification.LOSSY_ADVISORY
                )
                if item.edge_id == "compact"
                else item
            )
            for item in registry.migrations
        ),
    )
    with pytest.raises(ValueError):
        prove_semantic_migration(
            altered,
            "compact",
            first.source_profile_id,
            first.destination_profile_id,
            first.source_json,
        )
    with pytest.raises(ValueError, match="unreviewed"):
        prove_semantic_migration(
            registry,
            "compact",
            "future-family-v99",
            first.destination_profile_id,
            first.source_json,
        )


def test_canonical_write_once_and_independent_read_replay(graph, tmp_path):
    registry, evidence = graph
    for proof in evidence:
        path = write_semantic_proof(proof, tmp_path, registry)
        assert read_semantic_proof(path, registry) == proof
        assert write_semantic_proof(proof, tmp_path, registry) == path
    path.write_text('{"tampered":true}')
    with pytest.raises(ValueError):
        read_semantic_proof(path, registry)
    with pytest.raises(ValueError):
        write_semantic_proof(evidence[-1], tmp_path, registry)


def test_nonregular_proof_paths_fail_closed_without_blocking(graph, tmp_path):
    registry, evidence = graph
    path = write_semantic_proof(evidence[0], tmp_path, registry)
    link = tmp_path / "link.json"
    try:
        link.symlink_to(path)
    except NotImplementedError:
        pass
    except OSError as exc:
        if os.name != "nt" or getattr(exc, "winerror", None) != 1314:
            raise
    else:
        with pytest.raises(ValueError):
            read_semantic_proof(link, registry)
    if hasattr(os, "mkfifo"):
        fifo = tmp_path / "pipe.json"
        os.mkfifo(fifo)
        with pytest.raises(ValueError):
            read_semantic_proof(fifo, registry)


def test_failed_atomic_publication_leaves_no_accepted_or_temporary_artifact(
    graph, tmp_path, monkeypatch
):
    from histdatacom.schema_semantics import storage

    def fail_link(*args, **kwargs):
        raise OSError("synthetic interrupted publication")

    monkeypatch.setattr(storage.os, "link", fail_link)
    with pytest.raises(OSError):
        write_semantic_proof(graph[1][0], tmp_path, graph[0])
    assert tuple(tmp_path.iterdir()) == ()


def test_bounded_read_refuses_before_reading_proof(
    graph, tmp_path, monkeypatch
):
    from histdatacom.schema_semantics import storage

    path = write_semantic_proof(graph[1][0], tmp_path, graph[0])
    monkeypatch.setattr(storage, "MAX_PROOF_BYTES", 10)
    with pytest.raises(ValueError, match="bounded regular"):
        read_semantic_proof(path, graph[0])


def test_duplicate_unreferenced_and_wrong_type_catalogs_refuse(graph):
    registry, evidence = graph
    with pytest.raises(ValueError, match="duplicate"):
        validate_lossless_evidence(registry, (*evidence, evidence[0]))
    with pytest.raises(ValueError, match="unsupported"):
        validate_lossless_evidence(registry, ("not-a-proof",))
    with pytest.raises(ValueError, match="catalog bound"):
        validate_lossless_evidence(registry, list(evidence))


@pytest.mark.parametrize("text", ["[]", "null", "1", '"string"'])
def test_nonobject_canonical_files_are_deliberately_refused(
    graph, tmp_path, text
):
    path = tmp_path / (sha256(text) + ".json")
    path.write_text(text)
    with pytest.raises(ValueError, match="object envelope"):
        read_semantic_proof(path, graph[0])
