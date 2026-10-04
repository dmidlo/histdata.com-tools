"""Pure synthetic graph evidence, not native proof or filesystem authority."""

from dataclasses import replace

from hypothesis import given, settings, strategies as st
import pytest

from histdatacom.artifact_retention import contracts as c
from histdatacom.artifact_retention import planning as p

TOKEN = "a" * 32
DIGEST = "b" * 64
FOREIGN = "retention-object:sha256:" + "f" * 64
PLAN_ID = "retention-collection-plan:sha256:" + "c" * 64


def obj(
    number,
    *,
    kind=c.RetentionClass.SCRATCH,
    adapter="managed-transaction-scratch.v1",
    edges=(),
    historical=(),
    created=1,
    transaction=TOKEN,
):
    return c.NativeObjectV1(
        adapter,
        "synthetic-evidence.v1",
        f"subject-{number}",
        c.ManagedPayloadRefV1(
            f"objects/{number:032x}.data", f"{number:064x}", 10
        ),
        kind,
        tuple(
            sorted(edges, key=lambda e: (e.target_object_id, e.relation.value))
        ),
        tuple(sorted(historical)),
        transaction,
        created,
    )


def edge(item, relation=c.DependencyRelation.NATIVE_ARTIFACT):
    return c.DependencyV1(item.artifact_id, relation)


def snap(
    objects,
    *,
    roots=(),
    holds=(),
    proofs=(),
    completions=None,
    tombstones=(),
    present=None,
    blockers=(),
    ttl=10,
):
    objects = tuple(sorted(objects, key=lambda item: item.artifact_id))
    if completions is None:
        scratch = tuple(
            x.artifact_id
            for x in objects
            if x.retention_class is c.RetentionClass.SCRATCH
        )
        completions = (
            (
                c.TransactionCompletionV1(
                    TOKEN, "aborted", scratch, (), DIGEST, 10
                ),
            )
            if scratch
            else ()
        )
    return c.StoreSnapshotV1(
        TOKEN,
        c.RetentionPolicyV1(ttl),
        DIGEST,
        DIGEST,
        objects,
        (
            tuple(sorted(x.artifact_id for x in objects))
            if present is None
            else tuple(sorted(present))
        ),
        tuple(sorted(roots)),
        tuple(sorted(holds, key=lambda x: x.artifact_id)),
        tuple(sorted(completions, key=lambda x: x.artifact_id)),
        tuple(sorted(proofs, key=lambda x: x.artifact_id)),
        tuple(sorted(tombstones, key=lambda x: x.artifact_id)),
        tuple(sorted(blockers)),
    )


def plan(snapshot, cutoff=20, now=20):
    return p._derive_plan(snapshot, cutoff_ns=cutoff, observed_now_ns=now)


def decisions(result):
    return {x.object_id: x for x in result.decisions}


def blocked(result, prefix):
    assert any(x.startswith(prefix) for x in result.blockers), result.blockers
    assert not any(x.action == "delete" for x in result.decisions)


def cache_graph():
    source = obj(
        1,
        kind=c.RetentionClass.IMMUTABLE_SOURCE,
        adapter="ascii-tick-source.v1",
    )
    recipe = obj(
        2,
        kind=c.RetentionClass.REPRODUCIBILITY_DEPENDENCY,
        adapter="ascii-tick-recipe.v1",
        edges=(edge(source, c.DependencyRelation.SOURCE),),
    )
    cache = obj(
        3,
        kind=c.RetentionClass.REPLACEABLE_CACHE,
        adapter="histdata-arrow-cache.v1",
        edges=(
            edge(source, c.DependencyRelation.SOURCE),
            edge(recipe, c.DependencyRelation.RECIPE),
        ),
    )
    proof = c.CacheRegenerationV1(
        recipe.artifact_id,
        source.artifact_id,
        cache.artifact_id,
        cache.payload_ref.sha256,
        10,
        cache.payload_ref.sha256,
        10,
        cache.native_id,
        DIGEST,
        DIGEST,
        "synthetic-backend",
    )
    return source, recipe, cache, proof


def tombstone(item, **changes):
    record = c.CollectionTombstoneV1(
        item.artifact_id,
        PLAN_ID,
        "journal-1",
        item.payload_ref.sha256,
        item.payload_ref.size_bytes,
        15,
    )
    return replace(record, **changes)


def reference_paths(graph, roots):
    """Exhaustive finite DAG path enumeration, independent of planner BFS."""
    choices = {}
    pending = [(root,) for root in roots]
    while pending:
        path = pending.pop()
        choices.setdefault(path[-1], []).append(path)
        pending.extend((*path, child) for child in graph[path[-1]])
    return {
        node: min(paths, key=lambda path: (len(path), path))
        for node, paths in choices.items()
    }


@settings(max_examples=40, deadline=None, database=None, derandomize=True)
@given(
    st.lists(st.booleans(), min_size=15, max_size=15),
    st.sets(st.integers(0, 5)),
    st.sets(st.integers(0, 5)),
)
def test_dag_closure_and_lexical_witnesses_match_independent_oracle(
    flags, root_indices, held_indices
):
    objects = []
    cursor = 0
    for index in range(6):
        targets = []
        for previous in range(index):
            if flags[cursor]:
                targets.append(edge(objects[previous]))
            cursor += 1
        objects.append(obj(index + 1, edges=targets))
    roots = {objects[i].artifact_id for i in root_indices}
    held = {objects[i].artifact_id for i in held_indices}
    holds = (
        (c.RetentionHoldV1(tuple(sorted(held)), "synthetic hold", 2),)
        if held
        else ()
    )
    graph = {
        x.artifact_id: tuple(e.target_object_id for e in x.live_dependencies)
        for x in objects
    }
    expected = reference_paths(graph, roots | held)
    snapshot = snap(objects, roots=roots, holds=holds)
    result = plan(snapshot)
    assert result.blockers == ()
    assert tuple(x.object_id for x in result.decisions) == tuple(sorted(graph))
    for object_id, decision in decisions(result).items():
        assert decision.protected_path == expected.get(object_id, ())
        assert decision.action == (
            "keep" if object_id in expected else "delete"
        )
    assert (
        plan(
            snap(
                tuple(reversed(objects)),
                roots=reversed(sorted(roots)),
                holds=holds,
            )
        )
        == result
    )
    assert c.CollectionPlanV1.from_json(result.to_json()) == result


@pytest.mark.parametrize("relation", tuple(c.DependencyRelation))
def test_every_edge_kind_protects_shared_parent_and_migration_or_packed_bytes(
    relation,
):
    shared = obj(1)
    left = obj(2, edges=(edge(shared, relation),))
    right = obj(3, edges=(edge(shared, relation),))
    root = obj(4, edges=(edge(left), edge(right)))
    orphan = obj(5)
    result = plan(
        snap((shared, left, right, root, orphan), roots=(root.artifact_id,))
    )
    found = decisions(result)
    assert result.blockers == ()
    assert found[shared.artifact_id].protected_path == (
        root.artifact_id,
        min(left.artifact_id, right.artifact_id),
        shared.artifact_id,
    )
    assert all(
        found[x.artifact_id].action == "keep"
        for x in (shared, left, right, root)
    )
    assert found[orphan.artifact_id].action == "delete"


@pytest.mark.parametrize("kind", tuple(c.RetentionClass))
def test_all_ten_classes_have_explicit_retention_behavior(kind):
    if kind is c.RetentionClass.REPLACEABLE_CACHE:
        source, recipe, item, proof = cache_graph()
        result = plan(snap((source, recipe, item), proofs=(proof,)))
    else:
        item = obj(1, kind=kind)
        result = plan(snap((item,)))
    decision = decisions(result)[item.artifact_id]
    if kind is c.RetentionClass.RESTRICTED_PROVIDER:
        blocked(result, "restricted_provider_disposition_unsupported:")
    else:
        assert result.blockers == ()
    if kind in {c.RetentionClass.REPLACEABLE_CACHE, c.RetentionClass.SCRATCH}:
        assert decision.action == "delete"
        assert decision.protected_path == ()
    else:
        assert decision.action == "keep"
        assert decision.protected_path == (item.artifact_id,)


def test_historical_receipts_do_not_keep_cache_but_actual_byte_edge_does():
    source, recipe, cache, proof = cache_graph()
    historical = obj(
        4,
        kind=c.RetentionClass.RELEASE_CERTIFICATION,
        adapter="cache-regeneration.v1",
        historical=(cache.artifact_id,),
    )
    result = plan(snap((source, recipe, cache, historical), proofs=(proof,)))
    assert result.blockers == ()
    assert decisions(result)[cache.artifact_id].action == "delete"
    live_receipt = replace(
        historical,
        live_dependencies=(
            edge(cache, c.DependencyRelation.VERIFICATION_EVIDENCE),
        ),
    )
    result = plan(snap((source, recipe, cache, live_receipt), proofs=(proof,)))
    assert decisions(result)[cache.artifact_id].protected_path == (
        live_receipt.artifact_id,
        cache.artifact_id,
    )


def test_published_cache_history_does_not_make_disposable_output_a_root():
    source, recipe, cache, proof = cache_graph()
    scratch = obj(4)
    completion = c.TransactionCompletionV1(
        TOKEN,
        "published",
        (scratch.artifact_id,),
        (cache.artifact_id,),
        DIGEST,
        10,
    )
    result = plan(
        snap(
            (source, recipe, cache, scratch),
            proofs=(proof,),
            completions=(completion,),
        )
    )
    assert result.blockers == ()
    assert decisions(result)[cache.artifact_id].action == "delete"
    assert decisions(result)[scratch.artifact_id].action == "delete"


@pytest.mark.parametrize(
    "mode,prefix",
    [
        ("missing", "missing_live_dependency:"),
        ("opaque", "opaque_reference"),
        ("adapter", "unsupported_native_adapter:"),
        ("unknown_present", "unregistered_live_payload"),
    ],
)
def test_uncertain_complete_inventory_blocks_even_unrelated_orphan(
    mode, prefix
):
    orphan = obj(1)
    other = obj(2)
    kwargs = {}
    if mode == "missing":
        other = replace(
            other,
            live_dependencies=(
                c.DependencyV1(FOREIGN, c.DependencyRelation.NATIVE_ARTIFACT),
            ),
        )
    elif mode == "opaque":
        kwargs["blockers"] = ("opaque_reference:unparsed-native-ref",)
    elif mode == "adapter":
        other = replace(other, adapter_id="unreviewed.v1")
    else:
        kwargs["present"] = (orphan.artifact_id, other.artifact_id, FOREIGN)
    blocked(plan(snap((orphan, other), **kwargs)), prefix)


@pytest.mark.parametrize("registered", ["root", "hold"])
def test_explicit_root_or_hold_has_no_age_override(registered):
    item = obj(1)
    kwargs = (
        {"roots": (item.artifact_id,)}
        if registered == "root"
        else {
            "holds": (
                c.RetentionHoldV1((item.artifact_id,), "legal review", 2),
            )
        }
    )
    result = plan(snap((item,), **kwargs), cutoff=10**12, now=10**12)
    assert result.blockers == ()
    assert decisions(result)[item.artifact_id].action == "keep"
    assert decisions(result)[item.artifact_id].reason == (
        "registered_root" if registered == "root" else "registered_hold"
    )


@pytest.mark.parametrize("registered", ["root", "hold"])
def test_missing_protected_subject_blocks_complete_plan(registered):
    kwargs = (
        {"roots": (FOREIGN,)}
        if registered == "root"
        else {"holds": (c.RetentionHoldV1((FOREIGN,), "hold", 2),)}
    )
    blocked(plan(snap((obj(1),), **kwargs)), "missing_protected_root:")


@pytest.mark.parametrize(
    "mode,prefix",
    [
        ("none", "unproven_cache_classification:"),
        ("foreign_source", "missing_regeneration_subject:"),
        ("source_class", "invalid_regeneration_classification:"),
        ("source_absent", "missing_regeneration_input:"),
        ("cache_edge", "missing_regeneration_edge:"),
        ("recipe_edge", "missing_regeneration_edge:"),
        ("output", "regeneration_output_mismatch:"),
        ("native_id", "regeneration_output_mismatch:"),
        ("ambiguous", "ambiguous_regeneration_proof:"),
    ],
)
def test_exact_cache_proof_binding_failures_are_global_blockers(mode, prefix):
    source, recipe, cache, proof = cache_graph()
    kwargs = {}
    if mode == "foreign_source":
        proof = replace(proof, source_object_id=FOREIGN)
    elif mode == "source_class":
        source = replace(
            source, retention_class=c.RetentionClass.PUBLISHED_DERIVED
        )
        proof = replace(proof, source_object_id=source.artifact_id)
    elif mode == "source_absent":
        kwargs["present"] = (recipe.artifact_id, cache.artifact_id)
    elif mode == "cache_edge":
        cache = replace(cache, live_dependencies=())
        proof = replace(proof, cache_object_id=cache.artifact_id)
    elif mode == "recipe_edge":
        recipe = replace(recipe, live_dependencies=())
        proof = replace(proof, recipe_object_id=recipe.artifact_id)
    elif mode == "output":
        proof = replace(
            proof, historical_output_sha256=DIGEST, rebuilt_sha256=DIGEST
        )
    elif mode == "native_id":
        proof = replace(proof, native_partition_id="foreign-native-partition")
    proofs = () if mode == "none" else (proof,)
    if mode == "ambiguous":
        proofs = (proof, replace(proof, backend_id="another-backend"))
    blocked(
        plan(snap((source, recipe, cache), proofs=proofs, **kwargs)), prefix
    )


@pytest.mark.parametrize(
    "cutoff,ttl,action",
    [(9, 10, "keep"), (19, 10, "keep"), (20, 10, "delete"), (10, 0, "delete")],
)
def test_scratch_exact_completion_ttl_boundary(cutoff, ttl, action):
    item = obj(1)
    result = plan(snap((item,), ttl=ttl), cutoff=cutoff, now=20)
    assert result.blockers == ()
    assert decisions(result)[item.artifact_id].action == action


@pytest.mark.parametrize(
    "cutoff,now",
    [(True, 20), (20, True), (0, 20), (21, 20), (20.0, 20), (20, 2**63)],
)
def test_private_planner_rejects_invalid_clock_not_claiming_public_clock_authority(
    cutoff, now
):
    with pytest.raises(ValueError):
        plan(snap((obj(1),)), cutoff=cutoff, now=now)


@pytest.mark.parametrize(
    "mode,prefix",
    [
        ("incomplete", "incomplete_scratch_transaction:"),
        ("foreign_tx", "invalid_scratch_completion:"),
        ("ambiguous", "ambiguous_transaction_completion:"),
        ("future_object", "clock_precedes_object:"),
        ("future_completion", "clock_precedes_completion:"),
        ("future_hold", "clock_precedes_hold"),
        ("foreign_publication", "invalid_completed_publication:"),
    ],
)
def test_transaction_and_clock_evidence_is_not_inferred(mode, prefix):
    item = obj(1, created=21 if mode == "future_object" else 1)
    completion = c.TransactionCompletionV1(
        TOKEN,
        "aborted",
        (item.artifact_id,),
        (),
        DIGEST,
        21 if mode == "future_completion" else 10,
    )
    completions = (completion,)
    holds = ()
    if mode == "incomplete":
        completions = ()
    elif mode == "foreign_tx":
        completions = (replace(completion, transaction_id="d" * 32),)
    elif mode == "ambiguous":
        completions = (completion, replace(completion, completed_ns=11))
    elif mode == "future_hold":
        holds = (c.RetentionHoldV1((item.artifact_id,), "hold", 21),)
    elif mode == "foreign_publication":
        completions = (
            replace(
                completion, outcome="published", published_object_ids=(FOREIGN,)
            ),
        )
    blocked(plan(snap((item,), completions=completions, holds=holds)), prefix)


def test_absent_cache_requires_exact_tombstone_not_merely_historical_proof():
    source, recipe, cache, proof = cache_graph()
    snapshot = snap(
        (source, recipe, cache),
        proofs=(proof,),
        present=(source.artifact_id, recipe.artifact_id),
    )
    result = plan(snapshot)
    blocked(result, "unexplained_missing_payload:")
    assert decisions(result)[cache.artifact_id].action == "block"
    result = plan(replace(snapshot, tombstones=(tombstone(cache),)))
    assert result.blockers == ()
    assert decisions(result)[cache.artifact_id].action == "already_collected"
    assert all(
        decisions(result)[x.artifact_id].action == "keep"
        for x in (source, recipe)
    )


@pytest.mark.parametrize(
    "mode",
    [
        "foreign",
        "present",
        "permanent",
        "digest",
        "size",
        "before_created",
        "future",
        "duplicate",
    ],
)
def test_invalid_tombstone_never_permits_unrelated_deletion(mode):
    item = obj(1, created=3)
    orphan = obj(2)
    present = (orphan.artifact_id,)
    if mode == "permanent":
        item = replace(item, retention_class=c.RetentionClass.IMMUTABLE_SOURCE)
    record = tombstone(item)
    if mode == "foreign":
        record = replace(record, object_id=FOREIGN)
    elif mode == "present":
        present = (item.artifact_id, orphan.artifact_id)
    elif mode == "digest":
        record = replace(record, payload_sha256=DIGEST)
    elif mode == "size":
        record = replace(record, size_bytes_unlinked=9)
    elif mode == "before_created":
        record = replace(record, unlinked_ns=2)
    elif mode == "future":
        record = replace(record, unlinked_ns=21)
    records = (
        (record,)
        if mode != "duplicate"
        else (record, replace(record, journal_entry_id="journal-2"))
    )
    result = plan(snap((item, orphan), present=present, tombstones=records))
    blocked(result, "invalid_collection_tombstone:")


def test_collected_subject_still_required_by_live_root_blocks_missing_bytes():
    item = obj(1)
    root = obj(2, edges=(edge(item),))
    result = plan(
        snap(
            (item, root),
            roots=(root.artifact_id,),
            present=(root.artifact_id,),
            tombstones=(tombstone(item),),
        )
    )
    blocked(result, "missing_live_dependency:")


@pytest.mark.parametrize(
    "length,expected", [(64, set()), (65, {"graph_depth_exceeded"})]
)
def test_abstract_graph_depth_boundary_includes_disconnected_component(
    length, expected
):
    graph = {str(i): (str(i + 1),) for i in range(length)}
    graph[str(length)] = ()
    graph["unrelated-root"] = ()
    assert p._graph_blockers(graph) == expected


def test_abstract_cycle_and_missing_target_are_not_accepted_as_dag():
    assert p._graph_blockers({"a": ("b",), "b": ("a",)}) == {
        "cyclic_native_dependency_graph"
    }
    assert p._graph_blockers({"a": ("missing",)}) == {
        "missing_live_dependency:missing"
    }


def test_snapshot_nested_tampering_is_rejected_before_planning():
    snapshot = snap((obj(1),))
    object.__setattr__(snapshot.descriptors[0], "retention_class", "scratch")
    with pytest.raises(ValueError):
        plan(snapshot)
