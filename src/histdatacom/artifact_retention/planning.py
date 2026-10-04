"""Pure bounded closure, used only after the managed store verifies its state.

This module cannot authorize filesystem operations. Apply must independently
reinspect the locked store, rerun native verification and compare the exact plan.
"""

from __future__ import annotations

from collections import deque

from .contracts import (
    MAX_GRAPH_DEPTH,
    MAX_INTEGER,
    CollectionDecisionV1,
    CollectionPlanV1,
    DependencyRelation,
    NativeObjectV1,
    RetentionClass,
    StoreSnapshotV1,
)

# The storage/native dispatcher is closed as well. Adding a name here alone
# cannot admit a payload, and unsupported native families block the whole plan.
SUPPORTED_ADAPTER_IDS = frozenset(
    {
        "ascii-tick-source.v1",
        "ascii-tick-recipe.v1",
        "histdata-arrow-cache.v1",
        "dataset-catalog.v1",
        "cache-regeneration.v1",
        "managed-transaction-scratch.v1",
    }
)
_COLLECTABLE = frozenset(
    {RetentionClass.REPLACEABLE_CACHE, RetentionClass.SCRATCH}
)


def _graph_blockers(graph: dict[str, tuple[str, ...]]) -> set[str]:
    """Bound complete DAG depth, including disconnected retained components."""
    blockers: set[str] = set()
    color: dict[str, int] = {}
    longest: dict[str, int] = {}
    for start in sorted(graph):
        if color.get(start) == 2:
            continue
        pending: list[tuple[str, bool]] = [(start, False)]
        while pending:
            node, leaving = pending.pop()
            if leaving:
                color[node] = 2
                longest[node] = max(
                    (longest.get(child, 0) + 1 for child in graph[node]),
                    default=0,
                )
                if longest[node] > MAX_GRAPH_DEPTH:
                    blockers.add("graph_depth_exceeded")
                continue
            if color.get(node) == 2:
                continue
            if color.get(node) == 1:
                blockers.add("cyclic_native_dependency_graph")
                continue
            color[node] = 1
            pending.append((node, True))
            for child in reversed(graph[node]):
                if child not in graph:
                    blockers.add(f"missing_live_dependency:{child}")
                elif color.get(child) == 1:
                    blockers.add("cyclic_native_dependency_graph")
                elif color.get(child) != 2:
                    pending.append((child, False))
    return blockers


def _protected_paths(
    graph: dict[str, tuple[str, ...]], roots: set[str]
) -> dict[str, tuple[str, ...]]:
    """Deterministic shortest witnesses, with lexical ordering for equal paths."""
    paths: dict[str, tuple[str, ...]] = {
        root: (root,) for root in sorted(roots) if root in graph
    }
    queue = deque(paths)
    while queue:
        node = queue.popleft()
        if len(paths[node]) > MAX_GRAPH_DEPTH:
            continue
        for child in graph[node]:
            if child in graph and child not in paths:
                paths[child] = (*paths[node], child)
                queue.append(child)
    return paths


def _proof_blockers(
    snapshot: StoreSnapshotV1,
    objects: dict[str, NativeObjectV1],
    live: set[str],
) -> set[str]:
    blockers: set[str] = set()
    seen: set[str] = set()
    for proof in snapshot.regenerations:
        cache_id = proof.cache_object_id
        if cache_id in seen:
            blockers.add(f"ambiguous_regeneration_proof:{cache_id}")
        seen.add(cache_id)
        cache = objects.get(cache_id)
        source = objects.get(proof.source_object_id)
        recipe = objects.get(proof.recipe_object_id)
        if cache is None or source is None or recipe is None:
            blockers.add(f"missing_regeneration_subject:{cache_id}")
            continue
        if (
            cache.retention_class is not RetentionClass.REPLACEABLE_CACHE
            or cache.adapter_id != "histdata-arrow-cache.v1"
            or source.retention_class is not RetentionClass.IMMUTABLE_SOURCE
            or source.adapter_id != "ascii-tick-source.v1"
            or recipe.retention_class
            is not RetentionClass.REPRODUCIBILITY_DEPENDENCY
            or recipe.adapter_id != "ascii-tick-recipe.v1"
        ):
            blockers.add(f"invalid_regeneration_classification:{cache_id}")
        if (
            proof.source_object_id not in live
            or proof.recipe_object_id not in live
        ):
            blockers.add(f"missing_regeneration_input:{cache_id}")
        expected = {
            (proof.source_object_id, DependencyRelation.SOURCE),
            (proof.recipe_object_id, DependencyRelation.RECIPE),
        }
        actual = {
            (edge.target_object_id, edge.relation)
            for edge in cache.live_dependencies
        }
        if not expected.issubset(actual) or not any(
            edge.target_object_id == proof.source_object_id
            and edge.relation is DependencyRelation.SOURCE
            for edge in recipe.live_dependencies
        ):
            blockers.add(f"missing_regeneration_edge:{cache_id}")
        if (
            proof.historical_output_sha256 != cache.payload_ref.sha256
            or proof.historical_output_size_bytes
            != cache.payload_ref.size_bytes
            or proof.native_partition_id != cache.native_id
        ):
            blockers.add(f"regeneration_output_mismatch:{cache_id}")
    for object_id, descriptor in objects.items():
        if (
            descriptor.retention_class is RetentionClass.REPLACEABLE_CACHE
            and object_id not in seen
        ):
            blockers.add(f"unproven_cache_classification:{object_id}")
    return blockers


def _derive_plan(
    snapshot: StoreSnapshotV1, *, cutoff_ns: int, observed_now_ns: int
) -> CollectionPlanV1:
    """Derive evidence only; the caller of this private function owns the clock.

    The public store planner samples actual runtime time and rejects a future
    cutoff. The public apply routine samples it again under the same lock used
    by admission, registration and publication. Passing arbitrary times here
    does not create an applicable deletion plan.
    """
    if type(snapshot) is not StoreSnapshotV1:
        raise ValueError("planning requires an exact verified store snapshot")
    snapshot._readmit()
    if (
        type(cutoff_ns) is not int
        or type(observed_now_ns) is not int
        or not 0 < cutoff_ns <= observed_now_ns <= MAX_INTEGER
    ):
        raise ValueError("planning cutoff must not exceed fresh observed time")
    objects = {item.artifact_id: item for item in snapshot.descriptors}
    live = set(snapshot.present_object_ids)
    blockers = set(snapshot.blockers)
    if live - objects.keys():
        blockers.add("unregistered_live_payload")
    collected: set[str] = set()
    for tombstone in snapshot.tombstones:
        object_id = tombstone.object_id
        removed = objects.get(object_id)
        if (
            removed is None
            or object_id in live
            or object_id in collected
            or removed.retention_class not in _COLLECTABLE
            or removed.payload_ref.sha256 != tombstone.payload_sha256
            or removed.payload_ref.size_bytes != tombstone.size_bytes_unlinked
            or not removed.created_ns
            <= tombstone.unlinked_ns
            <= observed_now_ns
        ):
            blockers.add(f"invalid_collection_tombstone:{object_id}")
        else:
            collected.add(object_id)
    for object_id in objects.keys() - live - collected:
        blockers.add(f"unexplained_missing_payload:{object_id}")
    graph = {
        object_id: tuple(
            sorted({edge.target_object_id for edge in item.live_dependencies})
        )
        for object_id, item in objects.items()
        if object_id in live
    }
    blockers.update(_graph_blockers(graph))
    blockers.update(_proof_blockers(snapshot, objects, live))
    seeds = set(snapshot.root_object_ids)
    held = {
        object_id for hold in snapshot.holds for object_id in hold.object_ids
    }
    seeds.update(held)
    for hold in snapshot.holds:
        if hold.recorded_ns > observed_now_ns:
            blockers.add("clock_precedes_hold")
    for object_id in seeds:
        if object_id not in live:
            blockers.add(f"missing_protected_root:{object_id}")
    for object_id, item in objects.items():
        if item.adapter_id not in SUPPORTED_ADAPTER_IDS:
            blockers.add(f"unsupported_native_adapter:{object_id}")
        if item.retention_class is RetentionClass.RESTRICTED_PROVIDER:
            blockers.add(
                f"restricted_provider_disposition_unsupported:{object_id}"
            )
        if item.created_ns > observed_now_ns:
            blockers.add(f"clock_precedes_object:{object_id}")
        if item.retention_class not in _COLLECTABLE:
            seeds.add(object_id)
            if object_id not in live:
                blockers.add(f"missing_permanent_evidence:{object_id}")

    completions: dict[str, int] = {}
    transaction_ids: set[str] = set()
    for completion in snapshot.completions:
        if completion.transaction_id in transaction_ids:
            blockers.add(
                f"ambiguous_transaction_completion:{completion.transaction_id}"
            )
        transaction_ids.add(completion.transaction_id)
        if completion.completed_ns > observed_now_ns:
            blockers.add(
                f"clock_precedes_completion:{completion.transaction_id}"
            )
        for published_id in completion.published_object_ids:
            published = objects.get(published_id)
            if (
                published is None
                or published.transaction_id != completion.transaction_id
                or published.created_ns > completion.completed_ns
                or published.adapter_id
                not in {"dataset-catalog.v1", "histdata-arrow-cache.v1"}
                or published.retention_class
                not in {
                    RetentionClass.PUBLISHED_DERIVED,
                    RetentionClass.REPLACEABLE_CACHE,
                }
            ):
                blockers.add(f"invalid_completed_publication:{published_id}")
        for object_id in completion.scratch_object_ids:
            scratch = objects.get(object_id)
            if (
                scratch is None
                or scratch.retention_class is not RetentionClass.SCRATCH
                or scratch.adapter_id != "managed-transaction-scratch.v1"
                or scratch.transaction_id != completion.transaction_id
                or scratch.created_ns > completion.completed_ns
                or object_id in completions
            ):
                blockers.add(f"invalid_scratch_completion:{object_id}")
            completions[object_id] = completion.completed_ns
    for object_id, item in objects.items():
        if (
            item.retention_class is RetentionClass.SCRATCH
            and object_id not in completions
        ):
            blockers.add(f"incomplete_scratch_transaction:{object_id}")

    paths = _protected_paths(graph, seeds)
    decisions: list[CollectionDecisionV1] = []
    for object_id, item in sorted(objects.items()):
        witness = paths.get(object_id, ())
        if object_id in collected:
            action, reason = (
                "already_collected",
                "retained_descriptor_of_collected_payload",
            )
        elif object_id not in live:
            action, reason = "block", "unexplained_missing_payload"
        elif witness:
            action = "keep"
            reason = (
                "registered_hold"
                if object_id in held
                else (
                    "registered_root"
                    if object_id in snapshot.root_object_ids
                    else (
                        f"independent_retention:{item.retention_class.value}"
                        if item.retention_class not in _COLLECTABLE
                        else "requires_bytes_from_protected_root"
                    )
                )
            )
        elif blockers:
            action, reason = "block", "whole_store_audit_blocked"
        elif item.retention_class is RetentionClass.SCRATCH:
            matured = (
                cutoff_ns >= completions[object_id]
                and cutoff_ns - completions[object_id]
                >= snapshot.policy.scratch_ttl_ns
            )
            action = "delete" if matured else "keep"
            reason = (
                "completed_scratch_ttl_expired"
                if matured
                else "scratch_ttl_not_expired"
            )
        elif item.retention_class is RetentionClass.REPLACEABLE_CACHE:
            action, reason = "delete", "unreachable_exactly_regenerable_cache"
        else:
            # Every permanent class is seeded above; this is fail-closed if
            # future changes accidentally violate that invariant.
            action, reason = "keep", "independent_retention"
        decisions.append(
            CollectionDecisionV1(
                object_id=object_id,
                action=action,  # type: ignore[arg-type]
                reason=reason,
                protected_path=witness,
            )
        )
    return CollectionPlanV1(
        store_id=snapshot.store_id,
        snapshot_id=snapshot.artifact_id,
        policy_id=snapshot.policy.artifact_id,
        cutoff_ns=cutoff_ns,
        decisions=tuple(decisions),
        blockers=tuple(sorted(blockers)),
    )
