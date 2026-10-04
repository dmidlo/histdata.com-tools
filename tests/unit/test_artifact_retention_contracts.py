"""Synthetic passive records: roundtrip is never native or deletion authority."""

from dataclasses import replace
import hashlib

import pytest

from histdatacom.artifact_retention import contracts as c

TOKEN = "a" * 32
DIGEST = "b" * 64
OBJECT_ID = "retention-object:sha256:" + "c" * 64
PLAN_ID = "retention-collection-plan:sha256:" + "d" * 64


def descriptor(number, *, size=10, edges=()):
    return c.NativeObjectV1(
        "ascii-tick-source.v1",
        "generated-source.v1",
        f"generated-subject-{number}",
        c.ManagedPayloadRefV1(
            f"objects/{number:032x}.csv", f"{number:064x}", size
        ),
        c.RetentionClass.IMMUTABLE_SOURCE,
        tuple(
            sorted(edges, key=lambda e: (e.target_object_id, e.relation.value))
        ),
        (),
        TOKEN,
        1,
    )


def snapshot(**changes):
    item = descriptor(1)
    values = dict(
        store_id=TOKEN,
        policy=c.RetentionPolicyV1(10),
        marker_sha256=DIGEST,
        revision_sha256=DIGEST,
        descriptors=(item,),
        present_object_ids=(item.artifact_id,),
        root_object_ids=(item.artifact_id,),
        holds=(),
        completions=(),
        regenerations=(),
        blockers=(),
        tombstones=(),
    )
    values.update(changes)
    return c.StoreSnapshotV1(**values)


def records():
    item = descriptor(1)
    policy = c.RetentionPolicyV1(10)
    snap = snapshot()
    decision = c.CollectionDecisionV1(
        item.artifact_id, "keep", "synthetic", (item.artifact_id,)
    )
    plan = c.CollectionPlanV1(
        TOKEN, snap.artifact_id, policy.artifact_id, 20, (decision,), ()
    )
    return {
        "policy": policy,
        "marker": c.StoreMarkerV1(
            TOKEN, "/synthetic/store", 0, 1, 1, policy, "ready"
        ),
        "payload": item.payload_ref,
        "dependency": c.DependencyV1(
            item.artifact_id, c.DependencyRelation.SOURCE
        ),
        "object": item,
        "recipe": c.AsciiTickRecipeV1(
            item.artifact_id, "EURUSD", "202001", DIGEST, "synthetic-backend"
        ),
        "regeneration": c.CacheRegenerationV1(
            OBJECT_ID,
            item.artifact_id,
            OBJECT_ID,
            DIGEST,
            10,
            DIGEST,
            10,
            "generated-partition",
            DIGEST,
            DIGEST,
            "synthetic-backend",
        ),
        "completion": c.TransactionCompletionV1(
            TOKEN, "published", (), (item.artifact_id,), DIGEST, 2
        ),
        "hold": c.RetentionHoldV1((item.artifact_id,), "synthetic hold", 2),
        "snapshot": snap,
        "decision": decision,
        "plan": plan,
        "outcome": c.CollectionOutcomeV1(
            item.artifact_id,
            "not_attempted",
            0,
            "synthetic-journal",
            "synthetic observation",
        ),
        "receipt": c.CollectionReceiptV1(
            TOKEN, plan.artifact_id, 1, 2, "complete", (), "verified"
        ),
        "tombstone": c.CollectionTombstoneV1(
            item.artifact_id,
            PLAN_ID,
            "synthetic-journal",
            item.payload_ref.sha256,
            10,
            3,
        ),
    }


NAMES = (
    "policy",
    "marker",
    "payload",
    "dependency",
    "object",
    "recipe",
    "regeneration",
    "completion",
    "hold",
    "snapshot",
    "decision",
    "plan",
    "outcome",
    "receipt",
    "tombstone",
)


@pytest.mark.parametrize("name", NAMES)
def test_every_passive_contract_exact_roundtrip_and_content_identity(name):
    value = records()[name]
    wire = value.to_json()
    assert type(value).from_json(wire) == value
    assert type(value).from_dict(value.to_dict()) == value
    assert value.artifact_id == (
        f"retention-{value.KIND}:sha256:"
        + hashlib.sha256(wire.encode("ascii")).hexdigest()
    )
    payload = value.to_dict()
    payload["unreviewed_extra"] = None
    with pytest.raises(ValueError):
        type(value).from_dict(payload)
    with pytest.raises(ValueError):
        type(value).from_json(wire + "\n")


@pytest.mark.parametrize(
    "name,field,value",
    [
        ("policy", "scratch_ttl_ns", True),
        ("policy", "scratch_ttl_ns", -1),
        ("policy", "scratch_ttl_ns", 365 * 86_400_000_000_000 + 1),
        ("marker", "store_id", "A" * 32),
        ("marker", "absolute_path", "relative/store"),
        ("marker", "absolute_path", "/bad\npath"),
        ("marker", "device", -1),
        ("marker", "inode", 0),
        ("marker", "created_ns", 0),
        ("marker", "state", "disposed"),
        ("payload", "relative_path", "../outside.csv"),
        ("payload", "relative_path", "/objects/" + "a" * 32 + ".csv"),
        ("payload", "relative_path", "objects/" + "a" * 32 + ".zip"),
        ("payload", "relative_path", "objects\\" + "a" * 32 + ".csv"),
        ("payload", "sha256", "B" * 64),
        ("payload", "sha256", "b" * 63),
        ("payload", "size_bytes", True),
        ("payload", "size_bytes", -1),
        ("payload", "size_bytes", c.MAX_PAYLOAD_BYTES + 1),
        ("dependency", "target_object_id", DIGEST),
        ("dependency", "relation", "source"),
        ("object", "adapter_id", ""),
        ("object", "native_schema", "x" * 513),
        ("object", "retention_class", "immutable_source"),
        ("object", "transaction_id", "a" * 31),
        ("object", "created_ns", -1),
        ("object", "live_dependencies", []),
        ("object", "historical_subject_ids", ("z", "a")),
        ("object", "historical_subject_ids", ("a", "a")),
        ("recipe", "raw_source_object_id", "foreign"),
        ("recipe", "symbol", "eurusd"),
        ("recipe", "period", "202013"),
        ("recipe", "period", "2020"),
        ("recipe", "backend_id", ""),
        ("regeneration", "rebuilt_sha256", "e" * 64),
        ("regeneration", "rebuilt_size_bytes", 11),
        ("regeneration", "native_readback_sha256", "invalid"),
        ("completion", "outcome", "pending"),
        ("completion", "published_object_ids", ()),
        ("completion", "completed_ns", False),
        ("hold", "object_ids", ()),
        ("hold", "reason", ""),
        ("decision", "action", "force_delete"),
        ("decision", "reason", ""),
        ("plan", "snapshot_id", OBJECT_ID),
        ("plan", "policy_id", PLAN_ID),
        ("plan", "cutoff_ns", 0),
        ("outcome", "outcome", "deleted_probably"),
        ("outcome", "size_bytes_unlinked", 1),
        ("outcome", "journal_entry_id", ""),
        ("receipt", "plan_id", OBJECT_ID),
        ("receipt", "finished_ns", 0),
        ("receipt", "protected_replay", "failed"),
        ("tombstone", "object_id", "foreign"),
        ("tombstone", "plan_id", OBJECT_ID),
        ("tombstone", "payload_sha256", "invalid"),
        ("tombstone", "size_bytes_unlinked", -1),
        ("tombstone", "unlinked_ns", 0),
    ],
)
def test_domain_and_exact_type_rejections(name, field, value):
    with pytest.raises(ValueError):
        replace(records()[name], **{field: value})


def test_zero_ttl_and_exact_positive_limits():
    assert c.RetentionPolicyV1(0).scratch_ttl_ns == 0
    assert c.RetentionPolicyV1(365 * 86_400_000_000_000)
    assert c.ManagedPayloadRefV1("objects/" + "a" * 32 + ".scratch", DIGEST, 0)
    assert c.ManagedPayloadRefV1(
        "objects/" + "a" * 32 + ".data", DIGEST, c.MAX_PAYLOAD_BYTES
    )


def test_published_and_aborted_completion_are_distinct():
    value = records()["completion"]
    with pytest.raises(ValueError):
        replace(value, outcome="aborted")
    aborted = replace(value, outcome="aborted", published_object_ids=())
    assert c.TransactionCompletionV1.from_json(aborted.to_json()) == aborted


def test_dependency_order_and_relation_are_content_bound():
    a = c.DependencyV1(OBJECT_ID, c.DependencyRelation.SOURCE)
    b = c.DependencyV1(OBJECT_ID, c.DependencyRelation.PACKED_MEMBER)
    ordered = tuple(
        sorted((a, b), key=lambda e: (e.target_object_id, e.relation.value))
    )
    value = descriptor(1, edges=ordered)
    with pytest.raises(ValueError):
        replace(value, live_dependencies=tuple(reversed(ordered)))
    with pytest.raises(ValueError):
        replace(value, live_dependencies=(a, a))
    assert (
        descriptor(1, edges=(a,)).artifact_id
        != descriptor(1, edges=(b,)).artifact_id
    )


def test_snapshot_physical_membership_and_aggregate_payload_bound():
    first = descriptor(1, size=c.MAX_PAYLOAD_BYTES // 2 + 1)
    second = descriptor(2, size=c.MAX_PAYLOAD_BYTES // 2 + 1)
    with pytest.raises(ValueError, match="aggregate payload"):
        snapshot(
            descriptors=tuple(
                sorted((first, second), key=lambda x: x.artifact_id)
            )
        )
    second = replace(descriptor(2), payload_ref=descriptor(1).payload_ref)
    with pytest.raises(ValueError, match="duplicate physical"):
        snapshot(
            descriptors=tuple(
                sorted((descriptor(1), second), key=lambda x: x.artifact_id)
            )
        )


def test_snapshot_aggregate_edge_limit_before_planning(monkeypatch):
    monkeypatch.setattr(c, "MAX_EDGES", 3)
    edges = tuple(
        c.DependencyV1(
            "retention-object:sha256:" + f"{i:064x}",
            c.DependencyRelation.SOURCE,
        )
        for i in (1, 2)
    )
    objects = tuple(
        sorted(
            (descriptor(1, edges=edges), descriptor(2, edges=edges)),
            key=lambda x: x.artifact_id,
        )
    )
    with pytest.raises(ValueError, match="edge bound"):
        snapshot(descriptors=objects)


@pytest.mark.parametrize(
    "field,limit", [("present_object_ids", 1024), ("root_object_ids", 128)]
)
def test_real_inventory_count_bound(field, limit):
    ids = tuple("retention-object:sha256:" + f"{i:064x}" for i in range(limit))
    assert snapshot(**{field: ids})
    with pytest.raises(ValueError):
        snapshot(
            **{field: (*ids, "retention-object:sha256:" + f"{limit:064x}")}
        )


def test_snapshot_descriptors_and_evidence_require_canonical_order():
    objects = tuple(
        sorted((descriptor(1), descriptor(2)), key=lambda x: x.artifact_id)
    )
    with pytest.raises(ValueError):
        snapshot(descriptors=tuple(reversed(objects)))
    hold = records()["hold"]
    with pytest.raises(ValueError):
        snapshot(holds=(hold, hold))
    with pytest.raises(ValueError):
        snapshot(blockers=("z", "a"))


def test_blocked_plan_cannot_contain_delete_decision():
    plan = records()["plan"]
    decision = replace(plan.decisions[0], action="delete", protected_path=())
    with pytest.raises(ValueError):
        replace(plan, decisions=(decision,), blockers=("opaque",))


def test_protected_witness_has_exact_end_no_cycles_and_no_delete_authority():
    value = records()["decision"]
    for changes in (
        {"protected_path": (OBJECT_ID,)},
        {"protected_path": (value.object_id, value.object_id)},
        {"action": "delete"},
    ):
        with pytest.raises(ValueError):
            replace(value, **changes)


def test_receipt_cannot_claim_complete_after_partial_failure_or_clock_regression():
    receipt = records()["receipt"]
    with pytest.raises(ValueError):
        replace(receipt, outcomes=(records()["outcome"],))
    with pytest.raises(ValueError):
        replace(receipt, started_ns=3)
    assert replace(
        receipt,
        status="partial",
        protected_replay="not_attempted",
        outcomes=(records()["outcome"],),
    )


def test_nested_frozen_domain_tampering_rejected_before_identity():
    snap = snapshot()
    object.__setattr__(snap.policy, "scratch_ttl_ns", -1)
    with pytest.raises(ValueError):
        snap.to_json()


def test_all_ten_retention_classes_are_explicit_not_implicit_age_policy():
    assert {x.value for x in c.RetentionClass} == {
        "immutable_source",
        "release_certification",
        "reproducibility_dependency",
        "migration_predecessor",
        "published_derived",
        "restricted_provider",
        "replaceable_cache",
        "scratch",
        "quarantined_corrupt",
        "legal_policy_hold",
    }
