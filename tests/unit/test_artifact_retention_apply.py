"""Actual controlled unlink, protected replay and interrupted journal evidence."""

from dataclasses import replace
import importlib
import time

import pytest

from histdatacom.artifact_retention import storage
from histdatacom.artifact_retention.contracts import (
    CollectionOutcomeV1,
    CollectionReceiptV1,
    RetentionPolicyV1,
)
from histdatacom.artifact_retention.lifecycle_contracts import (
    ApplyStartedV1,
    CollectionInterruptedEvidenceV1,
    PayloadObservationV1,
    UnlinkIntentV1,
    UnlinkObservedV1,
)
from histdatacom.artifact_retention.secure_fs import StoreSession

api = importlib.import_module("histdatacom.artifact_retention.apply")
RAW = (
    b"20200102 000000000,1.100000,1.100200,0\n"
    b"20200102 000001000,1.100100,1.100300,1\n"
    b"20200102 000002000,1.100200,1.100400,2\n"
)


def _store(tmp_path):
    root = (tmp_path / "managed").resolve()
    storage.create_retention_store(root, RetentionPolicyV1(0))
    return root


def _cache(root, tmp_path):
    path = tmp_path / "source.csv"
    path.write_bytes(RAW)
    source = storage.ingest_ascii_tick_source(
        root, path, symbol="EURUSD", period="202001"
    )
    return source, storage.produce_histdata_cache(
        root, source.primary_object_id
    )


def _scratch_only(root, tmp_path, monkeypatch):
    path = tmp_path / "source.csv"
    path.write_bytes(RAW)
    source = storage.ingest_ascii_tick_source(
        root, path, symbol="EURUSD", period="202001"
    )

    def refuse(*args, **kwargs):
        raise ValueError("fixed native refusal")

    with monkeypatch.context() as patch:
        patch.setattr(storage.native, "execute_ascii_cache_recipe", refuse)
        with pytest.raises(ValueError, match="fixed native refusal"):
            storage.produce_histdata_cache(root, source.primary_object_id)
    plan = storage.plan_artifact_collection(root)
    assert sum(item.action == "delete" for item in plan.decisions) == 1
    return source, plan


def test_empty_plan_actual_apply_and_no_implicit_repeat(tmp_path):
    root = _store(tmp_path)
    plan = storage.plan_artifact_collection(root)
    receipt = api.apply_artifact_collection(root, plan)
    assert receipt.status == "complete" and receipt.outcomes == ()
    assert receipt.protected_replay == "verified"
    assert CollectionReceiptV1.from_json(receipt.to_json()) == receipt
    assert not storage.inspect_retention_store(root).blockers
    with pytest.raises(ValueError, match="already exists"):
        api.apply_artifact_collection(root, plan)


def test_post_receipt_publication_failure_retains_write_not_acceptance(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    plan = storage.plan_artifact_collection(root)
    check = api._unchanged_prefix

    def fail_after_actual_receipt(state):
        check(state)
        if state.of_type(CollectionReceiptV1):
            raise OSError("fixed post-publication validation failure")

    monkeypatch.setattr(api, "_unchanged_prefix", fail_after_actual_receipt)
    with pytest.raises(api.CollectionInterruptedError) as raised:
        api.apply_artifact_collection(root, plan)
    error = raised.value
    assert error.receipt is None and error.receipt_persisted
    assert error.unaccepted_receipt_id is not None
    evidence = CollectionInterruptedEvidenceV1.from_dict(error.to_dict())
    assert evidence.status == "interrupted" and evidence.receipt is None
    assert error.schema_version == error.to_dict()["schema_version"]
    assert evidence.unaccepted_receipt_id == error.unaccepted_receipt_id
    with StoreSession(root) as session:
        state = storage._load_state(session)
        retained = state.of_type(CollectionReceiptV1)
        assert len(retained) == 1
        assert retained[0].artifact_id == evidence.unaccepted_receipt_id
        assert retained[0].status == "complete"
        assert (
            state.entry_for(retained[0].artifact_id).event
            == "collection_receipt"
        )


def test_real_two_cache_catalog_collects_only_orphan_and_completed_scratch(
    tmp_path,
):
    root = _store(tmp_path)
    source, orphan = _cache(root, tmp_path)
    protected = storage.produce_histdata_cache(root, source.primary_object_id)
    catalog = storage.publish_histdata_cache_catalog(
        root, (protected.primary_object_id,), dataset_id="retained-fixture"
    )
    storage.register_retention_root(
        root, catalog.primary_object_id, reason="actual native catalog root"
    )
    before = storage.inspect_retention_store(root)
    objects = {item.artifact_id: item for item in before.descriptors}
    expected = {
        orphan.primary_object_id,
        *orphan.scratch_object_ids,
        *protected.scratch_object_ids,
        *catalog.scratch_object_ids,
    }
    before_bytes = {
        identity: (root / item.payload_ref.relative_path).read_bytes()
        for identity, item in objects.items()
    }
    plan = storage.plan_artifact_collection(root)
    assert {
        item.object_id for item in plan.decisions if item.action == "delete"
    } == expected
    receipt = api.apply_artifact_collection(root, plan)
    assert (
        receipt.status == "complete" and receipt.protected_replay == "verified"
    )
    assert {item.object_id for item in receipt.outcomes} == expected
    assert all(item.outcome == "unlinked" for item in receipt.outcomes)
    assert sum(item.size_bytes_unlinked for item in receipt.outcomes) == sum(
        len(before_bytes[identity]) for identity in expected
    )
    after = storage.inspect_retention_store(root)
    assert not after.blockers and len(after.tombstones) == 4
    assert after.descriptors == before.descriptors
    assert (
        len(after.regenerations) == 2
    )  # Includes actual replay of deleted output.
    for identity, item in objects.items():
        path = root / item.payload_ref.relative_path
        if identity in expected:
            assert not path.exists()
        else:
            assert path.read_bytes() == before_bytes[identity]
    next_plan = storage.plan_artifact_collection(root)
    assert not next_plan.blockers
    assert {
        item.object_id
        for item in next_plan.decisions
        if item.action == "already_collected"
    } == expected
    assert not any(item.action == "delete" for item in next_plan.decisions)
    assert (tmp_path / "source.csv").read_bytes() == RAW


@pytest.mark.parametrize("mutation", ["root", "hold", "admission"])
def test_actual_state_change_invalidates_retained_plan_before_unlink(
    tmp_path, monkeypatch, mutation
):
    root = _store(tmp_path)
    source, plan = _scratch_only(root, tmp_path, monkeypatch)
    if mutation == "root":
        storage.register_retention_root(
            root, source.primary_object_id, reason="new root"
        )
    elif mutation == "hold":
        storage.add_retention_hold(
            root, (source.primary_object_id,), reason="new hold"
        )
    else:
        storage.ingest_ascii_tick_source(
            root, tmp_path / "source.csv", symbol="EURUSD", period="202001"
        )
    called = []
    monkeypatch.setattr(
        StoreSession, "_unlink_payload", lambda *args: called.append(True)
    )
    with pytest.raises(ValueError, match="stale"):
        api.apply_artifact_collection(root, plan)
    assert not called


def test_current_payload_corruption_blocks_without_unlink(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    source, plan = _scratch_only(root, tmp_path, monkeypatch)
    snapshot = storage.inspect_retention_store(root)
    item = next(
        item
        for item in snapshot.descriptors
        if item.artifact_id == source.primary_object_id
    )
    path = root / item.payload_ref.relative_path
    path.chmod(0o600)
    path.write_bytes(RAW.replace(b"1.100000", b"1.900000"))
    called = []
    monkeypatch.setattr(
        StoreSession, "_unlink_payload", lambda *args: called.append(True)
    )
    with pytest.raises(ValueError):
        api.apply_artifact_collection(root, plan)
    assert not called


@pytest.mark.parametrize("after_unlink", [False, True])
def test_actual_private_unlink_failure_is_indeterminate_never_inferred_success(
    tmp_path, monkeypatch, after_unlink
):
    root = _store(tmp_path)
    _, plan = _scratch_only(root, tmp_path, monkeypatch)
    unlink = StoreSession._unlink_payload

    def fail(session, observation):
        if after_unlink:
            unlink(session, observation)
        raise OSError("fixed fsync-like failure")

    monkeypatch.setattr(StoreSession, "_unlink_payload", fail)
    with pytest.raises(api.CollectionInterruptedError) as raised:
        api.apply_artifact_collection(root, plan)
    error = raised.value
    assert error.receipt_persisted and error.receipt.status == "indeterminate"
    assert error.outcomes[0].outcome == "indeterminate"
    assert error.outcomes[0].size_bytes_unlinked == 0
    evidence = CollectionInterruptedEvidenceV1.from_dict(error.to_dict())
    assert (
        evidence.receipt == error.receipt and evidence.status == "interrupted"
    )
    snapshot = storage.inspect_retention_store(root)
    assert not snapshot.tombstones and snapshot.blockers
    target = next(
        item
        for item in snapshot.descriptors
        if item.artifact_id == error.outcomes[0].object_id
    )
    assert (
        root / target.payload_ref.relative_path
    ).exists() is not after_unlink
    with pytest.raises(ValueError, match="already exists"):
        api.apply_artifact_collection(root, plan)


@pytest.mark.parametrize("fault", ["post_unlink", "prefix_change"])
def test_two_candidates_stop_after_first_effect_and_preserve_full_denominator(
    tmp_path, monkeypatch, fault
):
    root = _store(tmp_path)
    source, _ = _scratch_only(root, tmp_path, monkeypatch)

    def refuse(*args, **kwargs):
        raise ValueError("second fixed native refusal")

    with monkeypatch.context() as patch:
        patch.setattr(storage.native, "execute_ascii_cache_recipe", refuse)
        with pytest.raises(ValueError, match="second fixed native refusal"):
            storage.produce_histdata_cache(root, source.primary_object_id)
    snapshot = storage.inspect_retention_store(root)
    objects = {item.artifact_id: item for item in snapshot.descriptors}
    plan = storage.plan_artifact_collection(root)
    candidates = tuple(
        item.object_id for item in plan.decisions if item.action == "delete"
    )
    assert len(candidates) == 2
    called = []
    unlink = StoreSession._unlink_payload
    append = api._append

    def observed_unlink(session, observation):
        called.append(observation.relative_path)
        unlink(session, observation)
        if fault == "post_unlink":
            raise OSError("fixed failure after first actual effect")

    def prefix_change(state, event, record):
        entry = append(state, event, record)
        if event == "tombstone" and fault == "prefix_change":
            # A real extra file appears outside the attempt's exact expected
            # prefix. It must be detected before the second unlink call.
            state.session.write_immutable(
                "objects/" + "e" * 32 + ".scratch",
                b"unregistered concurrent member",
            )
        return entry

    monkeypatch.setattr(StoreSession, "_unlink_payload", observed_unlink)
    monkeypatch.setattr(api, "_append", prefix_change)
    with pytest.raises(api.CollectionInterruptedError) as raised:
        api.apply_artifact_collection(root, plan)
    error = raised.value
    assert called == [objects[candidates[0]].payload_ref.relative_path]
    assert tuple(item.object_id for item in error.outcomes) == candidates
    assert error.outcomes[0].outcome == (
        "indeterminate" if fault == "post_unlink" else "unlinked"
    )
    assert error.outcomes[1].outcome == "not_attempted"
    assert error.outcomes[1].size_bytes_unlinked == 0
    assert not (
        root / objects[candidates[0]].payload_ref.relative_path
    ).exists()
    assert (root / objects[candidates[1]].payload_ref.relative_path).exists()
    after = storage.inspect_retention_store(root)
    assert after.blockers
    assert len(after.tombstones) == (0 if fault == "post_unlink" else 1)
    with pytest.raises(ValueError, match="already exists"):
        api.apply_artifact_collection(root, plan)


def test_clock_backstep_after_unlink_keeps_raw_time_and_no_normal_receipt(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    _, plan = _scratch_only(root, tmp_path, monkeypatch)
    unlink = StoreSession._unlink_payload

    def regress(session, observation):
        unlink(session, observation)
        monkeypatch.setattr(time, "time_ns", lambda: 1)

    monkeypatch.setattr(StoreSession, "_unlink_payload", regress)
    with pytest.raises(api.CollectionInterruptedError) as raised:
        api.apply_artifact_collection(root, plan)
    error = raised.value
    assert error.observed_time_ns == 1
    assert error.receipt is None and not error.receipt_persisted
    assert error.outcomes[0].outcome == "indeterminate"
    assert (
        CollectionInterruptedEvidenceV1.from_dict(
            error.to_dict()
        ).observed_time_ns
        == 1
    )


@pytest.mark.parametrize(
    "event",
    ["unlink_intent", "unlink_observed", "tombstone", "collection_receipt"],
)
def test_journal_publication_failure_preserves_partial_evidence(
    tmp_path, monkeypatch, event
):
    root = _store(tmp_path)
    _, plan = _scratch_only(root, tmp_path, monkeypatch)
    append = api._append

    def fail(state, actual_event, record):
        if actual_event == event:
            raise OSError("fixed publication failure")
        return append(state, actual_event, record)

    monkeypatch.setattr(api, "_append", fail)
    with pytest.raises(api.CollectionInterruptedError) as raised:
        api.apply_artifact_collection(root, plan)
    error = raised.value
    assert not error.receipt_persisted
    assert error.to_dict()["status"] == "interrupted"
    assert storage.inspect_retention_store(root).blockers


def _started(root, plan):
    session = StoreSession(root)
    session.__enter__()
    state = storage._load_state(session)
    ids = tuple(
        item.object_id for item in plan.decisions if item.action == "delete"
    )
    start = ApplyStartedV1(
        session.marker.store_id,
        plan.artifact_id,
        plan.snapshot_id,
        ids,
        storage._now(state),
    )
    entry = storage._append(state, "apply_started", start)
    return session, state, start, entry


def test_forged_complete_receipt_without_intent_is_refused(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    _, plan = _scratch_only(root, tmp_path, monkeypatch)
    session, state, start, entry = _started(root, plan)
    try:
        outcome = CollectionOutcomeV1(
            start.candidate_object_ids[0],
            "unlinked",
            1,
            entry.artifact_id,
            "forged",
        )
        receipt = CollectionReceiptV1(
            session.marker.store_id,
            plan.artifact_id,
            start.started_ns,
            storage._now(state),
            "complete",
            (outcome,),
            "verified",
        )
        storage._append(state, "collection_receipt", receipt)
    finally:
        session.__exit__(None, None, None)
    with pytest.raises(ValueError, match="durable intent"):
        storage.inspect_retention_store(root)


def test_duplicate_intents_and_misbound_indeterminate_observation_refuse(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    _, plan = _scratch_only(root, tmp_path, monkeypatch)
    session, state, start, _ = _started(root, plan)
    try:
        item = next(
            item
            for item in state.descriptors
            if item.artifact_id == start.candidate_object_ids[0]
        )
        observed = state.observations[item.payload_ref.relative_path]
        observation = PayloadObservationV1(
            observed.relative_path,
            observed.sha256,
            observed.size_bytes,
            observed.device,
            observed.inode,
            observed.mode,
            observed.mtime_ns,
            observed.ctime_ns,
        )
        intent = UnlinkIntentV1(
            session.marker.store_id,
            plan.artifact_id,
            item.artifact_id,
            observation,
            storage._now(state),
        )
        storage._append(state, "unlink_intent", intent)
        wrong = UnlinkObservedV1(
            session.marker.store_id,
            plan.artifact_id,
            "retention-object:sha256:" + "f" * 64,
            intent.artifact_id,
            "indeterminate",
            0,
            storage._now(state),
            "wrong exact subject",
        )
        storage._append(state, "unlink_observed", wrong)
        with pytest.raises(ValueError, match="differs from durable intent"):
            storage._lifecycle_blockers(state)
        duplicate = replace(intent, started_ns=storage._now(state))
        storage._append(state, "unlink_intent", duplicate)
        with pytest.raises(ValueError, match="multiple intents"):
            storage._lifecycle_blockers(state)
    finally:
        session.__exit__(None, None, None)


def test_future_record_time_is_refused_even_when_hash_chain_is_valid(tmp_path):
    root = _store(tmp_path)
    with StoreSession(root) as session:
        state = storage._load_state(session)
        from histdatacom.artifact_retention.lifecycle_contracts import (
            OperationBeginV1,
        )

        begin = OperationBeginV1(
            session.marker.store_id,
            "a" * 32,
            "ingest_ascii_source",
            (),
            (),
            None,
            2**63 - 1,
        )
        storage._append(state, "operation_begin", begin)
    with pytest.raises(ValueError, match="record time"):
        storage.inspect_retention_store(root)


def test_caller_resealed_plan_cannot_omit_decision_or_replace_policy(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    _, plan = _scratch_only(root, tmp_path, monkeypatch)
    altered = replace(plan, decisions=())
    with pytest.raises(ValueError, match="not persisted"):
        api.apply_artifact_collection(root, altered)


def test_missing_payload_without_completed_observation_never_becomes_tombstone(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    _, plan = _scratch_only(root, tmp_path, monkeypatch)
    session, state, start, _ = _started(root, plan)
    try:
        item = next(
            item
            for item in state.descriptors
            if item.artifact_id == start.candidate_object_ids[0]
        )
        session._unlink_payload(
            state.observations[item.payload_ref.relative_path]
        )
    finally:
        session.__exit__(None, None, None)
    snapshot = storage.inspect_retention_store(root)
    assert not snapshot.tombstones and snapshot.blockers
    next_plan = storage.plan_artifact_collection(root)
    assert any(
        "unexplained_missing_payload" in value for value in next_plan.blockers
    )
    assert not any(item.action == "delete" for item in next_plan.decisions)
