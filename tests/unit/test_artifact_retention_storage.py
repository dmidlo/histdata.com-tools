"""Real bounded managed admissions and complete inventory refusals."""

from dataclasses import replace
import hashlib
import os
import time

import pytest

from histdatacom.artifact_retention import native, storage as api
from histdatacom.artifact_retention.contracts import (
    CollectionPlanV1,
    RetentionClass,
    RetentionPolicyV1,
    StoreSnapshotV1,
)
from histdatacom.artifact_retention.lifecycle_contracts import (
    AdmissionReceiptV1,
    OperationAbortedV1,
    OperationBeginV1,
    RootRegistrationV1,
)
from histdatacom.artifact_retention.secure_fs import StoreSession

RAW = (
    b"20200102 000000000,1.100000,1.100200,0\n"
    b"20200102 000001000,1.100100,1.100300,1\n"
    b"20200102 000002000,1.100200,1.100400,2\n"
)


def _store(tmp_path, *, ttl=0):
    root = (tmp_path / "managed").resolve()
    api.create_retention_store(root, RetentionPolicyV1(ttl))
    return root


def _source(root, tmp_path):
    source = tmp_path / "source.csv"
    source.write_bytes(RAW)
    result = api.ingest_ascii_tick_source(
        root, source, symbol="EURUSD", period="202001"
    )
    assert source.read_bytes() == RAW
    return result


def _write_unjournaled(root, directory, raw):
    with StoreSession(root) as session:
        return session.write_immutable(
            f"{directory}/{hashlib.sha256(raw).hexdigest()}.json", raw
        )


def test_empty_store_complete_inspection_and_plan_stability(tmp_path):
    root = _store(tmp_path)
    before = api.inspect_retention_store(root)
    assert before.descriptors == before.blockers == ()
    cutoff = time.time_ns()
    plan = api.plan_artifact_collection(root, cutoff_ns=cutoff)
    assert plan.decisions == plan.blockers == ()
    assert api.inspect_retention_store(root).to_json() == before.to_json()
    assert api.plan_artifact_collection(root, cutoff_ns=cutoff) == plan
    assert CollectionPlanV1.from_json(plan.to_json()) == plan
    assert StoreSnapshotV1.from_json(before.to_json()) == before


def test_source_copy_is_permanent_actual_native_and_closed(tmp_path):
    root = _store(tmp_path)
    before = api.inspect_retention_store(root)
    receipt = _source(root, tmp_path)
    assert AdmissionReceiptV1.from_json(receipt.to_json()) == receipt
    snapshot = api.inspect_retention_store(root)
    assert not snapshot.blockers and len(snapshot.descriptors) == 1
    item = snapshot.descriptors[0]
    assert item.artifact_id == receipt.primary_object_id
    assert item.retention_class is RetentionClass.IMMUTABLE_SOURCE
    assert (root / item.payload_ref.relative_path).read_bytes() == RAW
    assert snapshot.revision_sha256 != before.revision_sha256
    assert api.plan_artifact_collection(root).decisions[0].action == "keep"


def test_actual_cache_recipe_proof_and_native_catalog_publication(tmp_path):
    root = _store(tmp_path)
    source = _source(root, tmp_path)
    cache = api.produce_histdata_cache(root, source.primary_object_id)
    assert (
        len(cache.output_object_ids) == 3 and len(cache.scratch_object_ids) == 1
    )
    snapshot = api.inspect_retention_store(root)
    assert len(snapshot.descriptors) == 5 and len(snapshot.regenerations) == 1
    assert not snapshot.blockers
    plan = api.plan_artifact_collection(root)
    assert {
        item.object_id for item in plan.decisions if item.action == "delete"
    } == {cache.primary_object_id, *cache.scratch_object_ids}
    catalog = api.publish_histdata_cache_catalog(
        root, (cache.primary_object_id,), dataset_id="managed-fixture"
    )
    after = api.inspect_retention_store(root)
    assert len(after.descriptors) == 7 and not after.blockers
    catalog_object = next(
        item
        for item in after.descriptors
        if item.artifact_id == catalog.primary_object_id
    )
    raw = (root / catalog_object.payload_ref.relative_path).read_bytes()
    assert b'"qualification_status":"unqualified"' in raw
    final = api.plan_artifact_collection(root)
    decision = next(
        item
        for item in final.decisions
        if item.object_id == cache.primary_object_id
    )
    assert (
        decision.action == "keep"
        and catalog.primary_object_id in decision.protected_path
    )
    assert all(
        (root / item.payload_ref.relative_path).exists()
        for item in after.descriptors
    )


def test_roots_holds_are_additive_current_and_revision_invalidating(tmp_path):
    root = _store(tmp_path)
    source = _source(root, tmp_path)
    before = api.inspect_retention_store(root)
    registered = api.register_retention_root(
        root, source.primary_object_id, reason="synthetic root"
    )
    assert RootRegistrationV1.from_json(registered.to_json()) == registered
    middle = api.inspect_retention_store(root)
    assert middle.root_object_ids == (source.primary_object_id,)
    assert before.revision_sha256 != middle.revision_sha256
    api.add_retention_hold(
        root, (source.primary_object_id,), reason="synthetic retention hold"
    )
    after = api.inspect_retention_store(root)
    assert (
        len(after.holds) == 1
        and after.root_object_ids == middle.root_object_ids
    )
    assert after.revision_sha256 != middle.revision_sha256
    with pytest.raises(ValueError, match="duplicate root"):
        api.register_retention_root(
            root, source.primary_object_id, reason="again"
        )


@pytest.mark.parametrize(
    "case",
    ["symlink", "fifo", "directory", "oversized", "malformed", "wrong_period"],
)
def test_source_refusals_before_managed_admission(tmp_path, case):
    root = _store(tmp_path)
    before = api.inspect_retention_store(root)
    path = tmp_path / "input.csv"
    period = "202001"
    if case == "symlink":
        target = tmp_path / "target.csv"
        target.write_bytes(RAW)
        path.symlink_to(target)
    elif case == "fifo":
        os.mkfifo(path)
    elif case == "directory":
        path.mkdir()
    elif case == "oversized":
        with path.open("wb") as stream:
            stream.truncate(native.MAX_ASCII_BYTES + 1)
    else:
        path.write_bytes(b"not native CSV\n" if case == "malformed" else RAW)
        if case == "wrong_period":
            period = "202002"
    with pytest.raises((ValueError, OSError)):
        api.ingest_ascii_tick_source(root, path, symbol="EURUSD", period=period)
    assert api.inspect_retention_store(root) == before


@pytest.mark.parametrize(
    "directory",
    ["recipes", "transactions", "plans", "receipts", "roots", "holds"],
)
def test_opaque_or_unjournaled_control_refuses_complete_inventory(
    tmp_path, directory
):
    root = _store(tmp_path)
    _write_unjournaled(root, directory, b'{"schema_version":"opaque.v1"}')
    with pytest.raises(ValueError):
        api.inspect_retention_store(root)


def test_unjournaled_valid_plan_is_not_authority(tmp_path):
    root = _store(tmp_path)
    with StoreSession(root) as session:
        state = api._load_state(session)
        snapshot = api._snapshot(state, api._NativeBudget(state, 0))
        plan = api._derive_plan(
            snapshot, cutoff_ns=1, observed_now_ns=time.time_ns()
        )
    _write_unjournaled(root, "plans", plan.to_json().encode("ascii"))
    with pytest.raises(ValueError, match="uncommitted"):
        api.inspect_retention_store(root)


def test_unknown_payload_blocks_all_collection(tmp_path):
    root = _store(tmp_path)
    with StoreSession(root) as session:
        session.write_immutable("objects/" + "f" * 32 + ".scratch", b"opaque")
    snapshot = api.inspect_retention_store(root)
    assert "unregistered_live_payload" in snapshot.blockers
    assert api.plan_artifact_collection(root).blockers


def test_pending_transaction_blocks_without_adopting_absence(tmp_path):
    root = _store(tmp_path)
    with StoreSession(root) as session:
        state = api._load_state(session)
        begin = OperationBeginV1(
            session.marker.store_id,
            "a" * 32,
            "ingest_ascii_source",
            (),
            (),
            None,
            api._now(state),
        )
        api._append(state, "operation_begin", begin)
    assert (
        f"pending_transaction:{'a' * 32}"
        in api.inspect_retention_store(root).blockers
    )
    assert api.plan_artifact_collection(root).blockers


def test_native_failure_before_outputs_closes_only_actual_scratch(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    source = _source(root, tmp_path)

    def refuse(*args, **kwargs):
        raise ValueError("synthetic native refusal")

    monkeypatch.setattr(native, "execute_ascii_cache_recipe", refuse)
    with pytest.raises(ValueError, match="synthetic native refusal"):
        api.produce_histdata_cache(root, source.primary_object_id)
    snapshot = api.inspect_retention_store(root)
    assert not snapshot.blockers
    assert len(snapshot.descriptors) == 2 and len(snapshot.completions) == 1
    assert snapshot.completions[0].outcome == "aborted"
    with StoreSession(root) as session:
        assert len(api._load_state(session).of_type(OperationAbortedV1)) == 1
    plan = api.plan_artifact_collection(root)
    assert [
        item.reason for item in plan.decisions if item.action == "delete"
    ] == ["completed_scratch_ttl_expired"]


@pytest.mark.parametrize(
    "case", ["native_count", "disk", "object_count", "future_cutoff"]
)
def test_resource_admission_precedes_native_work_and_effects(
    tmp_path, monkeypatch, case
):
    root = _store(tmp_path)
    source = _source(root, tmp_path)
    before = tuple(
        sorted(
            str(path.relative_to(root))
            for path in root.rglob("*")
            if path.is_file()
        )
    )

    def poison(*args, **kwargs):
        pytest.fail("native work must not start")

    monkeypatch.setattr(native, "execute_ascii_cache_recipe", poison)
    if case == "native_count":
        monkeypatch.setattr(api, "MAX_NATIVE_EXECUTIONS", 3)
    elif case == "disk":
        monkeypatch.setattr(
            api.shutil,
            "disk_usage",
            lambda path: type("Disk", (), {"free": 0})(),
        )
    elif case == "object_count":
        monkeypatch.setattr(api, "MAX_OBJECTS", 1)
    with pytest.raises(ValueError):
        if case == "future_cutoff":
            api.plan_artifact_collection(root, cutoff_ns=2**63 - 1)
        else:
            api.produce_histdata_cache(root, source.primary_object_id)
    assert before == tuple(
        sorted(
            str(path.relative_to(root))
            for path in root.rglob("*")
            if path.is_file()
        )
    )


def test_actual_workspace_shape_overrun_is_retained_and_refused(tmp_path):
    root = _store(tmp_path)
    with StoreSession(root) as session:
        state = api._load_state(session)
        budget = api._NativeBudget(state, 1)

        def malformed(path):
            path.mkdir()
            (path / "link").symlink_to(tmp_path)

        with pytest.raises(ValueError, match="workspace"):
            budget.invoke(1, malformed)
        assert budget.workspace is not None and any(budget.workspace.iterdir())


def test_outer_native_workspace_refuses_late_ancestor_marker_before_create(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    with StoreSession(root) as session:
        state = api._load_state(session)
        budget = api._NativeBudget(state, 1)
        marker = tmp_path / api.MANAGED_ARTIFACT_MARKER
        marker.write_bytes(b"malformed marker still protects this namespace")

        def poison(*args, **kwargs):
            pytest.fail(
                "outer workspace must not be created under marker ancestry"
            )

        monkeypatch.setattr(api.tempfile, "mkdtemp", poison)
        with pytest.raises(ValueError, match="managed"):
            budget.ensure_workspace()
        assert budget.workspace is None


def test_exact_contract_rejects_subclass_before_serializer(tmp_path):
    root = _store(tmp_path)

    class ForgedPolicy(RetentionPolicyV1):
        def to_json(self):
            pytest.fail("subclass serializer must not execute")

    with pytest.raises(ValueError):
        api.create_retention_store(tmp_path / "another", ForgedPolicy())
    with pytest.raises(ValueError):
        api.add_retention_hold(root, [], reason="invalid exact tuple")


def test_source_replacement_during_copy_never_completes_admission(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    source = tmp_path / "source.csv"
    source.write_bytes(RAW)
    publish = api._publish_object

    def changed(state, item, raw):
        publish(state, item, raw)
        source.write_bytes(RAW + b"invalid\n")

    monkeypatch.setattr(api, "_publish_object", changed)
    with pytest.raises(ValueError, match="external source changed"):
        api.ingest_ascii_tick_source(
            root, source, symbol="EURUSD", period="202001"
        )
    assert api.inspect_retention_store(root).blockers


def test_scratch_descriptor_clock_cannot_differ_from_exact_work(
    tmp_path, monkeypatch
):
    root = _store(tmp_path)
    source = _source(root, tmp_path)
    with StoreSession(root) as session:
        state = api._load_state(session)
        budget = api._NativeBudget(state, 1)
        begin = api._begin(
            state, "produce_histdata_cache", (source.primary_object_id,), budget
        )
        scratch = next(
            item
            for item in state.descriptors
            if item.artifact_id in begin.scratch_object_ids
        )
        altered = replace(scratch, created_ns=scratch.created_ns - 1)
        with pytest.raises(ValueError, match="scratch descriptor"):
            api._verify_scratch(state, altered)
