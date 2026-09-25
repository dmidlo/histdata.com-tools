"""Provider-bearing products use constructed synthetic roots only."""

from contextlib import nullcontext
from dataclasses import replace

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerPolicyError,
    BrokerPolicyOperation as Operation,
    BrokerPolicyDataClass as DataClass,
    BrokerPolicyStatus as Status,
    provider_native_inputs,
    provider_policy_scope,
    read_broker_policy_receipt,
    resolve_provider_subject,
    scope,
    verify_broker_policy_receipt,
)
from histdatacom.synthetic import (
    BrokerTransferConfigV1,
    SyntheticEventOrigin,
    commit_reconstruction_publication,
    estimate_reconstruction_retention,
    iter_reconstruction_event_batches,
    project_modern_reference_delivery,
    read_reconstruction_streams,
    render_broker_delivery,
    scan_reconstruction_events_polars,
    stage_reconstruction_publication,
    verify_reconstruction_publication,
)
from histdatacom.synthetic import persistence
from histdatacom.synthetic import broker_transfer
from tests.fixtures.broker_derived_policy import generated_fingerprint
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    generated_provider_scope,
    policy_context,
)
from tests.unit.test_synthetic_broker_transfer import _group_with_constraints


@pytest.fixture(scope="module")
def product_inputs():
    fingerprint = generated_fingerprint()
    run, window, group, constraints = _group_with_constraints()
    with generated_provider_scope(fingerprint):
        rendered = render_broker_delivery(
            run=run,
            window=window,
            group=group,
            fingerprint=fingerprint,
            constraints=constraints,
            selected_at_utc_ns=0,
            config=BrokerTransferConfigV1(
                strength=0.0, max_events_per_group=100
            ),
        )
    anchors = tuple(
        event
        for stream in group.streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.OBSERVED
    )
    retention = estimate_reconstruction_retention(
        run_id=run.run_id,
        primary_member_id=rendered.manifest.ensemble_member_id,
        retained_member_event_counts={
            rendered.manifest.ensemble_member_id: sum(
                len(s.events) for s in rendered.streams
            )
        },
        estimated_partition_count=len(rendered.streams),
        storage_policy=run.storage_policy,
    )
    return fingerprint, rendered, anchors, run.storage_policy, retention


def _stage(path, inputs):
    _, rendered, anchors, storage, retention = inputs
    return stage_reconstruction_publication(
        path,
        rendered,
        immutable_source_anchors=anchors,
        symbol_group_id="generated-policy-product",
        retention_plan=retention,
        storage_policy=storage,
        row_group_size=2,
    )


def _source(
    fingerprint, *, status=Status.ALLOWED, changes=(), expires_at_ns=2**63 - 1
):
    (binding,) = resolve_provider_subject(fingerprint).bindings
    return MutablePolicySource(
        policy_context(
            binding,
            status=status,
            changes=changes,
            expires_at_ns=expires_at_ns,
        )
    )


@pytest.mark.parametrize("status", [None, Status.UNKNOWN, Status.DENIED])
def test_product_staging_refuses_before_output_io(
    tmp_path, product_inputs, status
):
    fingerprint, *_ = product_inputs
    manager = (
        nullcontext()
        if status is None
        else provider_policy_scope(_source(fingerprint, status=status))
    )
    with (
        provider_native_inputs(fingerprint),
        manager,
        pytest.raises(BrokerPolicyError),
    ):
        _stage(tmp_path / "never-created", product_inputs)
    assert list(tmp_path.iterdir()) == []


def test_product_parent_registry_is_required_and_not_permission(
    tmp_path, product_inputs
):
    fingerprint, *_ = product_inputs
    with (
        generated_provider_scope(fingerprint),
        pytest.raises(ValueError, match="native inputs"),
    ):
        _stage(tmp_path / "no-parent", product_inputs)
    with provider_native_inputs(fingerprint), pytest.raises(BrokerPolicyError):
        _stage(tmp_path / "no-rights", product_inputs)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "operation", [Operation.MATERIAL_USE, Operation.RETAIN_LOCAL]
)
def test_product_specific_class_is_required(
    tmp_path, product_inputs, operation
):
    fingerprint, *_ = product_inputs
    source = _source(
        fingerprint,
        changes=((operation, DataClass.BROKER_SYNTHETIC, Status.DENIED),),
    )
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(source),
        pytest.raises(BrokerPolicyError),
    ):
        _stage(tmp_path / "refused", product_inputs)
    assert list(tmp_path.iterdir()) == []


def test_native_product_bytes_and_sidecar_pair_and_guarded_queries(
    tmp_path, product_inputs
):
    fingerprint, rendered, *_ = product_inputs
    source = _source(
        fingerprint,
        changes=tuple(
            (op, DataClass.RAW_PAYLOAD, Status.DENIED) for op in Operation
        ),
    )
    with provider_native_inputs(fingerprint), provider_policy_scope(source):
        staged = _stage(tmp_path, product_inputs)
        published = commit_reconstruction_publication(staged)
        manifest = published.manifest
        assert (
            published.manifest_path.read_bytes() == manifest.to_json().encode()
        )
        sidecar = published.manifest_path.with_name(
            "manifest.json.provider-policy.json"
        )
        verify_broker_policy_receipt(
            read_broker_policy_receipt(sidecar),
            manifest,
            published.manifest_path,
        )
        assert (
            read_reconstruction_streams(published.manifest_path)
            == rendered.streams
        )
        batches = list(
            iter_reconstruction_event_batches(
                published.manifest_path, batch_size=2
            )
        )
        assert sum(batch.num_rows for batch in batches) == manifest.event_count
        frame = scan_reconstruction_events_polars(
            published.manifest_path, columns=("event_time_ns",)
        )
        assert frame.collect().height == manifest.event_count
    # No deferred provider-file read remains in the returned in-memory frame.
    assert frame.collect().height == manifest.event_count
    with provider_native_inputs(fingerprint), pytest.raises(BrokerPolicyError):
        read_reconstruction_streams(published.manifest_path)
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        sidecar.write_bytes(b"{}")
        with pytest.raises(ValueError):
            verify_reconstruction_publication(published.manifest_path)
        with pytest.raises(ValueError):
            next(iter_reconstruction_event_batches(published.manifest_path))
        with pytest.raises(ValueError):
            scan_reconstruction_events_polars(published.manifest_path)


def test_policy_expiry_before_partition_promotion_removes_owned_scratch(
    tmp_path, monkeypatch, product_inputs
):
    fingerprint, *_ = product_inputs
    clock = [1000]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    original = persistence._fsync_file

    def expire(path):
        original(path)
        clock[0] = 1001

    monkeypatch.setattr(persistence, "_fsync_file", expire)
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(_source(fingerprint, expires_at_ns=1001)),
        pytest.raises(BrokerPolicyError),
    ):
        _stage(tmp_path, product_inputs)
    assert not list(tmp_path.rglob("*.parquet"))
    assert not list(tmp_path.rglob("*.partial"))
    assert not list(tmp_path.rglob("manifest.json"))


def test_expiry_between_batches_stops_iterator(
    tmp_path, monkeypatch, product_inputs
):
    fingerprint, *_ = product_inputs
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        published = commit_reconstruction_publication(
            _stage(tmp_path, product_inputs)
        )
    clock = [1000]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(_source(fingerprint, expires_at_ns=1001)),
    ):
        batches = iter_reconstruction_event_batches(
            published.manifest_path, batch_size=1
        )
        assert next(batches).num_rows == 1
        clock[0] = 1001
        with pytest.raises(BrokerPolicyError):
            next(batches)


def test_modern_projection_cannot_drop_declared_broker_lineage(product_inputs):
    fingerprint, *_ = product_inputs
    _, _, group, _ = _group_with_constraints()
    stream = group.streams[0]
    events = tuple(
        (
            replace(
                event, broker_profile_id=fingerprint.fingerprint_id, event_id=""
            )
            if event.origin is SyntheticEventOrigin.SYNTHETIC
            else event
        )
        for event in stream.events
    )
    broker_stream = replace(stream, events=events, stream_id="")
    # Preserve the native reconciled group type while exercising the material
    # projection seam. Its own frozen V1 metadata reader is not a rights gate.
    altered = replace(
        group, streams=(broker_stream, *group.streams[1:]), group_id=""
    )
    with pytest.raises(ValueError, match="broker provider lineage"):
        project_modern_reference_delivery(
            altered, delivery_profile_id="generic"
        )


@pytest.mark.parametrize(
    "operation,data_class",
    [
        (Operation.MATERIAL_USE, DataClass.FINGERPRINTS),
        (Operation.DERIVE, DataClass.BROKER_SYNTHETIC),
    ],
)
def test_render_refuses_before_computation(monkeypatch, operation, data_class):
    fingerprint = generated_fingerprint()
    run, window, group, constraints = _group_with_constraints()
    calls = []

    def unexpected(**kwargs):
        calls.append(kwargs)
        raise AssertionError("refused render must not compute")

    monkeypatch.setattr(broker_transfer, "_render_broker_delivery", unexpected)
    source = _source(
        fingerprint, changes=((operation, data_class, Status.DENIED),)
    )
    with provider_policy_scope(source), pytest.raises(BrokerPolicyError):
        render_broker_delivery(
            run=run,
            window=window,
            group=group,
            fingerprint=fingerprint,
            constraints=constraints,
            selected_at_utc_ns=0,
        )
    assert calls == []


def test_render_rechecks_current_policy_before_return(monkeypatch):
    fingerprint = generated_fingerprint()
    run, window, group, constraints = _group_with_constraints()
    original = broker_transfer._render_broker_delivery
    clock = [1000]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])

    def expire(**kwargs):
        result = original(**kwargs)
        clock[0] = 1001
        return result

    monkeypatch.setattr(broker_transfer, "_render_broker_delivery", expire)
    with (
        provider_policy_scope(_source(fingerprint, expires_at_ns=1001)),
        pytest.raises(BrokerPolicyError),
    ):
        render_broker_delivery(
            run=run,
            window=window,
            group=group,
            fingerprint=fingerprint,
            constraints=constraints,
            selected_at_utc_ns=0,
        )


def test_commit_requires_new_staged_receipt_pair(tmp_path, product_inputs):
    fingerprint, *_ = product_inputs
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        staged = _stage(tmp_path, product_inputs)
        sidecar = (
            staged.staging_directory / "manifest.json.provider-policy.json"
        )
        sidecar.unlink()  # Delete only this test's generated admission canary.
        with pytest.raises(FileNotFoundError):
            commit_reconstruction_publication(staged)
        assert not staged.committed_directory.exists()
        assert staged.staging_directory.is_dir()


def test_projected_batches_reject_foreign_physical_broker_lineage(
    tmp_path, product_inputs
):
    import pyarrow as pa
    import pyarrow.parquet as pq

    fingerprint, *_ = product_inputs
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        published = commit_reconstruction_publication(
            _stage(tmp_path, product_inputs)
        )
        partition = published.manifest.partitions[0]
        path = published.manifest_path.parent / partition.relative_path
        table = pq.ParquetFile(path).read()
        values = [
            (
                "foreign-broker-profile"
                if origin == SyntheticEventOrigin.SYNTHETIC.value
                else value
            )
            for origin, value in zip(
                table.column("origin").to_pylist(),
                table.column("broker_profile_id").to_pylist(),
            )
        ]
        assert "foreign-broker-profile" in values
        index = table.schema.get_field_index("broker_profile_id")
        altered = table.set_column(
            index, table.schema.field(index), pa.array(values, type=pa.string())
        )
        pq.write_table(altered, path)
        # The requested projection excludes the lineage fields. The host must
        # still check them before returning any quote rows from this batch.
        with pytest.raises(ValueError, match="provider lineage"):
            list(
                iter_reconstruction_event_batches(
                    published.manifest_path, columns=("bid",)
                )
            )
