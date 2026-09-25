"""Actual generated broker products, never provider permission or market data."""

from dataclasses import replace

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerPolicyError,
    BrokerPolicyOperation as Operation,
    BrokerPolicyDataClass as DataClass,
    BrokerPolicyStatus as Status,
    provider_native_inputs,
    provider_reconstruction_inputs,
    product_for,
    provider_policy_scope,
    resolve_provider_subject,
    read_broker_policy_receipt,
    verify_broker_policy_receipt,
    scope,
)
from histdatacom.broker_plugin_policy import native_inputs
from histdatacom.synthetic import bars
from histdatacom.synthetic.bar_features import (
    BarFeatureSourceV1,
    BarFeaturePolicyV1,
)
from histdatacom.synthetic.persistence import commit_reconstruction_publication
from histdatacom.synthetic.information import InformationMode
from histdatacom.synthetic.bar_hierarchy import (
    aggregate_qualified_bar_projection,
)
from tests.fixtures.broker_provider_policy import generated_provider_scope
from tests.unit.test_broker_provider_policy_products import (
    product_inputs as _product_inputs,
    _source,
    _stage,
)
from tests.unit.test_synthetic_bars import _boundary_stream


@pytest.fixture(scope="module")
def published_source(tmp_path_factory):
    product_inputs = _product_inputs.__wrapped__()
    fingerprint, *_ = product_inputs
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        product = commit_reconstruction_publication(
            _stage(tmp_path_factory.mktemp("broker-bar-source"), product_inputs)
        )
    return fingerprint, product


def _bar_stage(root, product):
    return bars.stage_derived_bar_publication(
        root,
        product.manifest_path,
        policy=bars.DerivedBarPolicyV1(
            intervals=("1m",), scopes=("observed", "synthetic", "merged")
        ),
        row_group_size=2,
        write_buffer_rows=1,
    )


@pytest.mark.parametrize(
    "operation", [Operation.DERIVE, Operation.RETAIN_LOCAL]
)
@pytest.mark.parametrize("status", [Status.UNKNOWN, Status.DENIED])
def test_bar_staging_requires_own_operation_before_scratch(
    tmp_path, published_source, operation, status
):
    fingerprint, product = published_source
    source = _source(
        fingerprint, changes=((operation, DataClass.BROKER_SYNTHETIC, status),)
    )
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(source),
        pytest.raises(BrokerPolicyError),
    ):
        _bar_stage(tmp_path / "no-output", product)
    assert list(tmp_path.iterdir()) == []


def test_bar_parent_overlay_is_bounded_detached_and_not_authority(
    published_source, monkeypatch
):
    fingerprint, product = published_source
    manifest = product.manifest
    with provider_native_inputs(fingerprint):
        with pytest.raises(ValueError, match="absent"):
            product_for(manifest.manifest_id)
        with provider_reconstruction_inputs(manifest):
            restored = product_for(manifest.manifest_id)
            assert restored.to_json() == manifest.to_json()
            assert restored is not manifest
            with provider_reconstruction_inputs(manifest):
                assert product_for(manifest.manifest_id) == manifest
            with pytest.raises(BrokerPolicyError):
                from histdatacom.broker_plugin_policy import (
                    require_provider_operation,
                )

                require_provider_operation(restored, Operation.MATERIAL_USE)
            with pytest.raises(ValueError, match="nested"):
                with provider_native_inputs(fingerprint):
                    pass
        with pytest.raises(ValueError, match="absent"):
            product_for(manifest.manifest_id)
        monkeypatch.setattr(native_inputs, "MAX_BYTES", 1)
        with pytest.raises(ValueError, match="aggregate byte"):
            with provider_reconstruction_inputs(manifest):
                pass


def test_overlay_rejects_foreign_pid_and_stale_product(
    published_source, monkeypatch
):
    fingerprint, product = published_source
    with pytest.raises(ValueError, match="1..128"):
        with provider_reconstruction_inputs(*((product.manifest,) * 129)):
            pass
    stale = type(product.manifest).from_json(product.manifest.to_json())
    object.__setattr__(
        stale, "manifest_id", "reconstruction-manifest:sha256:" + "f" * 64
    )
    with pytest.raises(ValueError):
        with provider_reconstruction_inputs(stale):
            pass
    with provider_native_inputs(fingerprint):
        monkeypatch.setattr(native_inputs.os, "getpid", lambda: -1)
        with pytest.raises(ValueError, match="another process"):
            with provider_reconstruction_inputs(product.manifest):
                pass


def test_bar_native_roundtrip_sidecar_and_current_read_admission(
    tmp_path, published_source
):
    fingerprint, source_product = published_source
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        staged = _bar_stage(tmp_path, source_product)
        with pytest.raises(ValueError, match="product is absent"):
            bars.commit_derived_bar_publication(staged)
        with provider_reconstruction_inputs(source_product.manifest):
            published = bars.commit_derived_bar_publication(staged)
            assert (
                published.manifest_path.read_bytes()
                == published.manifest.to_json().encode()
            )
            receipt = published.manifest_path.with_name(
                "manifest.json.provider-policy.json"
            )
            verify_broker_policy_receipt(
                read_broker_policy_receipt(receipt),
                published.manifest,
                published.manifest_path,
            )
            assert (
                bars.verify_derived_bar_publication(published.manifest_path)
                == published.manifest
            )
            frame = bars.scan_derived_bars_polars(
                published.manifest_path, columns=("bar_id", "mid_close")
            )
            assert frame.explain().startswith("DF ")
            assert "Parquet SCAN" not in frame.explain()
            assert frame.collect().height == published.manifest.bar_count
            assert bars.commit_derived_bar_publication(staged).idempotent_retry
        with pytest.raises(ValueError, match="product is absent"):
            bars.verify_derived_bar_publication(published.manifest_path)
    assert frame.collect().height == published.manifest.bar_count
    with (
        provider_native_inputs(fingerprint, source_product.manifest),
        pytest.raises(BrokerPolicyError),
    ):
        bars.verify_derived_bar_publication(published.manifest_path)
    with (
        provider_native_inputs(fingerprint, source_product.manifest),
        generated_provider_scope(fingerprint),
    ):
        receipt.write_bytes(b"{}")
        with pytest.raises(ValueError):
            bars.verify_derived_bar_publication(published.manifest_path)


def test_bar_commit_expiry_does_not_promote(
    tmp_path, published_source, monkeypatch
):
    fingerprint, product = published_source
    clock = [1000]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    with (
        provider_native_inputs(fingerprint, product.manifest),
        provider_policy_scope(_source(fingerprint, expires_at_ns=1100)),
    ):
        staged = _bar_stage(tmp_path, product)
        clock[0] = 1100
        with pytest.raises(BrokerPolicyError):
            bars.commit_derived_bar_publication(staged)
        assert not staged.committed_directory.exists()
        assert staged.staging_directory.exists()


def test_suspended_bar_batches_recheck_policy_before_next_read(
    tmp_path, published_source, monkeypatch
):
    fingerprint, product = published_source
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        published = bars.publish_derived_bars(
            tmp_path,
            product.manifest_path,
            policy=bars.DerivedBarPolicyV1(intervals=("1m",)),
        )
    clock = [1000]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    with (
        provider_native_inputs(fingerprint, product.manifest),
        provider_policy_scope(_source(fingerprint, expires_at_ns=1100)),
    ):
        iterator = bars.iter_derived_bar_batches(
            published.manifest_path, batch_size=1
        )
        assert next(iterator).num_rows == 1
        clock[0] = 1100
        with pytest.raises(BrokerPolicyError):
            next(iterator)


def test_bar_feature_derive_refuses_before_source_replay(
    tmp_path, published_source, monkeypatch
):
    fingerprint, product = published_source
    called = []
    monkeypatch.setattr(
        BarFeatureSourceV1,
        "_verified_evidence_bound",
        lambda *a: called.append(True),
    )
    source = BarFeatureSourceV1(
        str(product.manifest_path), str(tmp_path / "not-opened")
    )
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(
            _source(
                fingerprint,
                changes=(
                    (
                        Operation.DERIVE,
                        DataClass.BROKER_SYNTHETIC,
                        Status.DENIED,
                    ),
                ),
            )
        ),
        pytest.raises(BrokerPolicyError),
    ):
        source.verified_bars(
            "EURUSD", BarFeaturePolicyV1(InformationMode.EX_POST_RECONSTRUCTION)
        )
    assert called == []


def test_bar_features_positive_all_missing_retains_exact_parent(
    tmp_path, published_source
):
    fingerprint, product = published_source
    policy = BarFeaturePolicyV1(InformationMode.EX_POST_RECONSTRUCTION)
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        published = bars.publish_derived_bars(
            tmp_path,
            product.manifest_path,
            policy=bars.DerivedBarPolicyV1(intervals=("1m",)),
        )
        source = BarFeatureSourceV1(
            str(product.manifest_path), str(published.manifest_path)
        )
        snapshot = source.snapshot(
            symbol=product.manifest.symbols[0].upper(),
            decision_time_ns=product.manifest.max_event_time_ns + 1,
            policy=policy,
        )
        assert (
            snapshot.source_product_manifest_id == product.manifest.manifest_id
        )
        assert snapshot.bars == ()
        source.verify_snapshot(
            snapshot, information_mode=policy.information_mode
        )


@pytest.mark.parametrize(
    "operation", [Operation.MATERIAL_USE, Operation.DERIVE]
)
def test_low_level_native_broker_events_require_exact_parent_and_operation(
    published_source, operation
):
    from histdatacom.synthetic.persistence import read_reconstruction_streams

    fingerprint, product = published_source
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        streams = read_reconstruction_streams(product.manifest_path)
        event = next(
            event
            for stream in streams
            for event in stream.events
            if event.broker_profile_id
        )
    source = _source(
        fingerprint,
        changes=((operation, DataClass.BROKER_SYNTHETIC, Status.DENIED),),
    )
    with (
        provider_native_inputs(fingerprint, product.manifest),
        provider_policy_scope(source),
        pytest.raises(BrokerPolicyError),
    ):
        bars.derive_reconstruction_bars(
            (event,),
            source_product_manifest_id=product.manifest.manifest_id,
            run_id=event.run_id,
            ensemble_member_id=event.ensemble_member_id,
        )


def test_native_bar_parent_axes_cannot_be_resealed(tmp_path, published_source):
    fingerprint, product = published_source
    with (
        provider_native_inputs(fingerprint, product.manifest),
        generated_provider_scope(fingerprint),
    ):
        staged = _bar_stage(tmp_path, product)
        changed = replace(
            staged.manifest,
            source_product_logical_sha256="f" * 64,
            manifest_id="",
        )
        with pytest.raises(ValueError, match="product parent"):
            resolve_provider_subject(changed)


def test_nonbroker_native_bars_remain_pure_and_ungated():
    stream = _boundary_stream()
    events = tuple(
        replace(event, broker_profile_id=None, event_id="")
        for event in stream.events
    )
    assert bars.derive_reconstruction_bars(
        events,
        source_product_manifest_id="generated-generic-source",
        run_id=stream.run_id,
        ensemble_member_id=stream.ensemble_member_id,
        policy=bars.DerivedBarPolicyV1(intervals=("1m",)),
    )


def test_fingerprint_synthetic_bars_do_not_require_raw_or_provider_quotes(
    tmp_path, published_source
):
    fingerprint, product = published_source
    source = _source(
        fingerprint,
        changes=tuple(
            (op, data_class, Status.DENIED)
            for op in Operation
            for data_class in (
                DataClass.RAW_PAYLOAD,
                DataClass.NORMALIZED_QUOTES,
            )
        ),
    )
    with provider_native_inputs(fingerprint), provider_policy_scope(source):
        published = bars.publish_derived_bars(
            tmp_path,
            product.manifest_path,
            policy=bars.DerivedBarPolicyV1(intervals=("1m",)),
        )
        with provider_reconstruction_inputs(product.manifest):
            subject = resolve_provider_subject(published.manifest)
            assert DataClass.BROKER_SYNTHETIC in subject.data_classes
            assert DataClass.NORMALIZED_QUOTES not in subject.data_classes
            assert (
                bars.verify_derived_bar_publication(published.manifest_path)
                == published.manifest
            )


def test_broker_hierarchy_requires_current_derive_rights(
    tmp_path, published_source
):
    fingerprint, product = published_source
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
    ):
        published = bars.publish_derived_bars(
            tmp_path,
            product.manifest_path,
            policy=bars.DerivedBarPolicyV1(intervals=("1m",)),
        )
        with provider_reconstruction_inputs(product.manifest):
            row = next(
                bars.iter_derived_bar_batches(
                    published.manifest_path, batch_size=1
                )
            ).to_pylist()[0]
            child = bars.DerivedBarV1.from_dict(row)
            expected = aggregate_qualified_bar_projection(
                (child,),
                interval_code="1m",
                bar_start_ns=child.bar_start_ns,
                policy=published.manifest.policy,
            )
            assert expected["event_count"] == child.event_count
    with (
        provider_native_inputs(fingerprint, product.manifest),
        provider_policy_scope(
            _source(
                fingerprint,
                changes=(
                    (
                        Operation.DERIVE,
                        DataClass.BROKER_SYNTHETIC,
                        Status.DENIED,
                    ),
                ),
            )
        ),
        pytest.raises(BrokerPolicyError),
    ):
        aggregate_qualified_bar_projection(
            (child,),
            interval_code="1m",
            bar_start_ns=child.bar_start_ns,
            policy=published.manifest.policy,
        )


def test_generic_v2_publication_and_pushed_bar_scans_remain_ungated(tmp_path):
    from histdatacom.synthetic.persistence import (
        discover_reconstruction_manifests,
    )
    from tests.unit.test_synthetic_persistence import (
        test_generic_delivery_commit_recovers_after_atomic_rename,
    )

    # Reuse the genuine V2 producer fixture, never relabel a broker manifest.
    test_generic_delivery_commit_recovers_after_atomic_rename(tmp_path)
    (source_path,) = discover_reconstruction_manifests(tmp_path / "archive")
    published = bars.publish_derived_bars(
        tmp_path / "generic-bars",
        source_path,
        policy=bars.DerivedBarPolicyV1(intervals=("1m",)),
    )
    assert not published.manifest_path.with_name(
        "manifest.json.provider-policy.json"
    ).exists()
    frame = bars.scan_derived_bars_polars(
        published.manifest_path, columns=("bar_start_ns", "mid_close")
    )
    assert "Parquet SCAN" in frame.explain()
    assert frame.collect().height == published.manifest.bar_count
