"""Generated source-backed training artifacts keep exact provider rights."""

from dataclasses import replace
import hashlib
import os
from pathlib import Path

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerPolicyDataClass as DataClass,
    BrokerPolicyError,
    BrokerPolicyOperation as Operation,
    BrokerPolicyStatus as Status,
    provider_native_inputs,
    provider_policy_scope,
    read_broker_policy_receipt,
    resolve_provider_subject,
    scope,
    verify_broker_policy_receipt,
)
from histdatacom.data_quality import training_artifacts as plain
from histdatacom.data_quality import training_join_artifacts as joins
from histdatacom.data_quality import training_overlap_artifacts as overlap
from histdatacom.data_quality import training_temporal_artifacts as temporal
from histdatacom.data_quality import training_provider_policy as policy
from histdatacom.data_quality.training_contracts import (
    DAY_NS,
    TrainingConsumerMode,
    TrainingRequestV1,
    training_json,
    training_load,
)
from histdatacom.data_quality.training_join_contracts import (
    JoinFamily,
    JoinInformationMode,
    TrainingJoinBatchV1,
    TrainingJoinPlanV1,
    TrainingJoinSourceV1,
)
from histdatacom.data_quality.training_join_views import (
    materialize_training_joins,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_overlap_contracts import (
    TrainingOverlapGeometryV1,
    TrainingOverlapPlanV1,
)
from histdatacom.data_quality.training_overlap_views import (
    materialize_training_overlap,
)
from histdatacom.data_quality.training_temporal_views import (
    materialize_training_temporal,
)
from histdatacom.data_quality.training_views import materialize_training_rows
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    generated_provider_scope,
    policy_context,
)
from tests.fixtures.training_substrate_v1 import (
    BASE,
    SYMBOLS,
    observed_source,
    published_product,
    with_products,
)
from tests.fixtures.training_temporal_v1 import temporal_plan

WRITERS = (
    plain.write_training_artifact,
    joins.write_training_join_artifact,
    temporal.write_training_temporal_artifact,
    overlap.write_training_overlap_artifact,
)


def _symlink(path, target, *, directory=False):
    try:
        path.symlink_to(target, target_is_directory=directory)
    except NotImplementedError:
        pytest.skip("symlinks unsupported")
    except OSError:
        if os.name == "nt":
            pytest.skip("Windows symlink privilege unavailable")
        raise


def _read(index, path, batch):
    if index == 0:
        return plain.read_training_artifact(
            path, consumer_mode=batch.request.consumer_mode
        )
    if index == 1:
        return joins.read_training_join_artifact(
            path, information_mode=batch.plan.information_mode
        )
    if index == 2:
        return temporal.read_training_temporal_artifact(
            path, information_mode=batch.plan.information_mode
        )
    return overlap.read_training_overlap_artifact(path, allow_expost=True)


@pytest.fixture(scope="module")
def inputs(tmp_path_factory):
    root = tmp_path_factory.mktemp("generated-provider-training")
    source, version = observed_source(root / "source")
    roots = []
    product, _ = published_product(
        root / "product", version, storage_version=1, _provider_roots=roots
    )
    (fingerprint,) = roots
    source = with_products(source, product)
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        ownership = build_training_ownership(source)
        batch = materialize_training_rows(
            source,
            ownership,
            TrainingRequestV1(
                TrainingConsumerMode.DESCRIPTIVE,
                BASE,
                BASE + 3 * DAY_NS,
                SYMBOLS,
                product_manifest_id=product.manifest.manifest_id,
            ),
        )
        joined = materialize_training_joins(
            TrainingJoinPlanV1(batch, JoinInformationMode.EX_POST, (), ())
        )
        timed_plan = temporal_plan(source, ownership)
        timed = materialize_training_temporal(timed_plan)
        overlapped = materialize_training_overlap(
            TrainingOverlapPlanV1(
                source,
                ownership,
                timed_plan.split,
                TrainingOverlapGeometryV1(
                    BASE, BASE + 3 * DAY_NS, DAY_NS, DAY_NS
                ),
                native_intervals=(),
            )
        )
    return fingerprint, product, (batch, joined, timed, overlapped)


def _source(fingerprint, *, changes=(), expires_at_ns=2**63 - 1):
    (binding,) = resolve_provider_subject(fingerprint).bindings
    return MutablePolicySource(
        policy_context(binding, changes=changes, expires_at_ns=expires_at_ns)
    )


@pytest.mark.parametrize("index", range(4))
@pytest.mark.parametrize(
    "data_class", (DataClass.FINGERPRINTS, DataClass.BROKER_SYNTHETIC)
)
def test_all_training_writers_refuse_retention_before_output(
    tmp_path, inputs, index, data_class
):
    fingerprint, _, batches = inputs
    source = _source(
        fingerprint,
        changes=((Operation.RETAIN_LOCAL, data_class, Status.DENIED),),
    )
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(source),
        pytest.raises(BrokerPolicyError),
    ):
        WRITERS[index](batches[index], tmp_path / "denied")
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("index", range(4))
def test_exact_training_bytes_paired_receipts_and_tamper_refusal(
    tmp_path, inputs, index
):
    fingerprint, _, batches = inputs
    batch = batches[index]
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        path = WRITERS[index](batch, tmp_path / "published")
        assert path.read_bytes() == batch.to_json().encode()
        subject = policy.training_provider_subject(batch)
        assert subject is not None
        sidecar = path.with_name(path.name + ".provider-policy.json")
        receipt = read_broker_policy_receipt(sidecar)
        verify_broker_policy_receipt(receipt, subject, path)
        assert _read(index, path, batch) == batch
        assert WRITERS[index](batch, path.parent) == path
        assert read_broker_policy_receipt(sidecar) == receipt
        sidecar.write_bytes(b"{}")
        with pytest.raises(ValueError):
            _read(index, path, batch)
        with pytest.raises(ValueError):
            WRITERS[index](batch, path.parent)
        assert sidecar.read_bytes() == b"{}"
        assert path.read_bytes() == batch.to_json().encode()


@pytest.mark.parametrize("index", range(4))
def test_relative_training_paths_replay_without_following_receipt_leaf(
    tmp_path, inputs, index, monkeypatch
):
    fingerprint, _, batches = inputs
    batch = batches[index]
    monkeypatch.chdir(tmp_path)
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        path = WRITERS[index](batch, Path("relative"))
        assert not path.is_absolute()
        assert _read(index, path, batch) == batch
        assert WRITERS[index](batch, path.parent) == path
        sidecar = path.with_name(path.name + ".provider-policy.json")
        original = tmp_path / "retained-policy.json"
        sidecar.rename(original)
        _symlink(sidecar, original)
        with pytest.raises((ValueError, OSError)):
            _read(index, path, batch)
        with pytest.raises((ValueError, OSError)):
            WRITERS[index](batch, path.parent)
        assert sidecar.is_symlink()
        assert path.read_bytes() == batch.to_json().encode()


@pytest.mark.parametrize("index", range(4))
def test_training_parent_aliases_preserve_each_native_path_policy(
    tmp_path, inputs, index
):
    fingerprint, _, batches = inputs
    batch = batches[index]
    physical = tmp_path / "physical"
    physical.mkdir()
    alias = tmp_path / "alias"
    _symlink(alias, physical, directory=True)
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        if index == 3:
            # The overlap owner independently forbids ancestor aliases. The
            # receipt adapter must not weaken that existing native boundary.
            with pytest.raises(ValueError, match="parent.*real directory"):
                WRITERS[index](batch, alias)
            assert list(physical.iterdir()) == []
        else:
            path = WRITERS[index](batch, alias)
            assert path.parent == alias
            assert _read(index, path, batch) == batch
            assert WRITERS[index](batch, alias) == path
            assert path.read_bytes() == batch.to_json().encode()


@pytest.mark.parametrize("index", range(4))
def test_expiry_after_sidecar_refuses_native_promotion(
    tmp_path, inputs, index, monkeypatch
):
    fingerprint, _, batches = inputs
    clock = [1000]
    source = _source(fingerprint, expires_at_ns=2000)
    original = policy.write_broker_policy_receipt

    def expired_after_receipt(*args, **kwargs):
        result = original(*args, **kwargs)
        clock[0] = 2000
        return result

    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    monkeypatch.setattr(
        policy, "write_broker_policy_receipt", expired_after_receipt
    )
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(source),
        pytest.raises(BrokerPolicyError),
    ):
        WRITERS[index](batches[index], tmp_path / "interrupted")
    retained = tuple((tmp_path / "interrupted").iterdir())
    assert len(retained) == 1
    assert retained[0].name.endswith(".provider-policy.json")
    # A separately written sidecar is incomplete evidence, never completion.
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
        pytest.raises((ValueError, FileNotFoundError)),
    ):
        WRITERS[index](batches[index], tmp_path / "interrupted")
    assert tuple((tmp_path / "interrupted").iterdir()) == retained


def test_training_native_parents_cannot_be_omitted_or_relabelled(inputs):
    fingerprint, _, batches = inputs
    with provider_native_inputs(fingerprint):
        subject = policy.training_provider_subject(batches[0])
        assert subject is not None
        for forged in (
            replace(subject, products=()),
            replace(subject, fingerprints=()),
            replace(subject, products=subject.products * 2),
            replace(subject, fingerprints=subject.fingerprints * 2),
        ):
            with pytest.raises(ValueError):
                resolve_provider_subject(forged)


def test_overlap_native_bars_use_the_exact_replayed_product_overlay(inputs):
    fingerprint, product, batches = inputs
    plan = replace(batches[3].plan, native_intervals=("1d",))
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        result = materialize_training_overlap(plan)
    # Only the fingerprint is supplied by the caller. The source replay must
    # bind the exact committed product before invoking the native bar consumer.
    bars = tuple(
        training_load(text)
        for native in result.native
        for text in native.bars_json
    )
    assert bars
    assert {native.product_manifest_id for native in result.native} == {
        product.manifest.manifest_id
    }
    assert all(
        bar["source_product_manifest_id"] == product.manifest.manifest_id
        for bar in bars
    )
    assert any(
        fingerprint.fingerprint_id in bar["broker_profile_ids"] for bar in bars
    )


@pytest.mark.parametrize("index", range(4))
def test_existing_native_without_receipt_is_not_silently_upgraded(
    tmp_path, inputs, index
):
    fingerprint, _, batches = inputs
    batch = batches[index]
    data = batch.to_json().encode()
    prefix = (
        "training-batch-",
        "training-join-batch-",
        "training-temporal-batch-",
        "training-overlap-batch-",
    )[index]
    native = tmp_path / f"{prefix}{hashlib.sha256(data).hexdigest()}.json"
    native.write_bytes(data)
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        # Historical material replay is allowed under current rights, without
        # inventing evidence that the original publication was admitted.
        assert _read(index, native, batch) == batch
        with pytest.raises(
            ValueError, match="lacks its provider-policy receipt"
        ):
            WRITERS[index](batch, tmp_path)
    assert tuple(tmp_path.iterdir()) == (native,)
    assert native.read_bytes() == data


def test_derivation_permission_is_not_implied_by_material_use(inputs):
    fingerprint, _, batches = inputs
    batch = batches[0]
    source = _source(
        fingerprint,
        changes=(
            (Operation.DERIVE, DataClass.BROKER_SYNTHETIC, Status.DENIED),
        ),
    )
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(source),
        pytest.raises(BrokerPolicyError),
    ):
        materialize_training_rows(batch.source, batch.ownership, batch.request)


def test_retained_join_fingerprint_requires_retention_even_without_product(
    tmp_path, inputs
):
    fingerprint, _, _ = inputs
    from tests.fixtures.training_join_v1 import join_fixture

    generic = join_fixture(tmp_path / "generic")
    binding = TrainingJoinSourceV1(
        "broker",
        JoinFamily.BROKER,
        paths=(
            str(tmp_path / "not-read"),
            str(tmp_path / "not-read" / "manifest.json"),
        ),
        evidence_json=(training_json(fingerprint.to_dict()),),
    )
    # Permission preflight precedes even an attempted physical source replay.
    batch = TrainingJoinBatchV1(
        replace(generic, sources=(binding,), columns=()),
        tuple(
            replace(row, values=())
            for row in materialize_training_joins(generic).rows
        ),
    )
    source = _source(
        fingerprint,
        changes=(
            (Operation.RETAIN_LOCAL, DataClass.FINGERPRINTS, Status.DENIED),
        ),
    )
    with provider_policy_scope(source), pytest.raises(BrokerPolicyError):
        joins.write_training_join_artifact(batch, tmp_path / "forbidden")
    assert not (tmp_path / "forbidden").exists()
    assert not (tmp_path / "not-read").exists()
