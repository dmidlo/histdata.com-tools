"""Constructed native activity roots and fresh declared rights, no real data."""

from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path

import pytest

from histdatacom.broker_plugin_policy import (
    BrokerDerivedArtifactV1,
    BrokerPolicyArtifactReceiptV1,
    BrokerPolicyDataClass as DataClass,
    BrokerPolicyError,
    BrokerPolicyOperation as Operation,
    BrokerPolicyStatus as Status,
    decide_provider_operation,
    provider_native_inputs,
    provider_policy_scope,
    read_broker_policy_receipt,
    resolve_provider_subject,
    scope,
    verify_broker_policy_receipt,
)
from histdatacom.synthetic import (
    ActivitySliceScope,
    BrokerTransferConfigV1,
    InformationMode,
    ReconstructionActivityManifestV1,
    ReconstructionActivityPolicyV1,
    SyntheticEventOrigin,
    estimate_reconstruction_retention,
    publish_reconstruction_group,
    render_broker_delivery,
)
from histdatacom.synthetic import activity
from tests.fixtures.broker_derived_policy import generated_fingerprint
from tests.fixtures.broker_provider_policy import (
    MutablePolicySource,
    generated_provider_scope,
    policy_context,
)
from tests.unit.test_synthetic_broker_transfer import _group_with_constraints


def _source(fingerprint, *, changes=(), status=Status.ALLOWED, expires=None):
    (binding,) = resolve_provider_subject(fingerprint).bindings
    kwargs = {} if expires is None else {"expires_at_ns": expires}
    return MutablePolicySource(
        policy_context(binding, changes=changes, status=status, **kwargs)
    )


def _summary(streams, *, route="streams", policy=None):
    kwargs = dict(
        information_mode=InformationMode.EX_POST_RECONSTRUCTION,
        information_manifest_id="information-manifest:generated-activity",
        policy=policy,
    )
    if route == "events":
        return activity.summarize_reconstruction_activity(
            (event for stream in streams for event in stream.events),
            run_id=streams[0].run_id,
            ensemble_member_id=streams[0].ensemble_member_id,
            **kwargs,
        )
    return activity.summarize_reconstruction_activity_streams(streams, **kwargs)


@pytest.fixture(scope="module")
def native_activity(tmp_path_factory):
    # Native construction is explicit, not fabricated empirical qualification.
    fingerprint = generated_fingerprint()
    run, window, group, constraints = _group_with_constraints(dense=True)
    root = tmp_path_factory.mktemp("generated-activity")
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
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
                    len(stream.events) for stream in rendered.streams
                )
            },
            estimated_partition_count=len(rendered.streams),
            storage_policy=run.storage_policy,
        )
        product = publish_reconstruction_group(
            root,
            rendered,
            immutable_source_anchors=anchors,
            symbol_group_id="generated-activity-policy",
            retention_plan=retention,
            storage_policy=run.storage_policy,
        )
        manifest = _summary(rendered.streams)
    return fingerprint, rendered.streams, manifest, product


@pytest.mark.parametrize("route", ["events", "streams"])
@pytest.mark.parametrize("status", [None, Status.UNKNOWN, Status.DENIED])
def test_activity_aggregation_has_no_default_or_inherited_permission(
    native_activity, route, status
):
    fingerprint, streams, _, _ = native_activity
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
        _summary(streams, route=route)


@pytest.mark.parametrize(
    "operation", [Operation.MATERIAL_USE, Operation.DERIVE]
)
def test_activity_requires_exact_broker_output_class(
    native_activity, operation
):
    fingerprint, streams, _, _ = native_activity
    source = _source(
        fingerprint,
        changes=((operation, DataClass.BROKER_SYNTHETIC, Status.DENIED),),
    )
    with provider_native_inputs(fingerprint), provider_policy_scope(source):
        with pytest.raises(BrokerPolicyError):
            _summary(streams)


@pytest.mark.parametrize("status", [None, Status.UNKNOWN, Status.DENIED])
def test_activity_retention_refuses_before_creating_output(
    tmp_path, native_activity, status
):
    fingerprint, _, manifest, _ = native_activity
    manager = (
        nullcontext()
        if status is None
        else provider_policy_scope(
            _source(
                fingerprint,
                changes=(
                    (
                        Operation.RETAIN_LOCAL,
                        DataClass.BROKER_SYNTHETIC,
                        status,
                    ),
                ),
            )
        )
    )
    with (
        provider_native_inputs(fingerprint),
        manager,
        pytest.raises(BrokerPolicyError),
    ):
        activity.write_reconstruction_activity_manifest(
            manifest, tmp_path / "absent"
        )
    assert list(tmp_path.iterdir()) == []


def test_activity_exact_native_parent_is_not_optional(
    tmp_path, native_activity
):
    fingerprint, streams, manifest, _ = native_activity
    with generated_provider_scope(fingerprint):
        with pytest.raises(ValueError, match="native inputs"):
            _summary(streams)
        with pytest.raises(ValueError, match="native inputs"):
            activity.write_reconstruction_activity_manifest(
                manifest, tmp_path / "absent"
            )
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "mutation", ["omitted", "extra", "foreign", "duplicate"]
)
def test_activity_binding_requires_complete_exact_roots(
    native_activity, mutation
):
    fingerprint, _, manifest, _ = native_activity
    foreign = replace(
        fingerprint, adapter_id="other-generated", fingerprint_id=""
    )
    roots = {
        "omitted": (),
        "extra": (fingerprint, foreign),
        "foreign": (foreign,),
        "duplicate": (fingerprint, fingerprint),
    }[mutation]
    with pytest.raises(ValueError):
        resolve_provider_subject(BrokerDerivedArtifactV1(roots, manifest))


def test_activity_positive_bytes_receipt_and_full_class_closure(
    tmp_path, native_activity
):
    fingerprint, streams, expected, _ = native_activity
    source = _source(
        fingerprint,
        changes=tuple(
            (operation, DataClass.RAW_PAYLOAD, Status.DENIED)
            for operation in Operation
        ),
    )
    with (
        provider_policy_scope(source),
        provider_native_inputs(fingerprint),
    ):
        actual = _summary(streams)
        assert actual.to_json() == expected.to_json()
        assert {
            identity
            for item in actual.slices
            for identity in item.broker_profile_ids
        } == {fingerprint.fingerprint_id}
        ref = activity.write_reconstruction_activity_manifest(actual, tmp_path)
        assert (
            activity.write_reconstruction_activity_manifest(actual, tmp_path)
            == ref
        )
    path = Path(ref.path)
    assert path.read_bytes() == (expected.to_json() + "\n").encode()
    assert activity.read_reconstruction_activity_manifest(path) == expected
    native = BrokerDerivedArtifactV1((fingerprint,), expected)
    assert (
        DataClass.RAW_PAYLOAD
        not in resolve_provider_subject(native).data_classes
    )
    receipt = read_broker_policy_receipt(
        path.with_name(path.name + ".provider-policy.json")
    )
    verify_broker_policy_receipt(receipt, native, path)
    assert (
        DataClass.BROKER_SYNTHETIC
        in resolve_provider_subject(native).data_classes
    )
    reduced = replace(
        receipt.operation_admission.request.subject,
        data_classes=tuple(
            item
            for item in receipt.operation_admission.request.subject.data_classes
            if item is not DataClass.BROKER_SYNTHETIC
        ),
    )
    forged = replace(
        receipt,
        operation_admission=decide_provider_operation(
            receipt.operation_admission.context,
            replace(receipt.operation_admission.request, subject=reduced),
        ),
        retention_admission=decide_provider_operation(
            receipt.retention_admission.context,
            replace(receipt.retention_admission.request, subject=reduced),
        ),
    )
    forged = BrokerPolicyArtifactReceiptV1.from_json(forged.to_json())
    with pytest.raises(ValueError, match="classification"):
        verify_broker_policy_receipt(forged, native, path)


def test_committed_activity_checks_derive_before_projected_batches(
    native_activity, monkeypatch
):
    fingerprint, _, _, product = native_activity
    calls = []
    monkeypatch.setattr(
        activity,
        "iter_reconstruction_event_batches",
        lambda *a, **k: calls.append(True),
    )
    source = _source(
        fingerprint,
        changes=(
            (Operation.DERIVE, DataClass.BROKER_SYNTHETIC, Status.DENIED),
        ),
    )
    with provider_native_inputs(fingerprint), provider_policy_scope(source):
        with pytest.raises(BrokerPolicyError):
            activity.summarize_committed_reconstruction_activity(
                product.manifest_path,
                information_mode=InformationMode.EX_POST_RECONSTRUCTION,
                information_manifest_id="generated-no-derivation",
            )
    assert calls == []


def test_activity_refuses_truncated_and_projected_away_broker_parents(
    native_activity,
):
    fingerprint, streams, _, _ = native_activity
    other = replace(
        fingerprint, adapter_id="other-generated", fingerprint_id=""
    )
    stream = streams[0]
    generated = [
        i for i, event in enumerate(stream.events) if event.broker_profile_id
    ]
    assert len(generated) >= 2
    events = list(stream.events)
    events[generated[-1]] = replace(
        events[generated[-1]],
        broker_profile_id=other.fingerprint_id,
        event_id="",
    )
    changed = replace(stream, events=tuple(events), stream_id="")
    with (
        generated_provider_scope(fingerprint, other),
        provider_native_inputs(fingerprint, other),
    ):
        with pytest.raises(ValueError, match="complete broker profile"):
            _summary(
                (changed,),
                policy=ReconstructionActivityPolicyV1(max_provenance_values=1),
            )
        with pytest.raises(ValueError, match="omitted used broker parents"):
            _summary(
                streams,
                policy=ReconstructionActivityPolicyV1(
                    scopes=(ActivitySliceScope.OBSERVED,)
                ),
            )


def test_activity_rechecks_expiry_after_last_input(
    native_activity, monkeypatch
):
    fingerprint, streams, _, _ = native_activity
    clock = [10]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    source = _source(fingerprint, expires=50)

    def events():
        for stream in streams:
            yield from stream.events
        clock[0] = 50

    with provider_native_inputs(fingerprint), provider_policy_scope(source):
        with pytest.raises(BrokerPolicyError):
            activity.summarize_reconstruction_activity(
                events(),
                run_id=streams[0].run_id,
                ensemble_member_id=streams[0].ensemble_member_id,
                information_mode=InformationMode.EX_POST_RECONSTRUCTION,
                information_manifest_id="generated-expiry",
            )


def test_activity_expiry_before_promotion_leaves_no_native_output(
    tmp_path, native_activity, monkeypatch
):
    fingerprint, _, manifest, _ = native_activity
    clock = [10]
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    original = activity._activity_receipt

    def expire(*args):
        original(*args)
        clock[0] = 50

    monkeypatch.setattr(activity, "_activity_receipt", expire)
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(_source(fingerprint, expires=50)),
    ):
        with pytest.raises(BrokerPolicyError):
            activity.write_reconstruction_activity_manifest(manifest, tmp_path)
    files = tuple(tmp_path.iterdir())
    assert files
    assert all(path.name.endswith(".provider-policy.json") for path in files)


def test_opaque_activity_limitation_requires_raw_permission(
    tmp_path, native_activity
):
    fingerprint, _, manifest, _ = native_activity
    first = replace(
        manifest.slices[0],
        limitations=(
            *manifest.slices[0].limitations,
            "generated-opaque-provider-note",
        ),
        slice_id="",
    )
    altered = replace(
        manifest, slices=(first, *manifest.slices[1:]), manifest_id=""
    )
    native = BrokerDerivedArtifactV1((fingerprint,), altered)
    assert (
        DataClass.RAW_PAYLOAD in resolve_provider_subject(native).data_classes
    )
    source = _source(
        fingerprint,
        changes=(
            (Operation.RETAIN_LOCAL, DataClass.RAW_PAYLOAD, Status.DENIED),
        ),
    )
    with provider_native_inputs(fingerprint), provider_policy_scope(source):
        with pytest.raises(BrokerPolicyError):
            activity.write_reconstruction_activity_manifest(
                altered, tmp_path / "absent"
            )
    assert list(tmp_path.iterdir()) == []


def test_activity_stale_identity_refuses_before_io(tmp_path, native_activity):
    fingerprint, _, manifest, _ = native_activity
    damaged = ReconstructionActivityManifestV1.from_json(manifest.to_json())
    object.__setattr__(damaged, "run_id", "unreconciled-change")
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        with pytest.raises(ValueError):
            activity.write_reconstruction_activity_manifest(
                damaged, tmp_path / "absent"
            )
    assert list(tmp_path.iterdir()) == []


def test_activity_cannot_promise_unenforced_finite_retention(
    tmp_path, native_activity
):
    fingerprint, _, manifest, _ = native_activity
    (binding,) = resolve_provider_subject(fingerprint).bindings
    source = MutablePolicySource(
        policy_context(binding, maximum_retention_ns=100)
    )
    with provider_native_inputs(fingerprint), provider_policy_scope(source):
        with pytest.raises(BrokerPolicyError):
            activity.write_reconstruction_activity_manifest(
                manifest, tmp_path / "absent"
            )
    assert list(tmp_path.iterdir()) == []


def test_activity_rechecks_between_broker_events(native_activity, monkeypatch):
    fingerprint, streams, _, _ = native_activity
    events = tuple(
        event for event in streams[0].events if event.broker_profile_id
    )
    assert len(events) >= 2
    clock = [10]
    seen = []
    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    original = activity._ActivityAccumulator.add

    def add(accumulator, event):
        seen.append(event.event_id)
        original(accumulator, event)

    def delivered():
        yield events[0]
        clock[0] = 50
        yield events[1]

    monkeypatch.setattr(activity._ActivityAccumulator, "add", add)
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(_source(fingerprint, expires=50)),
    ):
        with pytest.raises(BrokerPolicyError):
            activity.summarize_reconstruction_activity(
                delivered(),
                run_id=streams[0].run_id,
                ensemble_member_id=streams[0].ensemble_member_id,
                information_mode=InformationMode.EX_POST_RECONSTRUCTION,
                information_manifest_id="generated-midstream-expiry",
            )
    assert seen == [events[0].event_id]


@pytest.mark.parametrize("missing", ["native", "sidecar"])
def test_activity_never_repairs_an_incomplete_policy_pair(
    tmp_path, native_activity, missing
):
    fingerprint, _, manifest, _ = native_activity
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        ref = activity.write_reconstruction_activity_manifest(
            manifest, tmp_path
        )
        path = Path(ref.path)
        sidecar = path.with_name(path.name + ".provider-policy.json")
        (path if missing == "native" else sidecar).unlink()
        before = {item.name: item.read_bytes() for item in tmp_path.iterdir()}
        with pytest.raises(ValueError, match="incomplete; no repair"):
            activity.write_reconstruction_activity_manifest(manifest, tmp_path)
        assert {
            item.name: item.read_bytes() for item in tmp_path.iterdir()
        } == before
    if missing == "sidecar":
        # Structural historical reading does not invent a past admission.
        assert activity.read_reconstruction_activity_manifest(path) == manifest


def test_activity_rejects_invalid_existing_sidecar_without_changes(
    tmp_path, native_activity
):
    fingerprint, _, manifest, _ = native_activity
    with (
        generated_provider_scope(fingerprint),
        provider_native_inputs(fingerprint),
    ):
        ref = activity.write_reconstruction_activity_manifest(
            manifest, tmp_path
        )
        path = Path(ref.path)
        sidecar = path.with_name(path.name + ".provider-policy.json")
        sidecar.write_bytes(b"{}")
        before = {item.name: item.read_bytes() for item in tmp_path.iterdir()}
        with pytest.raises(ValueError):
            activity.write_reconstruction_activity_manifest(manifest, tmp_path)
        assert {
            item.name: item.read_bytes() for item in tmp_path.iterdir()
        } == before
