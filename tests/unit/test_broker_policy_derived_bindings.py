"""Closed derived relationships over constructed, nonempirical native inputs."""

from dataclasses import replace

import pytest

from histdatacom.broker_plugin_policy.bindings import (
    native_provider_artifact_json,
    resolve_provider_subject,
)
from histdatacom.broker_plugin_policy.contracts import (
    BrokerPolicyDataClass as DataClass,
)
from histdatacom.broker_plugin_policy.derived import (
    BrokerDerivedArtifactV1,
    BrokerSyntheticOutputV1,
)
from histdatacom.broker_plugin_policy.native_inputs import (
    provider_native_inputs,
)
from histdatacom.synthetic.broker_transfer import (
    BrokerTransferConfigV1,
    BrokerTransferStatus,
    condition_broker_proposal,
    render_broker_delivery,
    select_broker_profile,
)
from tests.fixtures.broker_derived_policy import generated_fingerprint
from tests.fixtures.broker_provider_policy import generated_provider_scope
from tests.unit.test_broker_delivery_fingerprints import _motif_query
from tests.unit.test_synthetic_broker_transfer import _group_with_constraints


@pytest.fixture(scope="module")
def roots():
    fingerprint = generated_fingerprint()
    run, window, group, constraints = _group_with_constraints()
    with generated_provider_scope(fingerprint):
        selection = select_broker_profile(
            fingerprint, requested_condition={}, selected_at_utc_ns=0
        )
        proposal = condition_broker_proposal(
            _motif_query(spread=0.0001),
            fingerprint,
            requested_condition={},
            selected_at_utc_ns=0,
        )
        rendered = render_broker_delivery(
            run=run,
            window=window,
            group=group,
            fingerprint=fingerprint,
            constraints=constraints,
            selected_at_utc_ns=0,
            config=BrokerTransferConfigV1(strength=0, max_events_per_group=100),
            quality_period="202001",
        )
    assert rendered.status is BrokerTransferStatus.APPLIED
    return fingerprint, selection, proposal, rendered


def test_expected_shape_is_not_storable_and_carries_parent_classes():
    fingerprint = generated_fingerprint()
    expected = BrokerSyntheticOutputV1(fingerprint)
    subject = resolve_provider_subject(expected)
    assert subject.bindings == resolve_provider_subject(fingerprint).bindings
    assert set(subject.data_classes) == {
        DataClass.BROKER_SYNTHETIC,
        DataClass.FINGERPRINTS,
        DataClass.CONTENT_HASHES,
        DataClass.HEALTH,
    }
    with pytest.raises(ValueError, match="storable"):
        native_provider_artifact_json(expected)
    private = replace(
        fingerprint, account_id_sha256="d" * 64, fingerprint_id=""
    )
    assert (
        DataClass.PRIVATE_ACCOUNT
        in resolve_provider_subject(
            BrokerSyntheticOutputV1(private)
        ).data_classes
    )


@pytest.mark.parametrize("index", [1, 2, 3, 4])
def test_native_wrappers_and_registered_bare_roots_match_exact_bytes(
    roots, index
):
    fingerprint, selection, proposal, rendered = roots
    artifact = (None, selection, proposal, rendered, rendered.manifest)[index]
    wrapped = BrokerDerivedArtifactV1((fingerprint,), artifact)
    subject = resolve_provider_subject(wrapped)
    with provider_native_inputs(fingerprint):
        assert resolve_provider_subject(artifact) == subject
        assert native_provider_artifact_json(
            artifact
        ) == native_provider_artifact_json(wrapped)
    if index != 1:
        assert native_provider_artifact_json(wrapped) == artifact.to_json()
    assert (DataClass.BROKER_SYNTHETIC in subject.data_classes) == (index != 1)
    with pytest.raises(ValueError):
        resolve_provider_subject(artifact)


@pytest.mark.parametrize(
    "variant", ["missing", "extra", "duplicate", "wrong", "list"]
)
def test_exact_parent_inventory_is_required(roots, variant):
    fingerprint, selection, *_ = roots
    other = replace(fingerprint, server_id="other-server", fingerprint_id="")
    parents = {
        "missing": (),
        "extra": (fingerprint, other),
        "duplicate": (fingerprint, fingerprint),
        "wrong": (other,),
        "list": [fingerprint],
    }[variant]
    with pytest.raises(ValueError):
        resolve_provider_subject(BrokerDerivedArtifactV1(parents, selection))


@pytest.mark.parametrize(
    "variant", ["metrics", "sources", "clock", "schema", "period"]
)
def test_resealed_selection_cannot_change_parent_values(roots, variant):
    fingerprint, selection, *_ = roots
    changes = {
        "metrics": {"metrics": {"spread": 42.0}},
        "sources": {"metric_condition_ids": {"spread": "wrong-cell"}},
        "clock": {"selected_at_utc_ns": 100},
        "schema": {"fingerprint_schema_version": "wrong-schema"},
        "period": {"profile_effective_start_utc_ns": 1},
    }[variant]
    if variant == "clock":
        fingerprint = replace(
            fingerprint, effective_end_utc_ns=100, fingerprint_id=""
        )
        selection = replace(
            selection,
            fingerprint_id=fingerprint.fingerprint_id,
            profile_effective_end_utc_ns=100,
            selection_id="",
        )
    with pytest.raises(ValueError):
        tampered = replace(selection, **changes, selection_id="")
        resolve_provider_subject(
            BrokerDerivedArtifactV1((fingerprint,), tampered)
        )


def test_resealed_proposal_must_match_its_query(roots):
    fingerprint, _, proposal, _ = roots
    bad = replace(proposal, metrics_after={"spread": 0.9}, proposal_id="")
    with pytest.raises(ValueError, match="metrics differ"):
        resolve_provider_subject(BrokerDerivedArtifactV1((fingerprint,), bad))


@pytest.mark.parametrize("variant", ["output", "lineage", "actions", "quality"])
def test_resealed_manifest_cannot_contradict_retained_render(roots, variant):
    fingerprint, _, _, rendered = roots
    changes = {
        "output": {"output_content_sha256": "f" * 64},
        "lineage": {"lineage_content_sha256": "f" * 64},
        "actions": {"action_counts": {"invented": 3}},
        "quality": {"cross_instrument_quality_sha256": "f" * 64},
    }[variant]
    manifest = replace(rendered.manifest, **changes, manifest_id="")
    bad = replace(rendered, manifest=manifest)
    with pytest.raises(ValueError):
        resolve_provider_subject(BrokerDerivedArtifactV1((fingerprint,), bad))


@pytest.mark.parametrize(
    "variant",
    ["limitation", "metric", "kind", "unit", "categories", "quantile", "cell"],
)
def test_unknown_fingerprint_text_requires_raw_class(variant):
    fingerprint = generated_fingerprint()
    cell = fingerprint.cells[0]
    metric = cell.metrics[0]
    if variant == "limitation":
        fingerprint = replace(
            fingerprint,
            limitations=("opaque provider body",),
            fingerprint_id="",
        )
    elif variant == "cell":
        cell = replace(cell, limitations=("opaque provider body",), cell_id="")
        fingerprint = replace(fingerprint, cells=(cell,), fingerprint_id="")
    else:
        fields = {
            "metric": {"name": "unknown-provider-payload"},
            "kind": {"kind": "unknown"},
            "unit": {"unit": "unknown"},
            "categories": {"category_counts": {"opaque-label": 1}},
            "quantile": {"quantiles": {"unknown": 1.0}},
        }[variant]
        metric = replace(metric, **fields, metric_id="")
        cell = replace(cell, metrics=(metric,), cell_id="")
        fingerprint = replace(fingerprint, cells=(cell,), fingerprint_id="")
    assert (
        DataClass.RAW_PAYLOAD
        in resolve_provider_subject(fingerprint).data_classes
    )
    assert (
        DataClass.RAW_PAYLOAD
        in resolve_provider_subject(
            BrokerSyntheticOutputV1(fingerprint)
        ).data_classes
    )


def test_unknown_native_type_never_calls_its_serializer(roots):
    class Untrusted:
        def to_json(self):
            pytest.fail("unknown serializer must never run")

    with pytest.raises(ValueError, match="unsupported"):
        resolve_provider_subject(
            BrokerDerivedArtifactV1((roots[0],), Untrusted())
        )


def test_parent_alias_mutation_cannot_reuse_stale_native_id(roots):
    fingerprint, selection, *_ = roots
    detached = type(fingerprint).from_json(fingerprint.to_json())
    detached.cells[0].metrics[0].quantiles["q0.5"] = 99.0
    with pytest.raises(ValueError):
        resolve_provider_subject(
            BrokerDerivedArtifactV1((detached,), selection)
        )


@pytest.mark.parametrize(
    "field", ["run_id", "window_id", "output_content_sha256"]
)
def test_resealed_passing_validation_must_bind_actual_group(roots, field):
    fingerprint, _, _, rendered = roots
    changed = "e" * 64 if field == "output_content_sha256" else "other-scope"
    validation = replace(
        rendered.post_broker_validation, **{field: changed}, validation_id=""
    )
    manifest = replace(
        rendered.manifest,
        post_broker_validation_id=validation.validation_id,
        manifest_id="",
    )
    bad = replace(
        rendered, manifest=manifest, post_broker_validation=validation
    )
    with pytest.raises(ValueError, match="validation does not bind"):
        resolve_provider_subject(BrokerDerivedArtifactV1((fingerprint,), bad))


def test_expanded_wrapper_budget_precedes_native_serialization(
    roots, monkeypatch
):
    fingerprint, selection, *_ = roots
    object.__setattr__(selection, "reason_codes", ("x" * 9_000_000,))
    try:
        with pytest.raises(ValueError, match="budget|bound"):
            resolve_provider_subject(
                BrokerDerivedArtifactV1((fingerprint,), selection)
            )
    finally:
        object.__setattr__(selection, "reason_codes", ())
