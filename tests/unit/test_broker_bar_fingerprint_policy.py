"""Retained constructed profiles and actual generated bar parents, no fit claim."""

from dataclasses import replace
from unittest.mock import patch

import pytest

from histdatacom.broker_capture import bar_fingerprints as module
from histdatacom.broker_capture.fingerprints import (
    compare_broker_delivery_fingerprints,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyError,
    BrokerPolicyOperation as Operation,
    BrokerPolicyDataClass as DataClass,
    BrokerPolicyStatus as Status,
    provider_native_inputs,
    provider_policy_scope,
    scope,
)
from histdatacom.synthetic import bars
from histdatacom.synthetic.activity import ActivitySliceScope
from histdatacom.synthetic.bar_features import (
    BarAvailabilityDeclarationV1,
    BarAvailabilityBasis,
    BarFeaturePolicyV1,
    BarFeatureSourceV1,
    BarFeatureConsumerResultV1,
    canonical_bar_feature_json,
)
from histdatacom.synthetic.information import InformationMode
from tests.fixtures.broker_provider_policy import generated_provider_scope
from tests.unit.test_broker_bar_policy import (
    published_source as _published_source,
)
from tests.unit.test_broker_provider_policy_products import _source

MODE = InformationMode.EX_POST_RECONSTRUCTION


def _build_retained_fit(tmp_path_factory):
    fingerprint, product = _published_source.__wrapped__(tmp_path_factory)
    with (
        provider_native_inputs(fingerprint, product.manifest),
        generated_provider_scope(fingerprint),
    ):
        published = bars.publish_derived_bars(
            tmp_path_factory.mktemp("retained-broker-bar-state"),
            product.manifest_path,
            policy=bars.DerivedBarPolicyV1(
                intervals=("1m",), scopes=(ActivitySliceScope.OBSERVED,)
            ),
        )
        source = BarFeatureSourceV1(
            str(product.manifest_path), str(published.manifest_path)
        )
        policy = BarFeaturePolicyV1(
            MODE,
            intervals=("1m",),
            scopes=(ActivitySliceScope.OBSERVED,),
            feature_names=("mid_close",),
        )
        symbol = product.manifest.symbols[0].upper()
        native = source.verified_bars(symbol, policy)
        cutoff = max(bar.bar_end_ns for bar in native)
        snapshot = source.snapshot(
            symbol=symbol,
            decision_time_ns=cutoff,
            policy=policy,
            availability=tuple(
                BarAvailabilityDeclarationV1(
                    bar.bar_id,
                    bar.bar_end_ns,
                    bar.bar_end_ns,
                    BarAvailabilityBasis.DECLARED_SOURCE_CLOCK,
                    "generated fixture availability assertion, not historical proof",
                )
                for bar in native
            ),
        )
    evidence = [
        {
            "symbol": snapshot.symbol,
            "scope": cell.scope.value,
            "interval_code": cell.interval_code,
            "bar_start_ns": cell.bar_start_ns,
            "name": value.name,
            "value": value.value,
            "state": value.state.value,
            "source_bar_ids": list(value.source_bar_ids),
            "available_at_ns": value.available_at_ns,
        }
        for cell in snapshot.cells
        for value in cell.values
    ]
    assert any(item["value"] is not None for item in evidence)
    artifact = BarFeatureConsumerResultV1(
        "broker_fingerprint",
        MODE,
        (snapshot.artifact_id,),
        (policy.artifact_id,),
        canonical_bar_feature_json(
            {
                "state_schema_version": "histdatacom.causal-broker-bar-state.v1",
                "association": "verified_rendering_profile_not_simultaneous_capture",
                "delivery_fingerprint": fingerprint.to_dict(),
                "source_product_manifest_id": product.manifest.manifest_id,
                "feature_policy": policy.to_dict(),
                "bar_evidence": evidence,
                "state_summaries": module._state_summaries(evidence),
            }
        ),
    )
    return fingerprint, product, artifact, source, snapshot


@pytest.fixture(scope="module")
def retained_fit(tmp_path_factory):
    return _build_retained_fit(tmp_path_factory)


@pytest.fixture(scope="module")
def other_retained_fit(tmp_path_factory, retained_fit):
    other = replace(
        retained_fit[0], effective_end_utc_ns=2**63 - 1, fingerprint_id=""
    )
    # Change only a generated fixture input. The actual native renderer,
    # publication, bar derivation and source replay all execute again.
    with patch(
        "tests.unit.test_broker_provider_policy_products.generated_fingerprint",
        return_value=other,
    ):
        return _build_retained_fit(tmp_path_factory)


def _compare(artifact, other=None):
    return module.compare_broker_delivery_fingerprints_with_bar_state(
        artifact, artifact if other is None else other, information_mode=MODE
    )


def test_actual_parent_comparison_preserves_native_math_and_nonclaims(
    retained_fit,
    other_retained_fit,
):
    fingerprint, product, artifact = retained_fit[:3]
    other_fingerprint, other_product, other_artifact = other_retained_fit[:3]
    before = artifact.to_json()
    source = _source(
        fingerprint,
        changes=tuple(
            (op, cls, Status.DENIED)
            for op in Operation
            for cls in (DataClass.RAW_PAYLOAD, DataClass.NORMALIZED_QUOTES)
        ),
    )
    with (
        provider_native_inputs(
            fingerprint,
            product.manifest,
            other_fingerprint,
            other_product.manifest,
        ),
        provider_policy_scope(source),
    ):
        result = _compare(artifact, other_artifact)
        assert all(
            row["mean_delta"] == 0
            for row in result.result()["state_comparisons"]
        )
        assert (
            result.result()["delivery_comparison"]
            == compare_broker_delivery_fingerprints(
                fingerprint, other_fingerprint
            ).to_dict()
        )
        assert type(result).from_json(result.to_json()) == result
    assert artifact.to_json() == before
    assert not result.empirical_qualification_claim
    assert not result.historical_availability_verified


@pytest.mark.parametrize(
    "operation", [Operation.MATERIAL_USE, Operation.DERIVE]
)
@pytest.mark.parametrize("status", [Status.UNKNOWN, Status.DENIED])
def test_broker_product_right_is_required_before_summary_work(
    retained_fit, monkeypatch, operation, status
):
    fingerprint, product, artifact = retained_fit[:3]
    called = []
    monkeypatch.setattr(
        module, "_state_summaries", lambda rows: called.append(True)
    )
    source = _source(
        fingerprint, changes=((operation, DataClass.BROKER_SYNTHETIC, status),)
    )
    with (
        provider_native_inputs(fingerprint, product.manifest),
        provider_policy_scope(source),
    ):
        other = replace(
            fingerprint, effective_end_utc_ns=2**63 - 1, fingerprint_id=""
        )
        assert compare_broker_delivery_fingerprints(fingerprint, other)
        with pytest.raises(BrokerPolicyError):
            _compare(artifact)
    assert called == []


def test_missing_exact_product_is_not_filled_from_fingerprint(retained_fit):
    fingerprint, _, artifact = retained_fit[:3]
    with (
        provider_native_inputs(fingerprint),
        generated_provider_scope(fingerprint),
        pytest.raises(ValueError, match="product is absent"),
    ):
        _compare(artifact)


@pytest.mark.parametrize(
    "change",
    ["foreign_fingerprint", "unknown_fingerprint_field", "unknown_product"],
)
def test_resealed_retained_parent_mismatch_refuses(retained_fit, change):
    fingerprint, product, artifact = retained_fit[:3]
    body = artifact.result()
    if change == "foreign_fingerprint":
        other = replace(
            fingerprint, effective_end_utc_ns=2**63 - 1, fingerprint_id=""
        )
        body["delivery_fingerprint"] = other.to_dict()
    elif change == "unknown_fingerprint_field":
        body["delivery_fingerprint"]["unretained_extra"] = "not-native-evidence"
    else:
        body["source_product_manifest_id"] = (
            "reconstruction-manifest:sha256:" + "f" * 64
        )
    changed = replace(artifact, result_json=canonical_bar_feature_json(body))
    with (
        provider_native_inputs(fingerprint, product.manifest),
        generated_provider_scope(fingerprint),
        pytest.raises(ValueError),
    ):
        _compare(changed)


def test_policy_is_rechecked_after_summary_computation(
    retained_fit, monkeypatch
):
    fingerprint, product, artifact = retained_fit[:3]
    clock = [1000]
    original = module._state_summaries
    calls = []

    def expire(rows):
        result = original(rows)
        calls.append(True)
        if len(calls) == 2:
            clock[0] = 1100
        return result

    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    monkeypatch.setattr(module, "_state_summaries", expire)
    with (
        provider_native_inputs(fingerprint, product.manifest),
        provider_policy_scope(_source(fingerprint, expires_at_ns=1100)),
        pytest.raises(BrokerPolicyError),
    ):
        _compare(artifact)
    assert len(calls) == 2


@pytest.mark.parametrize("phase", ["capture_fit", "bar_summary"])
def test_source_based_fit_rechecks_rights_after_long_work(
    retained_fit, monkeypatch, phase
):
    fingerprint, _, _, source, snapshot = retained_fit
    clock = [1000]
    original = module._state_summaries
    calls = []

    def finish_capture(*args, **kwargs):
        if phase == "capture_fit":
            clock[0] = 1100
        return fingerprint

    def finish_summary(rows):
        calls.append(True)
        result = original(rows)
        if phase == "bar_summary":
            clock[0] = 1100
        return result

    monkeypatch.setattr(scope, "_now_ns", lambda: clock[0])
    monkeypatch.setattr(
        module, "fit_broker_delivery_fingerprint", finish_capture
    )
    monkeypatch.setattr(module, "_state_summaries", finish_summary)
    # Actual source/snapshot verification runs. Only long capture/summary
    # completion clocks are controlled; no test stub grants policy rights.
    with (
        provider_native_inputs(fingerprint),
        provider_policy_scope(_source(fingerprint, expires_at_ns=1100)),
        pytest.raises(BrokerPolicyError),
    ):
        module.fit_broker_delivery_fingerprint_with_bar_state(
            "unused-by-controlled-capture",
            (),
            source=source,
            snapshots=(snapshot,),
            information_mode=MODE,
        )
    assert len(calls) == (0 if phase == "capture_fit" else 1)
