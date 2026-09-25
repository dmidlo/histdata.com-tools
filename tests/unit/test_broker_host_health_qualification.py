"""Generated native scientific-fit admission and retained audit references."""

import hashlib
from dataclasses import replace

import pytest

from histdatacom.broker_capture import (
    AppendOnlyBrokerCaptureWriterV1,
    BrokerDeliveryFingerprintV1,
    BrokerDeliveryIneligibleCaptureError,
    fit_broker_delivery_fingerprint,
)
from histdatacom.broker_plugin_health import BrokerHostHealthPolicyV1
from histdatacom.broker_plugin_health.qualification import (
    BrokerHostHealthQualificationV1,
    read_current_broker_health_qualification,
    write_broker_health_qualification,
)
from tests.fixtures.broker_provider_policy import (
    generated_legacy_request,
    generated_provider_scope,
    legacy_policy_inputs,
)
from tests.unit.test_broker_host_health_runtime_legacy import capture


def test_new_native_fit_retains_exact_health_and_policy_without_changing_v1(
    tmp_path,
):
    inputs = legacy_policy_inputs()
    request, result = capture(
        tmp_path, messages=inputs.messages[:7] + inputs.messages[-1:]
    )
    with generated_provider_scope(request):
        fingerprint = fit_broker_delivery_fingerprint(
            tmp_path,
            (result.manifest,),
            provider_requests=(request,),
        )
        qualification = read_current_broker_health_qualification(
            tmp_path,
            fingerprint,
            (result.manifest,),
            (request,),
        )
        assert (
            write_broker_health_qualification(
                tmp_path,
                fingerprint,
                (result.manifest,),
                (request,),
            )
            == qualification
        )
        with pytest.raises(
            ValueError, match="complete native capture inventory"
        ):
            read_current_broker_health_qualification(
                tmp_path, fingerprint, (), ()
            )
    assert type(fingerprint) is BrokerDeliveryFingerprintV1
    assert (
        BrokerDeliveryFingerprintV1.from_json(fingerprint.to_json())
        == fingerprint
    )
    assert qualification.fingerprint_id == fingerprint.fingerprint_id
    assert (
        qualification.native_fingerprint_sha256
        == hashlib.sha256(fingerprint.to_json().encode()).hexdigest()
    )
    (reference,) = qualification.captures
    assert reference.manifest_id == result.manifest.manifest_id
    assert reference.audit_id == result.audit.artifact_id
    assert reference.policy_id == result.audit.header.policy.artifact_id
    assert (
        BrokerHostHealthQualificationV1.from_json(qualification.to_json())
        == qualification
    )
    (path,) = (tmp_path / "host-health-qualifications").glob("*.json")
    substituted = replace(qualification, native_fingerprint_sha256="0" * 64)
    path.write_text(substituted.to_json())
    with generated_provider_scope(request):
        with pytest.raises(
            ValueError, match="differs from current source replay"
        ):
            read_current_broker_health_qualification(
                tmp_path, fingerprint, (result.manifest,), (request,)
            )
        with pytest.raises(
            ValueError, match="immutable health qualification conflict"
        ):
            write_broker_health_qualification(
                tmp_path, fingerprint, (result.manifest,), (request,)
            )


@pytest.mark.parametrize("evidence", ["missing", "degraded"])
def test_new_scientific_fit_refuses_missing_or_degraded_host_observations(
    tmp_path, evidence
):
    inputs = legacy_policy_inputs()
    request = generated_legacy_request(inputs.session)
    if evidence == "missing":
        with (
            generated_provider_scope(request),
            AppendOnlyBrokerCaptureWriterV1(
                tmp_path,
                session=inputs.session,
                storage_policy=inputs.storage_policy,
                provider_request=request,
            ) as writer,
        ):
            for event in inputs.events:
                writer.append(event)
        manifest = writer.manifest
    else:
        request, result = capture(
            tmp_path,
            messages=inputs.messages[:7] + inputs.messages[-1:],
            policy=BrokerHostHealthPolicyV1(max_persistence_p95_ns=1),
        )
        manifest = result.manifest
    with (
        generated_provider_scope(request),
        pytest.raises(BrokerDeliveryIneligibleCaptureError),
    ):
        fit_broker_delivery_fingerprint(
            tmp_path, (manifest,), provider_requests=(request,)
        )
    assert not (tmp_path / "host-health-qualifications").exists()
