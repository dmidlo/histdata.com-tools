"""Small constructed native profiles, never empirical fit evidence or rights."""

from histdatacom.broker_capture.fingerprint_contracts import (
    BrokerCaptureEligibilityStatus,
    BrokerCaptureEligibilityV1,
    BrokerDeliveryCaptureEvidenceV1,
    BrokerDeliveryCellV1,
    BrokerDeliveryConditionV1,
    BrokerDeliveryFingerprintV1,
    BrokerDeliveryFitConfigV1,
    BrokerDeliveryMetricV1,
    BrokerDeliverySupportStatus,
)


def generated_fingerprint() -> BrokerDeliveryFingerprintV1:
    """Return a valid in-memory global profile; no capture or fit is executed."""
    config = BrokerDeliveryFitConfigV1()
    decision = BrokerCaptureEligibilityV1(
        session_id="generated-session",
        manifest_id="generated-manifest",
        config_id=config.config_id,
        status=BrokerCaptureEligibilityStatus.ELIGIBLE,
        reason_codes=(),
        manifest_complete=True,
        inspection_clean=True,
        integrity_verified=True,
        event_count=8,
        quote_count=8,
        clock_correction_count=0,
        max_abs_clock_correction_ns=0,
        explained_wall_regression_count=0,
        unexplained_wall_regression_count=0,
        first_receive_time_utc_ns=0,
        last_receive_time_utc_ns=8,
        logical_content_sha256="a" * 64,
    )
    evidence = BrokerDeliveryCaptureEvidenceV1(
        decision.session_id,
        decision.manifest_id,
        decision.decision_id,
        "a" * 64,
        "b" * 64,
        1,
        8,
        0,
        8,
    )
    condition = BrokerDeliveryConditionV1({})
    metric = BrokerDeliveryMetricV1(
        "spread", "distribution", "price", 8, 8, 0.0002, 0.0002, 0.0002
    )
    cell = BrokerDeliveryCellV1(
        condition,
        8,
        BrokerDeliverySupportStatus.SUPPORTED,
        (),
        condition.condition_id,
        (metric,),
    )
    return BrokerDeliveryFingerprintV1(
        adapter_id="generated-broker",
        adapter_version="1.0.0",
        adapter_config_sha256="c" * 64,
        protocol="generated",
        environment_id="generated-environment",
        server_id="generated-server",
        account_id_sha256=None,
        collector_id="histdatacom",
        collector_version="1.0.0",
        fit_config=config,
        capture_evidence=(evidence,),
        eligibility_decisions=(decision,),
        support_start_utc_ns=0,
        support_end_utc_ns=8,
        effective_start_utc_ns=0,
        effective_end_utc_ns=None,
        cells=(cell,),
    )


def generated_qualified_fingerprint(root):
    """Execute a tiny synthetic host capture and real V2 fit, never a fake seal."""
    from dataclasses import replace

    from histdatacom.broker_capture import (
        SequenceBrokerCaptureAdapterV1,
        fit_broker_delivery_fingerprint,
    )
    from histdatacom.broker_plugin_health.runtime_legacy import (
        capture_legacy_with_host_health,
    )
    from histdatacom.broker_plugin_policy import (
        BrokerLegacyCaptureV1,
        BrokerProviderOutputContractV1,
    )
    from tests.fixtures.broker_host_health import SyntheticHostHealthClock
    from tests.fixtures.broker_provider_policy import (
        generated_provider_scope,
        legacy_policy_inputs,
    )

    inputs = legacy_policy_inputs()
    messages = inputs.messages[:7] + inputs.messages[-1:]
    request = BrokerLegacyCaptureV1(
        inputs.session,
        BrokerProviderOutputContractV1(
            "legacy-capture-v1",
            tuple(sorted({item.kind.value for item in messages})),
            allow_raw_hashes=False,
            allow_opaque_metadata=False,
            allow_private_account_metadata=False,
        ),
    )
    with generated_provider_scope(request):
        result = capture_legacy_with_host_health(
            root,
            provider_request=request,
            adapter=SequenceBrokerCaptureAdapterV1(
                inputs.session.adapter_id,
                inputs.session.adapter_version,
                messages,
            ),
            clock=SyntheticHostHealthClock(inputs.session),
            storage_policy=replace(
                inputs.storage_policy,
                max_partition_events=32,
                fsync_each_event=True,
                policy_id="",
            ),
            symbols=("EURUSD",),
        )
        return fit_broker_delivery_fingerprint(
            root,
            (result.manifest,),
            provider_requests=(request,),
            effective_start_utc_ns=0,
        )
