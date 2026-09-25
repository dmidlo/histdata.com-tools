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
