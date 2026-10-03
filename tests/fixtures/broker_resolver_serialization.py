"""Constructed wire examples only: no capture, fit, replay, rights or output.

The V2 graph has internally consistent, invented source identities, not physical
source evidence. Proposal/render examples explicitly retain REFUSED status.
"""

import hashlib
from dataclasses import replace

from histdatacom.broker_capture.contracts import (
    BROKER_CAPTURE_DATA_ARTIFACT_KIND,
    BrokerCapturePartitionManifestV1,
    BrokerCaptureSessionManifestV1,
    BrokerCaptureSessionState,
    BrokerCaptureSessionV1,
    BrokerCaptureStoragePolicyV1,
)
from histdatacom.broker_capture.fingerprint_v2 import (
    BrokerDeliveryFingerprintV2,
    BrokerFingerprintCaptureRootV1,
)
from histdatacom.broker_capture.fingerprints import _partition_hashes_digest
from histdatacom.broker_plugin_policy.bindings import (
    BrokerProviderOutputContractV1,
)
from histdatacom.broker_plugin_provenance.contracts import (
    BrokerProvenanceConformanceStatus,
    BrokerProvenanceHeaderV1,
    BrokerProvenanceNativeFamily,
    BrokerProvenanceSealV1,
    BrokerProvenanceTerminalV1,
)
from histdatacom.runtime_contracts import ArtifactRef
from histdatacom.synthetic.broker_transfer import (
    BrokerConditionedProposalV1,
    BrokerProfileSelectionV1,
    BrokerRenderedGroupV1,
    BrokerTransferConfigV1,
    BrokerTransferManifestV1,
    BrokerTransferStatus,
)
from tests.fixtures.broker_derived_policy import generated_fingerprint


def _sha(text):
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def constructed_v2():
    """A serializer-valid graph, deliberately not a qualified source fixture."""
    statistics = generated_fingerprint()
    session = BrokerCaptureSessionV1(
        statistics.adapter_id,
        statistics.adapter_version,
        statistics.adapter_config_sha256,
        statistics.protocol,
        statistics.environment_id,
        statistics.server_id,
        0,
        0,
        collector_id=statistics.collector_id,
        collector_version=statistics.collector_version,
    )
    policy = BrokerCaptureStoragePolicyV1()
    partition = BrokerCapturePartitionManifestV1(
        session.session_id,
        policy.policy_id,
        0,
        ArtifactRef(
            BROKER_CAPTURE_DATA_ARTIFACT_KIND,
            "constructed/partition.jsonl",
            8,
            "a" * 64,
            {
                "encoding": "utf-8",
                "format": "canonical-json-lines",
                "ordering": "capture_sequence",
            },
        ),
        8,
        0,
        7,
        0,
        8,
        0,
        8,
        {"quote": 8},
    )
    manifest = BrokerCaptureSessionManifestV1(
        session,
        policy,
        BrokerCaptureSessionState.COMPLETED,
        (partition,),
        8,
        {"quote": 8},
        0,
        7,
    )
    header = BrokerProvenanceHeaderV1(
        family=BrokerProvenanceNativeFamily.LEGACY_CAPTURE_V1,
        capture_id=session.session_id,
        native_header_id=session.session_id,
        native_header_sha256=_sha(session.to_json()),
        plugin_id=session.adapter_id,
        plugin_version=session.adapter_version,
        provider_id=session.adapter_id,
        feed_id=None,
        configuration_id="constructed-configuration",
        configuration_sha256=session.adapter_config_sha256,
        distribution_name=None,
        distribution_version=None,
        implementation_sha256=None,
        registration_sha256=None,
        sdk_version=None,
        event_schema_version="histdatacom.broker-capture-event.v1",
        permission_manifest_id=None,
        permission_context_id=None,
        permission_decision_id=None,
        permission_grant_id=None,
        provider_decision_id="constructed-provider-decision-not-authority",
        host_version="2.5.0",
        host_python_version="3.10.19",
        environment_id="constructed-environment",
        started_at_utc_ns=0,
        started_at_monotonic_ns=0,
        conformance_version=None,
        conformance_status=BrokerProvenanceConformanceStatus.NOT_APPLICABLE,
        conformance_receipt_id=None,
    )
    terminal = BrokerProvenanceTerminalV1(
        header.artifact_id,
        8,
        8,
        manifest.manifest_id,
        _sha(manifest.to_json()),
        "constructed-health-not-audit",
        "d" * 64,
        None,
        None,
        True,
        True,
    )
    seal = BrokerProvenanceSealV1(
        header.artifact_id,
        terminal,
        "e" * 64,
        1,
        1,
        "f" * 64,
        0,
    )
    root = BrokerFingerprintCaptureRootV1(
        manifest,
        BrokerProviderOutputContractV1("legacy-capture-v1", ("quote",)),
        header,
        seal,
    )
    decision = replace(
        statistics.eligibility_decisions[0],
        session_id=session.session_id,
        manifest_id=manifest.manifest_id,
        decision_id="",
    )
    evidence = replace(
        statistics.capture_evidence[0],
        session_id=session.session_id,
        manifest_id=manifest.manifest_id,
        eligibility_decision_id=decision.decision_id,
        partition_hashes_sha256=_partition_hashes_digest(manifest),
        evidence_id="",
    )
    return BrokerDeliveryFingerprintV2(
        replace(
            statistics,
            eligibility_decisions=(decision,),
            capture_evidence=(evidence,),
            fingerprint_id="",
        ),
        (root,),
    )


def artifacts():
    """Return exact supported types without invoking material-use APIs."""
    fingerprint = generated_fingerprint()
    private = replace(
        fingerprint,
        account_id_sha256="d" * 64,
        limitations=("constructed opaque note",),
        fingerprint_id="",
    )
    selection = BrokerProfileSelectionV1(
        fingerprint.fingerprint_id,
        fingerprint.schema_version,
        {"symbol": "EURUSD"},
        None,
        None,
        "unsupported",
        BrokerTransferStatus.REFUSED,
        0,
        0,
        None,
        None,
        {},
        {},
        ("constructed_refusal",),
    )
    config = BrokerTransferConfigV1()
    proposal = BrokerConditionedProposalV1(
        selection,
        config,
        "constructed-query",
        None,
        {"spread": 0.0002},
        {"spread": 0.0002},
    )
    manifest = BrokerTransferManifestV1(
        "constructed-run",
        "constructed-window",
        "constructed-unit",
        "constructed-member",
        "constructed-group",
        fingerprint.fingerprint_id,
        config,
        (selection,),
        BrokerTransferStatus.REFUSED,
        ("constructed_refusal",),
        "a" * 64,
        None,
        0,
        0,
        {},
        0,
        None,
        False,
        None,
        None,
        None,
        None,
    )
    rendered = BrokerRenderedGroupV1(manifest, (), (), None, None)
    return {
        "fingerprint_v1": fingerprint,
        "fingerprint_v1_private_opaque": private,
        "fingerprint_v2_constructed": constructed_v2(),
        "proposal_refused": proposal,
        "rendered_refused": rendered,
        "selection_refused": selection,
        "manifest_refused": manifest,
    }
