"""Intentional installed typing failures; this module must not be executed."""

from pathlib import Path

from histdatacom.broker_capture import (
    BrokerCaptureSessionV1,
    BrokerDeliveryFingerprintV1,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyArtifactReceiptV1,
    BrokerPolicyFileSourceV1,
    BrokerPolicySource,
    BrokerPolicyStatus,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
    BrokerTrainingArtifactV1,
    product_for,
    provider_policy_scope,
    read_broker_policy_receipt,
    require_provider_operation,
)
from histdatacom.data_quality.training_contracts import TrainingBatchV1
from histdatacom.synthetic import ReconstructionProductManifestV1


class WrongSource:
    def read_policy_context(self) -> str:
        return "a string is not a canonical typed review context"


def invalid_usage(request: BrokerSDKInvocationV1, path: Path) -> None:
    source: BrokerPolicySource = WrongSource()
    BrokerPolicyFileSourceV1("not-a-Path")
    BrokerProviderOutputContractV1("sdk-v1", ("quote",), allow_raw_hashes=1)
    with provider_policy_scope(source):
        require_provider_operation(request, "invoke")
    receipt: BrokerPolicyArtifactReceiptV1 = read_broker_policy_receipt(path)
    status: BrokerPolicyStatus = receipt.retention_admission.status.value
    native_length: str = receipt.native_file.byte_length
    assert status and native_length


def invalid_native_fields(
    product: ReconstructionProductManifestV1,
    batch: TrainingBatchV1,
    session: BrokerCaptureSessionV1,
    fingerprint: BrokerDeliveryFingerprintV1,
) -> None:
    native = BrokerTrainingArtifactV1((product,), (fingerprint,), batch)
    product_versions: int = product_for(
        product.manifest_id
    ).source.source_version_ids
    training_version: int = batch.source.dataset_version_id
    capture_clock: str = session.started_at_utc_ns
    fingerprint_support: str = native.fingerprints[
        0
    ].fit_config.min_cell_support
    assert product_versions and training_version
    assert capture_clock and fingerprint_support
