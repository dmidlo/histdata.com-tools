"""Standalone installed-API typing only; declarations are not legal approvals.

No fixture imports, provider calls, file access, or policy creation occur at
module import. These functions exercise public types, not an authority bypass.
"""

from pathlib import Path

from histdatacom.broker_capture import (
    BrokerCaptureSessionV1,
    BrokerDeliveryFingerprintV1,
)
from histdatacom.broker_plugin_capabilities import BrokerCapabilityPlanV1
from histdatacom.broker_plugin_policy import (
    BrokerLegacyCaptureV1,
    BrokerLegacyRecordV1,
    BrokerPolicyArtifactReceiptV1,
    BrokerPolicyContextV1,
    BrokerPolicyDecisionV1,
    BrokerPolicyFileSourceV1,
    BrokerPolicyOperation,
    BrokerPolicyReferenceV1,
    BrokerPolicySource,
    BrokerPolicyStatus,
    BrokerProviderConfigurationV1,
    BrokerProviderOutputContractV1,
    BrokerSDKInvocationV1,
    BrokerTrainingArtifactV1,
    product_for,
    provider_native_inputs,
    provider_policy_scope,
    provider_reconstruction_inputs,
    read_broker_policy_receipt,
    read_current_provider_policy_context,
    require_provider_operation,
    verify_broker_policy_receipt,
    write_broker_policy_context,
    write_broker_policy_receipt,
)
from histdatacom.broker_plugins import BrokerConfigurationSchemaV1
from histdatacom.data_quality.training_contracts import TrainingBatchV1
from histdatacom.synthetic import ReconstructionProductManifestV1


def reviewed_request(
    plan: BrokerCapabilityPlanV1,
    schema: BrokerConfigurationSchemaV1,
    public_configuration_json: str,
    private_field_names: tuple[str, ...],
    provider_id: str,
    profile_id: str,
) -> BrokerSDKInvocationV1:
    profile = BrokerProviderConfigurationV1(
        provider_id,
        profile_id,
        schema.to_json(),
        public_configuration_json,
        private_field_names,
    )
    profile.verify_schema(schema)
    shape = BrokerProviderOutputContractV1("sdk-v1", ("quote",))
    request = BrokerSDKInvocationV1(plan, profile, shape)
    return BrokerSDKInvocationV1.from_json(request.to_json())


def legacy_request(
    session: BrokerCaptureSessionV1,
) -> BrokerLegacyCaptureV1:
    return BrokerLegacyCaptureV1(
        session,
        BrokerProviderOutputContractV1("legacy-capture-v1", ("quote",)),
    )


def file_source(review_path: Path) -> BrokerPolicySource:
    source: BrokerPolicySource = BrokerPolicyFileSourceV1(review_path)
    return source


def read_current(
    source: BrokerPolicySource,
) -> BrokerPolicyContextV1:
    with provider_policy_scope(source):
        return read_current_provider_policy_context()


def check_invocation(
    source: BrokerPolicySource, request: BrokerSDKInvocationV1
) -> BrokerPolicyDecisionV1:
    with provider_policy_scope(source):
        decision = require_provider_operation(
            request, BrokerPolicyOperation.INVOKE
        )
    status: BrokerPolicyStatus = decision.status
    assert status is BrokerPolicyStatus.ALLOWED
    return decision


def persist_review(path: Path, context: BrokerPolicyContextV1) -> None:
    write_broker_policy_context(path, context)


def check_training_parents(
    source: BrokerPolicySource,
    batch: TrainingBatchV1,
    product: ReconstructionProductManifestV1,
    fingerprint: BrokerDeliveryFingerprintV1,
) -> BrokerPolicyDecisionV1:
    # Exact roots are evidence inputs, not source replay or a policy grant.
    with provider_native_inputs(product, fingerprint):
        retained: ReconstructionProductManifestV1 = product_for(
            product.manifest_id
        )
        with provider_reconstruction_inputs(retained):
            native = BrokerTrainingArtifactV1(
                (retained,), (fingerprint,), batch
            )
            with provider_policy_scope(source):
                return require_provider_operation(
                    native, BrokerPolicyOperation.DERIVE
                )


def retain_sidecar(
    source: BrokerPolicySource,
    native: BrokerLegacyRecordV1,
    native_path: Path,
    native_bytes: bytes,
    sidecar_path: Path,
) -> BrokerPolicyArtifactReceiptV1:
    with provider_policy_scope(source):
        receipt = write_broker_policy_receipt(
            sidecar_path,
            native,
            native_artifact_name=native_path.name,
            native_file_bytes=native_bytes,
            operation=BrokerPolicyOperation.RETAIN_LOCAL,
            maximum_bytes=8 * 1024 * 1024,
        )
    restored: BrokerPolicyArtifactReceiptV1 = read_broker_policy_receipt(
        sidecar_path
    )
    verify_broker_policy_receipt(restored, native, native_path)
    reference: BrokerPolicyReferenceV1 = restored.native_file
    assert reference == receipt.native_file
    # Historical receipt verification alone grants no subsequent operation.
    return restored
