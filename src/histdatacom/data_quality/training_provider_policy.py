"""Fresh provider gates for exact retained training provenance, not tainting."""

from __future__ import annotations

from pathlib import Path
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING

from histdatacom.broker_capture.fingerprint_contracts import (
    BrokerDeliveryFingerprintV1,
)
from histdatacom.broker_plugin_policy import (
    BrokerPolicyOperation,
    BrokerTrainingArtifactV1,
    fingerprint_for,
    provider_reconstruction_inputs,
    read_broker_policy_receipt,
    require_provider_operation,
    resolve_provider_subject,
    verify_broker_policy_receipt,
    write_broker_policy_receipt,
)
from histdatacom.broker_plugin_policy.training_bindings import _training_parts
from histdatacom.synthetic.persistence import (
    RECONSTRUCTION_PRODUCT_SCHEMA_VERSION,
    RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION,
    RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION,
    ReconstructionProductManifestV1,
    ReconstructionProductManifestV2,
    ReconstructionProductManifestV3,
)

from .training_contracts import training_json, training_load
from .training_join_contracts import JoinFamily
from .training_lineage import _VerifiedSource, read_training_regular

if TYPE_CHECKING:
    from histdatacom.broker_plugin_policy.training_bindings import (
        TrainingNative,
        TrainingProduct,
    )


def require_training_derivation(verified: _VerifiedSource) -> None:
    """Recheck exact source products, including those omitted by projection."""
    for product in verified.products:
        if type(product.manifest) is ReconstructionProductManifestV1:
            require_provider_operation(
                product.manifest, BrokerPolicyOperation.MATERIAL_USE
            )
            require_provider_operation(
                product.manifest, BrokerPolicyOperation.DERIVE
            )


def training_reconstruction_inputs(
    verified: _VerifiedSource,
) -> AbstractContextManager[None]:
    """Bind actual already-replayed product metadata for ID-only descendants."""
    products = tuple(
        item.manifest
        for item in verified.products
        if type(item.manifest) is ReconstructionProductManifestV1
    )
    return (
        provider_reconstruction_inputs(*products) if products else nullcontext()
    )


def training_provider_subject(
    batch: TrainingNative,
) -> BrokerTrainingArtifactV1 | None:
    """Bind metadata-only native roots; caller must still execute source replay.

    All paths are explicit in the native source inventory. No filesystem search,
    provider selection, policy discovery, or permissive root substitution occurs.
    """
    source, _, joins = _training_parts(batch)
    products: list[TrainingProduct] = []
    fingerprints: dict[str, BrokerDeliveryFingerprintV1] = {}
    for name in source.product_manifest_paths:
        payload = training_load(read_training_regular(Path(name)).decode())
        version = payload.get("schema_version")
        product: TrainingProduct
        if version == RECONSTRUCTION_PRODUCT_SCHEMA_VERSION:
            product = ReconstructionProductManifestV1.from_dict(payload)
            fingerprint = fingerprint_for(product.broker_profile_id)
            fingerprints[fingerprint.fingerprint_id] = fingerprint
        elif version == RECONSTRUCTION_PRODUCT_V2_SCHEMA_VERSION:
            product = ReconstructionProductManifestV2.from_dict(payload)
        elif version == RECONSTRUCTION_PRODUCT_V3_SCHEMA_VERSION:
            product = ReconstructionProductManifestV3.from_dict(payload)
        else:
            raise ValueError("unsupported native training product schema")
        if training_json(product.to_dict()) != training_json(payload):
            raise ValueError("training product metadata is not canonical")
        products.append(product)
    for join in joins:
        if join.family is not JoinFamily.BROKER:
            continue
        if len(join.evidence_json) != 1:
            raise ValueError(
                "training broker join needs one native fingerprint"
            )
        fingerprint = BrokerDeliveryFingerprintV1.from_json(
            join.evidence_json[0]
        )
        if training_json(fingerprint.to_dict()) != join.evidence_json[0]:
            raise ValueError("training broker fingerprint is not canonical")
        previous = fingerprints.get(fingerprint.fingerprint_id)
        if previous is not None and previous.to_json() != fingerprint.to_json():
            raise ValueError(
                "training fingerprint identity has conflicting bytes"
            )
        fingerprints[fingerprint.fingerprint_id] = fingerprint
    if not fingerprints:
        return None
    subject = BrokerTrainingArtifactV1(
        tuple(products),
        tuple(fingerprints[key] for key in sorted(fingerprints)),
        batch,
    )
    resolve_provider_subject(subject)
    return subject


def require_training_retention(
    subject: BrokerTrainingArtifactV1 | None,
) -> None:
    if subject is not None:
        require_provider_operation(subject, BrokerPolicyOperation.RETAIN_LOCAL)


def verify_training_policy_receipt(
    subject: BrokerTrainingArtifactV1 | None,
    target: Path,
    *,
    required: bool = False,
) -> None:
    if subject is None:
        return
    # Preserve the native writer's path spelling while policy storage binds a
    # canonical parent. Never resolve the native or receipt leaf through a link.
    target = target.parent.resolve(strict=True) / target.name
    sidecar = target.with_name(target.name + ".provider-policy.json")
    if sidecar.exists() or sidecar.is_symlink():
        verify_broker_policy_receipt(
            read_broker_policy_receipt(sidecar), subject, target
        )
    elif required:
        raise ValueError(
            "new training artifact requires its provider-policy receipt"
        )


def publish_training_policy_receipt(
    subject: BrokerTrainingArtifactV1 | None, target: Path, data: bytes
) -> None:
    """Fresh admission, exact native bytes, no repair of an orphaned pair."""
    if subject is None:
        return
    require_training_retention(subject)
    target = target.parent.resolve(strict=True) / target.name
    sidecar = target.with_name(target.name + ".provider-policy.json")
    if target.exists() or target.is_symlink():
        if read_training_regular(target) != data:
            raise ValueError(
                "existing training artifact differs from canonical bytes"
            )
        if not (sidecar.exists() or sidecar.is_symlink()):
            raise ValueError(
                "existing training artifact lacks its provider-policy receipt"
            )
    if sidecar.exists() or sidecar.is_symlink():
        # An orphan is incomplete evidence, not an idempotent successful write.
        verify_training_policy_receipt(subject, target, required=True)
    else:
        write_broker_policy_receipt(
            sidecar,
            subject,
            native_artifact_name=target.name,
            native_file_bytes=data,
        )
