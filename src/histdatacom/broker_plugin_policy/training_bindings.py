"""Exact retained training parents; no source replay or permission inference."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from .bindings import (
    _ResolvedNative,
    _composite_ref,
    _preflight_native,
    _ref,
    _resolve_native,
    _restore_native,
)
from .contracts import BrokerPolicyDataClass as DataClass
from .contracts import BrokerPolicySubjectV1

if TYPE_CHECKING:
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.data_quality.training_contracts import TrainingBatchV1
    from histdatacom.data_quality.training_join_contracts import (
        TrainingJoinBatchV1,
    )
    from histdatacom.data_quality.training_overlap_contracts import (
        TrainingOverlapBatchV1,
    )
    from histdatacom.data_quality.training_temporal_contracts import (
        TrainingTemporalBatchV1,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
        ReconstructionProductManifestV2,
        ReconstructionProductManifestV3,
    )

    TrainingNative: TypeAlias = (
        TrainingBatchV1
        | TrainingJoinBatchV1
        | TrainingTemporalBatchV1
        | TrainingOverlapBatchV1
    )
    TrainingProduct: TypeAlias = (
        ReconstructionProductManifestV1
        | ReconstructionProductManifestV2
        | ReconstructionProductManifestV3
    )


@dataclass(frozen=True, slots=True)
class BrokerTrainingArtifactV1:
    """Native batch plus its complete exact product/fingerprint inventory.

    The existing training producer still owns complete physical-source and
    numerical replay. This wrapper checks retained native relationships only.
    """

    products: tuple[TrainingProduct, ...]
    fingerprints: tuple[BrokerDeliveryFingerprintV1, ...]
    artifact: TrainingNative


def _training_parts(artifact: object) -> tuple[Any, Any, tuple[Any, ...]]:
    from histdatacom.data_quality.training_contracts import TrainingBatchV1
    from histdatacom.data_quality.training_join_contracts import (
        TrainingJoinBatchV1,
    )
    from histdatacom.data_quality.training_overlap_contracts import (
        TrainingOverlapBatchV1,
    )
    from histdatacom.data_quality.training_temporal_contracts import (
        TrainingTemporalBatchV1,
    )

    if type(artifact) is TrainingBatchV1:
        return artifact.source, artifact.ownership, ()
    if type(artifact) is TrainingJoinBatchV1:
        return (
            artifact.plan.spine.source,
            artifact.plan.spine.ownership,
            artifact.plan.sources,
        )
    if type(artifact) in (TrainingTemporalBatchV1, TrainingOverlapBatchV1):
        exact = cast(Any, artifact)
        return exact.plan.source, exact.plan.ownership, ()
    raise ValueError("unsupported exact training provider artifact")


def resolve_training_native(
    native: BrokerTrainingArtifactV1,
) -> _ResolvedNative:
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.data_quality.training_contracts import (
        TrainingBatchV1,
        training_json,
    )
    from histdatacom.data_quality.training_join_contracts import (
        JoinFamily,
        TrainingJoinBatchV1,
    )
    from histdatacom.data_quality.training_overlap_contracts import (
        TrainingOverlapBatchV1,
    )
    from histdatacom.data_quality.training_temporal_contracts import (
        TrainingTemporalBatchV1,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
        ReconstructionProductManifestV2,
        ReconstructionProductManifestV3,
    )

    if (
        type(native) is not BrokerTrainingArtifactV1
        or type(native.products) is not tuple
        or len(native.products) > 32
        or type(native.fingerprints) is not tuple
        or not 1 <= len(native.fingerprints) <= 16
    ):
        raise ValueError("training provider roots require bounded exact tuples")
    _preflight_native(native)
    artifact = _restore_native(
        native.artifact,
        {
            TrainingBatchV1,
            TrainingJoinBatchV1,
            TrainingTemporalBatchV1,
            TrainingOverlapBatchV1,
        },
    )
    source, ownership, joins = _training_parts(artifact)
    products = tuple(
        _restore_native(
            item,
            {
                ReconstructionProductManifestV1,
                ReconstructionProductManifestV2,
                ReconstructionProductManifestV3,
            },
        )
        for item in native.products
    )
    product_ids = [item.manifest_id for item in products]
    retained_roots = {
        root.upstream_id: root.content_sha256
        for root in ownership.dependency_roots
        if root.kind == "committed-reconstruction"
    }
    # Ownership includes all products, even those not projected into rows.
    if (
        len(products) != len(source.product_manifest_paths)
        or len(set(product_ids)) != len(products)
        or set(product_ids) != set(retained_roots)
    ):
        raise ValueError("training product inventory differs from ownership")
    for product in products:
        digest = hashlib.sha256(
            training_json(product.to_dict()).encode()
        ).hexdigest()
        if retained_roots[product.manifest_id] != digest:
            raise ValueError("training product bytes differ from ownership")
    fingerprints = tuple(
        _restore_native(item, {BrokerDeliveryFingerprintV1})
        for item in native.fingerprints
    )
    parents = {item.fingerprint_id: item for item in fingerprints}
    if len(parents) != len(fingerprints):
        raise ValueError("training fingerprint inventory has duplicate parents")
    required = {
        item.broker_profile_id
        for item in products
        if type(item) is ReconstructionProductManifestV1
    }
    for product in products:
        if type(product) is ReconstructionProductManifestV1 and (
            product.quality.broker_fingerprint_id != product.broker_profile_id
        ):
            raise ValueError("training product fingerprint binding differs")
    for join in joins:
        if join.family is not JoinFamily.BROKER:
            continue
        if len(join.evidence_json) != 1:
            raise ValueError("training broker join needs its exact fingerprint")
        fingerprint = BrokerDeliveryFingerprintV1.from_json(
            join.evidence_json[0]
        )
        if training_json(fingerprint.to_dict()) != join.evidence_json[0]:
            raise ValueError("training join fingerprint is not canonical")
        parent = parents.get(fingerprint.fingerprint_id)
        if parent is None or parent.to_json() != fingerprint.to_json():
            raise ValueError("training join fingerprint bytes differ")
        required.add(fingerprint.fingerprint_id)
    if set(parents) != required:
        raise ValueError(
            "training fingerprint inventory differs from native parents"
        )
    resolved = tuple(
        _resolve_native(parents[key]).subject for key in sorted(parents)
    )
    bindings = {
        binding.artifact_id: binding
        for item in resolved
        for binding in item.bindings
    }
    classes = {DataClass.FINGERPRINTS, DataClass.CONTENT_HASHES}
    for item in resolved:
        classes.update(item.data_classes)
    if any(type(item) is ReconstructionProductManifestV1 for item in products):
        classes.add(DataClass.BROKER_SYNTHETIC)
    text = cast(str, artifact.to_json())
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            _composite_ref(
                "broker-training-artifact",
                {
                    "artifact": _ref(
                        type(artifact).__name__, artifact.artifact_id, text
                    ),
                    "products": [
                        _ref(
                            type(item).__name__,
                            item.manifest_id,
                            item.to_json(),
                        )
                        for item in sorted(
                            products, key=lambda item: item.manifest_id
                        )
                    ],
                    "fingerprints": [item.native_ref for item in resolved],
                },
            ),
            tuple(bindings[key] for key in sorted(bindings)),
            tuple(sorted(classes)),
        ),
        text,
    )
