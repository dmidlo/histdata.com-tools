"""Mandatory source provenance for new broker science, not metadata review.

This module stays stdlib-only at import. SDK workers never load scientific
packages merely to check an unrelated native provider operation.
"""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from histdatacom.broker_capture.fingerprint_v2 import (
        BrokerDeliveryFingerprint,
    )


_CLASSES = {
    "histdatacom.broker_capture.fingerprint_contracts": (
        "BrokerDeliveryFingerprintV1",
    ),
    "histdatacom.broker_capture.fingerprint_v2": (
        "BrokerDeliveryFingerprintV2",
    ),
    "histdatacom.broker_plugin_policy.derived": (
        "BrokerDerivedArtifactV1",
        "BrokerSyntheticOutputV1",
    ),
    "histdatacom.broker_plugin_policy.training_bindings": (
        "BrokerTrainingArtifactV1",
    ),
    "histdatacom.broker_plugin_policy.bar_bindings": ("BrokerBarEventInputV1",),
    "histdatacom.synthetic.broker_transfer": (
        "BrokerConditionedProposalV1",
        "BrokerProfileSelectionV1",
        "BrokerRenderedGroupV1",
        "BrokerTransferManifestV1",
    ),
    "histdatacom.synthetic.persistence": ("ReconstructionProductManifestV1",),
    "histdatacom.synthetic.activity": ("ReconstructionActivityManifestV1",),
    "histdatacom.synthetic.bars": (
        "DerivedBarProductManifestV1",
        "DerivedBarV1",
    ),
    "histdatacom.synthetic.certification": (
        "ReconstructionCertificationDossierV1",
    ),
}


def scientific_fingerprints(
    native: object,
) -> tuple[BrokerDeliveryFingerprint, ...]:
    """Resolve the complete exact retained inventory; do not perform IO here."""
    # The module string is a lazy-import optimization, not an admission claim.
    # Also recognize already-loaded exact classes if their metadata was changed.
    known = any(
        type(native) is getattr(sys.modules[name], class_name, None)
        for name, classes in _CLASSES.items()
        if name in sys.modules
        for class_name in classes
    )
    if not known and type(native).__module__ not in _CLASSES:
        return ()
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.broker_capture.fingerprint_v2 import (
        BrokerDeliveryFingerprintV2,
    )
    from histdatacom.synthetic.activity import ReconstructionActivityManifestV1
    from histdatacom.synthetic.bars import (
        DerivedBarProductManifestV1,
        DerivedBarV1,
    )
    from histdatacom.synthetic.broker_transfer import (
        BrokerConditionedProposalV1,
        BrokerProfileSelectionV1,
        BrokerRenderedGroupV1,
        BrokerTransferManifestV1,
    )
    from histdatacom.synthetic.certification import (
        ReconstructionCertificationDossierV1,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    from .activity_bindings import _activity_profile_ids
    from .bar_bindings import BrokerBarEventInputV1
    from .derived import BrokerDerivedArtifactV1, BrokerSyntheticOutputV1
    from .native_inputs import fingerprint_for, product_for
    from .training_bindings import BrokerTrainingArtifactV1

    if type(native) in {
        BrokerDeliveryFingerprintV1,
        BrokerDeliveryFingerprintV2,
    }:
        return (cast("BrokerDeliveryFingerprint", native),)
    if type(native) is BrokerSyntheticOutputV1:
        return (native.fingerprint,)
    if type(native) is BrokerDerivedArtifactV1:
        return native.fingerprints
    if type(native) is BrokerTrainingArtifactV1:
        return native.fingerprints
    identities: tuple[str, ...]
    if type(native) is ReconstructionProductManifestV1:
        identities = (native.broker_profile_id,)
    elif type(native) is ReconstructionCertificationDossierV1:
        identities = (native.policy.broker_fingerprint_id,)
    elif type(native) is BrokerConditionedProposalV1:
        identities = (native.selection.fingerprint_id,)
    elif type(native) is BrokerRenderedGroupV1:
        identities = (native.manifest.fingerprint_id,)
    elif type(native) in {BrokerProfileSelectionV1, BrokerTransferManifestV1}:
        identities = (
            cast(
                "BrokerProfileSelectionV1 | BrokerTransferManifestV1", native
            ).fingerprint_id,
        )
    elif type(native) is ReconstructionActivityManifestV1:
        identities = _activity_profile_ids(native)
    elif type(native) is BrokerBarEventInputV1:
        if native.event.broker_profile_id is None:
            raise ValueError("broker bar input lacks its exact parent")
        identities = (native.event.broker_profile_id,)
    elif type(native) in {DerivedBarProductManifestV1, DerivedBarV1}:
        identities = (
            product_for(
                cast(
                    "DerivedBarProductManifestV1 | DerivedBarV1", native
                ).source_product_manifest_id
            ).broker_profile_id,
        )
    else:
        return ()
    return tuple(fingerprint_for(identity) for identity in identities)


def require_scientific_provenance(native: object) -> None:
    """Run before acquiring the provider lock: replay itself needs fresh rights."""
    fingerprints = scientific_fingerprints(native)
    if not fingerprints:
        return
    from histdatacom.broker_capture.fingerprint_sources import (
        verify_broker_fingerprint_sources,
    )

    seen: dict[str, str] = {}
    for fingerprint in fingerprints:
        current = verify_broker_fingerprint_sources(fingerprint)
        previous = seen.get(current.fingerprint_id)
        if previous is not None and previous != current.to_json():
            raise ValueError(
                "scientific parent inventory contains conflicting bytes"
            )
        seen[current.fingerprint_id] = current.to_json()
