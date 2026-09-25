"""Closed broker bar roots; native identities are unchanged and grant no rights."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from .bindings import (
    _ResolvedNative,
    _composite_ref,
    _ref,
    _resolve_native,
    _restore_native,
)
from .contracts import BrokerPolicyDataClass, BrokerPolicySubjectV1

if TYPE_CHECKING:
    from histdatacom.synthetic.contracts import SyntheticEventV1


@dataclass(frozen=True, slots=True)
class BrokerBarEventInputV1:
    """Exact single native event consumed by the bounded bar accumulator."""

    event: SyntheticEventV1


def broker_bar_source_id(manifest_id: str) -> bool:
    """Recognize the reserved V1 hint, never infer permission from a hash."""
    return manifest_id.startswith("reconstruction-manifest:sha256:")


def _resolve_bar_native(native: object) -> _ResolvedNative | None:
    """Return None for unrelated types; require all actual broker parents."""
    from histdatacom.synthetic.bars import (
        DerivedBarProductManifestV1,
        DerivedBarV1,
    )
    from histdatacom.synthetic.contracts import SyntheticEventV1

    from .native_inputs import fingerprint_for, product_for

    if type(native) is BrokerBarEventInputV1:
        event = _restore_native(native.event, {SyntheticEventV1})
        if event.broker_profile_id is None:
            raise ValueError("bar event has no native broker parent")
        parent = _resolve_native(
            fingerprint_for(event.broker_profile_id)
        ).subject
        text = event.to_json()
        ref = _ref("SyntheticEventV1", event.event_id, text)
    elif type(native) in {DerivedBarProductManifestV1, DerivedBarV1}:
        artifact = _restore_native(
            native, {DerivedBarProductManifestV1, DerivedBarV1}
        )
        product = product_for(artifact.source_product_manifest_id)
        if (
            artifact.run_id != product.run_id
            or artifact.ensemble_member_id != product.ensemble_member_id
        ):
            raise ValueError(
                "bar native run/member differs from product parent"
            )
        if type(artifact) is DerivedBarProductManifestV1:
            if (
                artifact.source_product_publication_id != product.publication_id
                or artifact.source_product_logical_sha256
                != product.replay.logical_content_sha256
                or not set(artifact.symbols).issubset(product.symbols)
            ):
                raise ValueError(
                    "bar native manifest differs from product parent"
                )
            identity = artifact.manifest_id
        else:
            if artifact.symbol not in product.symbols or not set(
                artifact.broker_profile_ids
            ).issubset({product.broker_profile_id}):
                raise ValueError("bar row provider lineage differs from parent")
            identity = artifact.bar_id
        parent = _resolve_native(product).subject
        text = artifact.to_json()
        ref = _ref(type(artifact).__name__, identity, text)
    else:
        return None
    classes = set(parent.data_classes) | {
        BrokerPolicyDataClass.BROKER_SYNTHETIC,
    }
    return _ResolvedNative(
        BrokerPolicySubjectV1(
            _composite_ref(
                "broker-bar-artifact",
                {"artifact": ref, "parent": parent.native_ref},
            ),
            parent.bindings,
            tuple(sorted(classes)),
        ),
        text,
    )
