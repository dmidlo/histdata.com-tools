"""Explicit process-local native roots for downstream provider-derived work.

An input registry carries exact retained parent values, not permission. It never
loads a path, chooses a provider, invents a missing fingerprint or installs a
policy scope. Every material operation still needs fresh declared admission.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import TYPE_CHECKING

from ._wire import MAX_BYTES, canonical_json
from .bindings import (
    BrokerLegacyCaptureV1,
    BrokerProviderOutputContractV1,
    resolve_provider_subject,
    _restore_native,
)

if TYPE_CHECKING:
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )


@dataclass(frozen=True, slots=True)
class _NativeInputs:
    process_id: int
    captures: dict[str, tuple[str, str]]
    fingerprints: dict[str, str]
    products: dict[str, str]


_CURRENT: ContextVar[_NativeInputs | None] = ContextVar(
    "broker_provider_native_inputs", default=None
)


@contextmanager
def provider_native_inputs(*native_roots: object) -> Iterator[None]:
    """Register bounded exact roots for one downstream execution, without IO."""
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    if _CURRENT.get() is not None:
        raise ValueError(
            "nested provider native-input scopes are not supported"
        )
    if not 1 <= len(native_roots) <= 128:
        raise ValueError("provider native-input scope requires1..128 roots")
    captures: dict[str, tuple[str, str]] = {}
    fingerprints: dict[str, str] = {}
    products: dict[str, str] = {}
    bounds: list[object] = []
    total = 0
    values: tuple[str, ...]
    for root in native_roots:
        if type(root) is BrokerLegacyCaptureV1:
            resolve_provider_subject(root)
            session = BrokerCaptureSessionV1.from_json(root.session.to_json())
            contract = BrokerProviderOutputContractV1.from_json(
                root.output_contract.to_json()
            )
            if session.session_id in captures:
                raise ValueError("duplicate native capture root")
            values = (session.to_json(), contract.to_json())
            captures[session.session_id] = values
        elif type(root) is BrokerDeliveryFingerprintV1:
            resolve_provider_subject(root)
            fingerprint = BrokerDeliveryFingerprintV1.from_json(root.to_json())
            if fingerprint.fingerprint_id in fingerprints:
                raise ValueError("duplicate native fingerprint root")
            values = (fingerprint.to_json(),)
            fingerprints[fingerprint.fingerprint_id] = values[0]
        elif type(root) is ReconstructionProductManifestV1:
            product = _restore_native(root, {ReconstructionProductManifestV1})
            if product.manifest_id in products:
                raise ValueError("duplicate native reconstruction root")
            values = (product.to_json(),)
            products[product.manifest_id] = values[0]
        else:
            raise ValueError(
                "unsupported provider native root; exact values required"
            )
        for text in values:
            total += len(text.encode("utf-8"))
            if total > MAX_BYTES:
                raise ValueError(
                    "provider native-input aggregate byte budget exceeded"
                )
            bounds.append(json.loads(text))
        # Charge the complete expanded representation, not merely root count.
        canonical_json(bounds)
    token = _CURRENT.set(
        _NativeInputs(os.getpid(), captures, fingerprints, products)
    )
    try:
        yield
    finally:
        _CURRENT.reset(token)


def _current() -> _NativeInputs:
    state = _CURRENT.get()
    if state is None or state.process_id != os.getpid():
        raise ValueError(
            "exact current-process provider native inputs required"
        )
    return state


def capture_request_for(
    session: BrokerCaptureSessionV1,
) -> BrokerLegacyCaptureV1:
    from histdatacom.broker_capture.contracts import BrokerCaptureSessionV1

    if type(session) is not BrokerCaptureSessionV1:
        raise ValueError("exact native capture session required")
    state = _current()
    values = state.captures.get(session.session_id)
    if values is None or session.to_json() != values[0]:
        raise ValueError(
            "capture session is absent or differs from its registered native root"
        )
    return BrokerLegacyCaptureV1(
        BrokerCaptureSessionV1.from_json(values[0]),
        BrokerProviderOutputContractV1.from_json(values[1]),
    )


def fingerprint_for(fingerprint_id: str) -> BrokerDeliveryFingerprintV1:
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )

    if type(fingerprint_id) is not str:
        raise ValueError("exact native fingerprint identity required")
    text = _current().fingerprints.get(fingerprint_id)
    if text is None:
        raise ValueError(
            "fingerprint is absent from the registered native roots"
        )
    return BrokerDeliveryFingerprintV1.from_json(text)


def product_for(manifest_id: str) -> ReconstructionProductManifestV1:
    """Return an exact detached broker product, never permission or IO proof."""
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    if type(manifest_id) is not str:
        raise ValueError("exact native product identity required")
    text = _current().products.get(manifest_id)
    if text is None:
        raise ValueError("product is absent from the registered native roots")
    return ReconstructionProductManifestV1.from_json(text)


@contextmanager
def provider_reconstruction_inputs(
    *products: ReconstructionProductManifestV1,
) -> Iterator[None]:
    """Bind exact source metadata inside one closed operation, without rights.

    Existing capture/fingerprint roots are retained. The complete inherited
    inventory is charged again; overlays cannot evade root, node or byte limits.
    This never loads a path or verifies physical source data.
    """
    from histdatacom.synthetic.persistence import (
        ReconstructionProductManifestV1,
    )

    if not 1 <= len(products) <= 128:
        raise ValueError("reconstruction overlay requires1..128 exact products")
    previous = _CURRENT.get()
    if previous is not None and previous.process_id != os.getpid():
        raise ValueError("provider native inputs belong to another process")
    captures = {} if previous is None else dict(previous.captures)
    fingerprints = {} if previous is None else dict(previous.fingerprints)
    retained = {} if previous is None else dict(previous.products)
    for value in products:
        product = _restore_native(value, {ReconstructionProductManifestV1})
        text = product.to_json()
        old = retained.get(product.manifest_id)
        if old is not None and old != text:
            raise ValueError("native product identity has conflicting bytes")
        retained[product.manifest_id] = text
    if len(captures) + len(fingerprints) + len(retained) > 128:
        raise ValueError("provider native-input root budget exceeded")
    values = [text for pair in captures.values() for text in pair]
    values.extend(fingerprints.values())
    values.extend(retained.values())
    if sum(len(text.encode("utf-8")) for text in values) > MAX_BYTES:
        raise ValueError("provider native-input aggregate byte budget exceeded")
    canonical_json([json.loads(text) for text in values])
    token = _CURRENT.set(
        _NativeInputs(os.getpid(), captures, fingerprints, retained)
    )
    try:
        yield
    finally:
        _CURRENT.reset(token)
