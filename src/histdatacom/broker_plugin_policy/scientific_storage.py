"""Mandatory complete scientific parents beside derived native publications.

This additive artifact leaves every native V1 wire unchanged. Reads reconcile
the exact native file and parent bytes; new reuse still needs actual source
replay and current provider rights. A path is never a provenance identity.
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ._wire import MAX_BYTES, canonical_json, load_json

if TYPE_CHECKING:
    from histdatacom.broker_capture.fingerprint_v2 import (
        BrokerDeliveryFingerprintV2,
    )


SCIENTIFIC_LINEAGE_SUFFIX = ".broker-provenance.json"
_SCHEMA = "histdatacom.broker-scientific-lineage.v1"


@dataclass(frozen=True, slots=True)
class BrokerScientificLineageV1:
    native_id: str
    native_sha256: str
    native_byte_length: int
    fingerprints: tuple[BrokerDeliveryFingerprintV2, ...]
    schema_version: str = _SCHEMA

    def __post_init__(self) -> None:
        from histdatacom.broker_capture.fingerprint_v2 import (
            require_qualified_broker_fingerprint,
        )

        if (
            type(self.native_id) is not str
            or not 1 <= len(self.native_id) <= 512
            or not re.fullmatch(r"[a-zA-Z0-9_.:-]+", self.native_id)
            or type(self.native_sha256) is not str
            or not re.fullmatch(r"[a-f0-9]{64}", self.native_sha256)
            or type(self.native_byte_length) is not int
            or not 1 <= self.native_byte_length <= MAX_BYTES
            or type(self.fingerprints) is not tuple
            or not 1 <= len(self.fingerprints) <= 16
            or self.schema_version != _SCHEMA
        ):
            raise ValueError("invalid exact scientific lineage inventory")
        roots = tuple(
            require_qualified_broker_fingerprint(item)
            for item in self.fingerprints
        )
        identities = tuple(item.fingerprint_id for item in roots)
        if identities != tuple(sorted(set(identities))):
            raise ValueError(
                "scientific fingerprints must be unique and sorted"
            )
        object.__setattr__(self, "fingerprints", roots)
        canonical_json(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "native_id": self.native_id,
            "native_sha256": self.native_sha256,
            "native_byte_length": self.native_byte_length,
            "fingerprints": [item.to_dict() for item in self.fingerprints],
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_json(cls, text: str) -> BrokerScientificLineageV1:
        from histdatacom.broker_capture.fingerprint_v2 import (
            BrokerDeliveryFingerprintV2,
        )

        value = load_json(text)
        if (
            type(value) is not dict
            or set(value)
            != {
                "schema_version",
                "native_id",
                "native_sha256",
                "native_byte_length",
                "fingerprints",
            }
            or type(value["fingerprints"]) is not list
        ):
            raise ValueError("unknown or missing scientific lineage fields")
        data = cast(dict[str, Any], value)
        result = cls(
            data["native_id"],
            data["native_sha256"],
            data["native_byte_length"],
            tuple(
                BrokerDeliveryFingerprintV2.from_dict(item)
                for item in data["fingerprints"]
            ),
            data["schema_version"],
        )
        if result.to_json() != text:
            raise ValueError(
                "scientific lineage must retain exact canonical bytes"
            )
        return result


def _expected(
    native_subject: object, data: bytes
) -> BrokerScientificLineageV1 | None:
    from .scientific import scientific_fingerprints

    values = scientific_fingerprints(native_subject)
    if not values:
        return None
    from histdatacom.broker_capture.fingerprint_contracts import (
        BrokerDeliveryFingerprintV1,
    )
    from histdatacom.broker_capture.fingerprint_v2 import (
        BrokerDeliveryFingerprintV2,
        require_qualified_broker_fingerprint,
    )

    from .storage import _file_reference

    # Fingerprint V2 itself already persists the full inventory in native bytes.
    if type(native_subject) in {
        BrokerDeliveryFingerprintV1,
        BrokerDeliveryFingerprintV2,
    }:
        return None
    if all(type(item) is BrokerDeliveryFingerprintV1 for item in values):
        return None  # Historical metadata verification only; new effects refuse V1.
    roots = tuple(
        sorted(
            (require_qualified_broker_fingerprint(item) for item in values),
            key=lambda item: item.fingerprint_id,
        )
    )
    reference = _file_reference(native_subject, data)
    return BrokerScientificLineageV1(
        reference.native_id, reference.sha256, reference.byte_length, roots
    )


def read_broker_scientific_lineage(
    native_path: Path,
) -> BrokerScientificLineageV1:
    """Read retained parents for explicit native-input setup, not admission."""
    from .storage import _path, _read

    native_path = _path(native_path)
    result = BrokerScientificLineageV1.from_json(
        _read(
            native_path.with_name(native_path.name + SCIENTIFIC_LINEAGE_SUFFIX)
        ).decode("ascii")
    )
    data = _read(native_path)
    if (
        len(data) != result.native_byte_length
        or hashlib.sha256(data).hexdigest() != result.native_sha256
    ):
        raise ValueError("retained scientific lineage differs from native file")
    return result


def verify_broker_scientific_lineage(
    native_path: Path, native_subject: object
) -> None:
    from .storage import _read

    expected = _expected(native_subject, _read(native_path))
    if expected is None:
        return
    actual = read_broker_scientific_lineage(native_path)
    if actual.to_json() != expected.to_json():
        raise ValueError(
            "retained scientific parent inventory differs from actual native roots"
        )


def write_broker_scientific_lineage(
    native_path: Path,
    native_subject: object,
    data: bytes,
    *,
    maximum_bytes: int,
) -> int:
    """Write a bounded no-clobber parent envelope before the native commit."""
    from .contracts import BrokerPolicyOperation
    from .scope import require_provider_operation
    from .storage import _path, _sync

    expected = _expected(native_subject, data)
    if expected is None:
        return 0
    native_path = _path(native_path)
    target = native_path.with_name(native_path.name + SCIENTIFIC_LINEAGE_SUFFIX)
    encoded = expected.to_json().encode("ascii")
    if type(maximum_bytes) is not int or len(encoded) > maximum_bytes:
        raise ValueError(
            "scientific lineage exceeds remaining native storage budget"
        )
    require_provider_operation(
        native_subject, BrokerPolicyOperation.RETAIN_LOCAL
    )
    if target.exists() or target.is_symlink():
        if not native_path.exists():
            raise ValueError(
                "scientific lineage exists without native artifact; no repair"
            )
        verify_broker_scientific_lineage(native_path, native_subject)
        return len(encoded)
    descriptor, name = tempfile.mkstemp(
        prefix=".broker-provenance-", dir=target.parent
    )
    temporary = Path(name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        require_provider_operation(
            native_subject, BrokerPolicyOperation.RETAIN_LOCAL
        )
        if _expected(native_subject, data) != expected:
            raise ValueError("scientific parents changed before publication")
        os.link(temporary, target, follow_symlinks=False)
        _sync(target.parent)
    finally:
        temporary.unlink(missing_ok=True)
    return len(encoded)
