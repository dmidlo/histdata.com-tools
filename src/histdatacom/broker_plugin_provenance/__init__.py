"""Host-owned capture provenance; integrity is not scientific admission."""

from typing import TYPE_CHECKING

from .chain import (
    link_provenance_entry,
    make_provenance_entry,
    provenance_header_digest,
    verify_provenance_chain,
)
from .contracts import (
    BROKER_PROVENANCE_ALGORITHM,
    BrokerProvenanceCheckpointV1,
    BrokerProvenanceConformanceStatus,
    BrokerProvenanceEntryKind,
    BrokerProvenanceEntryV1,
    BrokerProvenanceHeaderV1,
    BrokerProvenanceLinkV1,
    BrokerProvenanceNativeFamily,
    BrokerProvenancePolicyV1,
    BrokerProvenanceSealV1,
    BrokerProvenanceTerminalV1,
    BrokerProvenanceVerificationReason,
    BrokerProvenanceVerificationV1,
)

if TYPE_CHECKING:
    from .native import (
        read_lifecycle_capture_provenance,
        require_legacy_capture_provenance,
    )

__all__ = [
    "BROKER_PROVENANCE_ALGORITHM",
    "BrokerProvenanceCheckpointV1",
    "BrokerProvenanceConformanceStatus",
    "BrokerProvenanceEntryKind",
    "BrokerProvenanceEntryV1",
    "BrokerProvenanceHeaderV1",
    "BrokerProvenanceLinkV1",
    "BrokerProvenanceNativeFamily",
    "BrokerProvenancePolicyV1",
    "BrokerProvenanceSealV1",
    "BrokerProvenanceTerminalV1",
    "BrokerProvenanceVerificationReason",
    "BrokerProvenanceVerificationV1",
    "link_provenance_entry",
    "make_provenance_entry",
    "provenance_header_digest",
    "read_lifecycle_capture_provenance",
    "require_legacy_capture_provenance",
    "verify_provenance_chain",
]


def __getattr__(name: str) -> object:
    if name in (
        "read_lifecycle_capture_provenance",
        "require_legacy_capture_provenance",
    ):
        from . import native

        return getattr(native, name)
    raise AttributeError(name)
