"""SDK 1.1 optional public provider-protocol information, never provider trust.

The event/session ABI is unchanged. A 1.0 implementation is never inspected or
called for this method; unsupported is different from unknown protocol version.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import ClassVar, Protocol, runtime_checkable

from .contracts import BrokerPluginMetadataV1, _Artifact, _text
from .protocols import BrokerPluginV1

BROKER_PROVIDER_PROTOCOL_CAPABILITY = "provider.protocol.v1"
BROKER_PROVIDER_PROTOCOL_NAMESPACE = "org.histdatacom.provider-protocol"


@dataclass(frozen=True, slots=True)
class BrokerProviderProtocolV1(_Artifact):
    """Public provider-declared identity, not authenticated API compatibility."""

    provider_id: str
    protocol_name: str
    protocol_version: str
    KIND: ClassVar[str] = "provider-protocol"

    def _validate(self) -> None:
        if (
            re.fullmatch(r"[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*", self.provider_id)
            is None
        ):
            raise ValueError("invalid provider identifier")
        for name in ("provider_id", "protocol_name", "protocol_version"):
            _text(getattr(self, name), name, 128)


@runtime_checkable
class BrokerProviderProtocolPluginV1_1(BrokerPluginV1, Protocol):
    def provider_protocol(self) -> BrokerProviderProtocolV1:
        """Return public protocol facts without credentials or opening a session."""
        ...


def declared_provider_protocol(
    metadata: BrokerPluginMetadataV1,
) -> BrokerProviderProtocolV1 | None:
    """Read the bounded optional declaration; this does not execute a plugin."""
    metadata = BrokerPluginMetadataV1.from_json(metadata.to_json())
    # In SDK 1.0 this namespace was opaque. Preserve historical validation and
    # canonical bytes without retroactively interpreting it as typed evidence.
    if metadata.sdk_version != "1.1.0":
        return None
    declarations = tuple(
        item
        for item in metadata.extensions
        if item.namespace == BROKER_PROVIDER_PROTOCOL_NAMESPACE
    )
    if not declarations:
        return None
    return BrokerProviderProtocolV1.from_json(declarations[0].payload_json)
