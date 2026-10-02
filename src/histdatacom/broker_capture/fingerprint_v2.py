"""Versioned scientific parent identity with exact retained capture roots.

Parsing checks canonical relationships, never execution or source authenticity.
New material use separately replays the actual retained sources against the
complete expected seals. Historical V1 statistical bytes remain unchanged.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeAlias, cast

from histdatacom.broker_capture.contracts import (
    BrokerCaptureSessionManifestV1,
)
from histdatacom.broker_capture.fingerprint_contracts import (
    BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION,
    MAX_BROKER_DELIVERY_CAPTURES,
    BrokerCaptureEligibilityV1,
    BrokerDeliveryCaptureEvidenceV1,
    BrokerDeliveryCellV1,
    BrokerDeliveryFingerprintV1,
    BrokerDeliveryFitConfigV1,
)
from histdatacom.runtime_contracts import JSONValue

if TYPE_CHECKING:
    from histdatacom.broker_plugin_policy.bindings import (
        BrokerProviderOutputContractV1,
    )
    from histdatacom.broker_plugin_provenance.contracts import (
        BrokerProvenanceHeaderV1,
        BrokerProvenanceSealV1,
    )

BROKER_DELIVERY_FINGERPRINT_V2_SCHEMA_VERSION = (
    "histdatacom.broker-delivery-fingerprint.v2"
)
BROKER_FINGERPRINT_CAPTURE_ROOT_SCHEMA_VERSION = (
    "histdatacom.broker-fingerprint-capture-root.v1"
)


def _digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_json(value: object) -> str:
    from histdatacom.broker_plugin_policy._wire import canonical_json as encode

    return encode(value)


def load_json(value: str) -> object:
    from histdatacom.broker_plugin_policy._wire import load_json as decode

    return decode(value)


def _object(value: object) -> dict[str, Any]:
    if type(value) is not dict:
        raise ValueError("fingerprint provenance requires an exact JSON object")
    return cast(dict[str, Any], value)


@dataclass(frozen=True, slots=True)
class BrokerFingerprintCaptureRootV1:
    """Exact expected native source and seal; not proof of a current replay."""

    manifest: BrokerCaptureSessionManifestV1
    output_contract: BrokerProviderOutputContractV1
    header: BrokerProvenanceHeaderV1
    seal: BrokerProvenanceSealV1
    schema_version: str = BROKER_FINGERPRINT_CAPTURE_ROOT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerProviderOutputContractV1,
        )
        from histdatacom.broker_plugin_provenance.contracts import (
            BrokerProvenanceHeaderV1,
            BrokerProvenanceNativeFamily,
            BrokerProvenanceSealV1,
        )

        if (
            self.schema_version
            != BROKER_FINGERPRINT_CAPTURE_ROOT_SCHEMA_VERSION
            or type(self.manifest) is not BrokerCaptureSessionManifestV1
            or type(self.output_contract) is not BrokerProviderOutputContractV1
            or type(self.header) is not BrokerProvenanceHeaderV1
            or type(self.seal) is not BrokerProvenanceSealV1
        ):
            raise ValueError(
                "exact native fingerprint capture-root types required"
            )
        terminal = self.seal.terminal
        session = self.manifest.session
        if (
            self.header.family
            is not BrokerProvenanceNativeFamily.LEGACY_CAPTURE_V1
            or self.header.capture_id != session.session_id
            or self.header.native_header_id != session.session_id
            or self.header.native_header_sha256 != _digest(session.to_json())
            or self.seal.header_id != self.header.artifact_id
            or terminal.header_id != self.header.artifact_id
            or terminal.native_manifest_id != self.manifest.manifest_id
            or terminal.native_manifest_sha256
            != _digest(self.manifest.to_json())
            or not terminal.native_complete
            or not terminal.observations_complete
            or not self.manifest.complete
        ):
            raise ValueError(
                "fingerprint root differs from its complete native source"
            )
        # Bound the entire expanded source, including all retained native fields.
        canonical_json(self.to_dict())

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "manifest": self.manifest.to_dict(),
            "output_contract": cast(JSONValue, self.output_contract.to_dict()),
            "header": cast(JSONValue, self.header.to_dict()),
            "seal": cast(JSONValue, self.seal.to_dict()),
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> BrokerFingerprintCaptureRootV1:
        from histdatacom.broker_plugin_policy.bindings import (
            BrokerProviderOutputContractV1,
        )
        from histdatacom.broker_plugin_provenance.contracts import (
            BrokerProvenanceHeaderV1,
            BrokerProvenanceSealV1,
        )

        value = _object(data)
        if set(value) != {
            "schema_version",
            "manifest",
            "output_contract",
            "header",
            "seal",
        }:
            raise ValueError("unknown or missing fingerprint source fields")
        result = cls(
            BrokerCaptureSessionManifestV1.from_dict(
                _object(value["manifest"])
            ),
            BrokerProviderOutputContractV1.from_dict(
                _object(value["output_contract"])
            ),
            BrokerProvenanceHeaderV1.from_dict(_object(value["header"])),
            BrokerProvenanceSealV1.from_dict(_object(value["seal"])),
            value["schema_version"],
        )
        if canonical_json(result.to_dict()) != canonical_json(value):
            raise ValueError(
                "fingerprint source is not exact canonical native evidence"
            )
        return result


@dataclass(frozen=True, slots=True)
class BrokerDeliveryFingerprintV2:
    """Native V1 statistics plus every exact expected source-chain identity."""

    statistics: BrokerDeliveryFingerprintV1
    capture_roots: tuple[BrokerFingerprintCaptureRootV1, ...]
    fingerprint_id: str = ""
    schema_version: str = BROKER_DELIVERY_FINGERPRINT_V2_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            type(self.statistics) is not BrokerDeliveryFingerprintV1
            or type(self.capture_roots) is not tuple
            or not 1 <= len(self.capture_roots) <= MAX_BROKER_DELIVERY_CAPTURES
            or any(
                type(item) is not BrokerFingerprintCaptureRootV1
                for item in self.capture_roots
            )
            or self.schema_version
            != BROKER_DELIVERY_FINGERPRINT_V2_SCHEMA_VERSION
            or type(self.fingerprint_id) is not str
        ):
            raise ValueError(
                "exact bounded V2 fingerprint composition required"
            )
        # Detach all mutable dictionaries inherited from the legacy contracts.
        statistics = BrokerDeliveryFingerprintV1.from_json(
            self.statistics.to_json()
        )
        roots = tuple(
            BrokerFingerprintCaptureRootV1.from_dict(item.to_dict())
            for item in self.capture_roots
        )
        sessions = tuple(item.manifest.session.session_id for item in roots)
        if sessions != tuple(sorted(set(sessions))):
            raise ValueError(
                "fingerprint source roots must be unique and sorted"
            )
        evidence = {
            item.session_id: item for item in statistics.capture_evidence
        }
        if len(evidence) != len(statistics.capture_evidence) or set(
            sessions
        ) != set(evidence):
            raise ValueError(
                "fingerprint root inventory differs from native statistics"
            )
        for root in roots:
            manifest = root.manifest
            native = evidence[manifest.session.session_id]
            session = manifest.session
            from .fingerprints import _partition_hashes_digest

            if (
                native.manifest_id != manifest.manifest_id
                or native.event_count != manifest.event_count
                or native.partition_count != len(manifest.partitions)
                or native.partition_hashes_sha256
                != _partition_hashes_digest(manifest)
                or any(
                    getattr(statistics, name) != getattr(session, name)
                    for name in (
                        "adapter_id",
                        "adapter_version",
                        "adapter_config_sha256",
                        "protocol",
                        "environment_id",
                        "server_id",
                        "account_id_sha256",
                        "collector_id",
                        "collector_version",
                    )
                )
            ):
                raise ValueError(
                    "fingerprint statistics do not bind exact capture sources"
                )
        object.__setattr__(self, "statistics", statistics)
        object.__setattr__(self, "capture_roots", roots)
        expected = "broker-delivery-fingerprint-v2:sha256:" + _digest(
            canonical_json(self.identity_payload())
        )
        if self.fingerprint_id and self.fingerprint_id != expected:
            raise ValueError(
                "V2 fingerprint identity differs from complete native roots"
            )
        object.__setattr__(self, "fingerprint_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        return {
            "schema_version": self.schema_version,
            "statistics": self.statistics.to_dict(),
            "capture_roots": [item.to_dict() for item in self.capture_roots],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        return {
            **self.identity_payload(),
            "fingerprint_id": self.fingerprint_id,
        }

    def to_json(self) -> str:
        return canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> BrokerDeliveryFingerprintV2:
        value = _object(data)
        if (
            set(value)
            != {
                "schema_version",
                "statistics",
                "capture_roots",
                "fingerprint_id",
            }
            or type(value["capture_roots"]) is not list
        ):
            raise ValueError("unknown or missing V2 fingerprint fields")
        result = cls(
            BrokerDeliveryFingerprintV1.from_dict(_object(value["statistics"])),
            tuple(
                BrokerFingerprintCaptureRootV1.from_dict(_object(item))
                for item in value["capture_roots"]
            ),
            value["fingerprint_id"],
            value["schema_version"],
        )
        if result.to_json() != canonical_json(value):
            raise ValueError("V2 fingerprint is not exact canonical evidence")
        return result

    @classmethod
    def from_json(cls, text: str) -> BrokerDeliveryFingerprintV2:
        return cls.from_dict(_object(load_json(text)))

    @property
    def adapter_id(self) -> str:
        return self.statistics.adapter_id

    @property
    def adapter_version(self) -> str:
        return self.statistics.adapter_version

    @property
    def adapter_config_sha256(self) -> str:
        return self.statistics.adapter_config_sha256

    @property
    def protocol(self) -> str:
        return self.statistics.protocol

    @property
    def environment_id(self) -> str:
        return self.statistics.environment_id

    @property
    def server_id(self) -> str:
        return self.statistics.server_id

    @property
    def account_id_sha256(self) -> str | None:
        return self.statistics.account_id_sha256

    @property
    def collector_id(self) -> str:
        return self.statistics.collector_id

    @property
    def collector_version(self) -> str:
        return self.statistics.collector_version

    @property
    def fit_config(self) -> BrokerDeliveryFitConfigV1:
        return self.statistics.fit_config

    @property
    def capture_evidence(self) -> tuple[BrokerDeliveryCaptureEvidenceV1, ...]:
        return self.statistics.capture_evidence

    @property
    def eligibility_decisions(self) -> tuple[BrokerCaptureEligibilityV1, ...]:
        return self.statistics.eligibility_decisions

    @property
    def support_start_utc_ns(self) -> int:
        return self.statistics.support_start_utc_ns

    @property
    def support_end_utc_ns(self) -> int:
        return self.statistics.support_end_utc_ns

    @property
    def effective_start_utc_ns(self) -> int:
        return self.statistics.effective_start_utc_ns

    @property
    def effective_end_utc_ns(self) -> int | None:
        return self.statistics.effective_end_utc_ns

    @property
    def cells(self) -> tuple[BrokerDeliveryCellV1, ...]:
        return self.statistics.cells

    @property
    def supersedes_fingerprint_id(self) -> str | None:
        return self.statistics.supersedes_fingerprint_id

    @property
    def limitations(self) -> tuple[str, ...]:
        return self.statistics.limitations


BrokerDeliveryFingerprint: TypeAlias = (
    BrokerDeliveryFingerprintV1 | BrokerDeliveryFingerprintV2
)


def parse_broker_delivery_fingerprint(
    value: str | Mapping[str, Any],
) -> BrokerDeliveryFingerprint:
    """Historical parser only; neither JSON generation grants material use."""
    if type(value) is str:
        from histdatacom.broker_plugin_policy._wire import MAX_BYTES

        if len(value) > MAX_BYTES + 1:
            raise ValueError("fingerprint JSON exceeds bounded artifact size")
        data = _object(json.loads(value))
    else:
        data = _object(value)
    schema = data.get("schema_version")
    if schema == BROKER_DELIVERY_FINGERPRINT_SCHEMA_VERSION:
        # Preserve historical reader whitespace and V1 normalization behavior.
        return BrokerDeliveryFingerprintV1.from_dict(data)
    if schema == BROKER_DELIVERY_FINGERPRINT_V2_SCHEMA_VERSION:
        if type(value) is str:
            # Native file publication adds exactly one line terminator. All
            # retained V2 JSON content otherwise remains strict canonical wire.
            return BrokerDeliveryFingerprintV2.from_json(
                value.removesuffix("\n")
            )
        return BrokerDeliveryFingerprintV2.from_dict(data)
    raise ValueError("unsupported broker fingerprint generation")


def require_qualified_broker_fingerprint(
    value: object,
) -> BrokerDeliveryFingerprintV2:
    """Restore exact retained roots; actual IO replay is a separate mandatory gate."""
    if type(value) is not BrokerDeliveryFingerprintV2:
        raise ValueError(
            "new broker science requires a V2 fingerprint with complete verified roots"
        )
    return BrokerDeliveryFingerprintV2.from_json(value.to_json())
