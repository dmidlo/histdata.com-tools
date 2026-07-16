"""Generic final-delivery contracts for reconstructed event groups.

The v2.1 reference product does not require a broker fingerprint.  This module
therefore owns the narrow delivery boundary shared by identity delivery today
and optional delivery adapters later.  Broker-specific transfer remains in
``broker_transfer`` and is not impersonated by a synthetic fingerprint.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Any

from histdatacom.runtime_contracts import JSONValue
from histdatacom.synthetic.contracts import (
    SyntheticEventOrigin,
    SyntheticEventStreamV1,
    canonical_contract_json,
)
from histdatacom.synthetic.cross_currency import (
    CrossCurrencyGroupStatus,
    CrossCurrencyReconciledGroupV1,
)

RECONSTRUCTION_DELIVERY_MANIFEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-delivery-manifest.v1"
)
RECONSTRUCTION_DELIVERED_GROUP_SCHEMA_VERSION = (
    "histdatacom.reconstruction-delivered-group.v1"
)
MODERN_REFERENCE_DELIVERY_ENGINE_ID = (
    "histdatacom.reconstruction.modern-reference-identity"
)
MODERN_REFERENCE_DELIVERY_ENGINE_VERSION = "1.0.0"


class ReconstructionDeliveryMode(str, Enum):
    """Supported final delivery projections."""

    MODERN_REFERENCE = "modern_reference"
    BROKER_CONDITIONED = "broker_conditioned"

    @classmethod
    def from_value(
        cls, value: str | "ReconstructionDeliveryMode"
    ) -> "ReconstructionDeliveryMode":
        """Normalize CLI spelling without weakening the persisted contract."""
        if isinstance(value, cls):
            return value
        normalized = str(value).strip().lower().replace("-", "_")
        try:
            return cls(normalized)
        except ValueError as err:
            raise ValueError(
                "unsupported reconstruction delivery mode"
            ) from err


class ReconstructionDeliveryStatus(str, Enum):
    """Whether a delivery projection produced a publishable group."""

    APPLIED = "applied"
    REFUSED = "refused"


@dataclass(frozen=True, slots=True)
class ReconstructionDeliveryManifestV1:
    """Compact content and identity evidence for one delivery projection."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    ensemble_member_id: str
    input_group_id: str
    delivery_mode: ReconstructionDeliveryMode
    delivery_profile_id: str
    status: ReconstructionDeliveryStatus
    reason_codes: tuple[str, ...]
    input_content_sha256: str
    output_content_sha256: str | None
    observed_event_count: int
    synthetic_event_count: int
    identity_event_count: int
    identity_lineage_sha256: str | None
    manifest_id: str = ""
    schema_version: str = RECONSTRUCTION_DELIVERY_MANIFEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_DELIVERY_MANIFEST_SCHEMA_VERSION
        ):
            raise ValueError("unsupported reconstruction delivery manifest")
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "ensemble_member_id",
            "input_group_id",
            "delivery_profile_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self,
            "delivery_mode",
            ReconstructionDeliveryMode(self.delivery_mode),
        )
        if (
            self.delivery_mode
            is not ReconstructionDeliveryMode.MODERN_REFERENCE
        ):
            raise ValueError(
                "broker-conditioned delivery requires a separate adapter contract"
            )
        object.__setattr__(
            self, "status", ReconstructionDeliveryStatus(self.status)
        )
        reasons = tuple(_bounded_text(item) for item in self.reason_codes)
        if len(reasons) > 32:
            raise ValueError("delivery refusal reasons exceed bounded limit")
        object.__setattr__(self, "reason_codes", reasons)
        object.__setattr__(
            self,
            "input_content_sha256",
            _required_sha256(self.input_content_sha256),
        )
        output_hash = _optional_sha256(self.output_content_sha256)
        lineage_hash = _optional_sha256(self.identity_lineage_sha256)
        object.__setattr__(self, "output_content_sha256", output_hash)
        object.__setattr__(self, "identity_lineage_sha256", lineage_hash)
        for name in (
            "observed_event_count",
            "synthetic_event_count",
            "identity_event_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name))
            )
        if self.status is ReconstructionDeliveryStatus.APPLIED:
            if reasons or output_hash is None or lineage_hash is None:
                raise ValueError(
                    "applied delivery lacks output identity evidence"
                )
            if (
                self.delivery_mode
                is ReconstructionDeliveryMode.MODERN_REFERENCE
            ):
                if (
                    self.input_content_sha256 != output_hash
                    or self.identity_event_count != self.synthetic_event_count
                ):
                    raise ValueError(
                        "modern-reference delivery is not identity"
                    )
        elif not reasons or output_hash is not None or lineage_hash is not None:
            raise ValueError("refused delivery must retain reasons, not output")
        expected = _stable_id(
            "reconstruction-delivery", self.identity_payload()
        )
        if self.manifest_id and self.manifest_id != expected:
            raise ValueError("delivery manifest_id differs")
        object.__setattr__(self, "manifest_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return semantic delivery identity and bounded reconciliation evidence."""
        return {
            "schema_version": self.schema_version,
            "engine_id": MODERN_REFERENCE_DELIVERY_ENGINE_ID,
            "engine_version": MODERN_REFERENCE_DELIVERY_ENGINE_VERSION,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "ensemble_member_id": self.ensemble_member_id,
            "input_group_id": self.input_group_id,
            "delivery_mode": self.delivery_mode.value,
            "delivery_profile_id": self.delivery_profile_id,
            "status": self.status.value,
            "reason_codes": list(self.reason_codes),
            "input_content_sha256": self.input_content_sha256,
            "output_content_sha256": self.output_content_sha256,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "identity_event_count": self.identity_event_count,
            "identity_lineage_sha256": self.identity_lineage_sha256,
            "event_rows_inline": False,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return deterministic compact JSON-compatible evidence."""
        return {**self.identity_payload(), "manifest_id": self.manifest_id}

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionDeliveryManifestV1":
        """Restore and verify a delivery manifest."""
        _require_schema(data, RECONSTRUCTION_DELIVERY_MANIFEST_SCHEMA_VERSION)
        if data.get("event_rows_inline") is not False:
            raise ValueError("delivery manifest cannot contain event rows")
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            ensemble_member_id=str(data.get("ensemble_member_id", "")),
            input_group_id=str(data.get("input_group_id", "")),
            delivery_mode=ReconstructionDeliveryMode(
                str(data.get("delivery_mode", ""))
            ),
            delivery_profile_id=str(data.get("delivery_profile_id", "")),
            status=ReconstructionDeliveryStatus(str(data.get("status", ""))),
            reason_codes=_string_tuple(data.get("reason_codes")),
            input_content_sha256=str(data.get("input_content_sha256", "")),
            output_content_sha256=_optional_text(
                data.get("output_content_sha256")
            ),
            observed_event_count=_strict_int(
                data.get("observed_event_count"), "observed_event_count"
            ),
            synthetic_event_count=_strict_int(
                data.get("synthetic_event_count"), "synthetic_event_count"
            ),
            identity_event_count=_strict_int(
                data.get("identity_event_count"), "identity_event_count"
            ),
            identity_lineage_sha256=_optional_text(
                data.get("identity_lineage_sha256")
            ),
            manifest_id=str(data.get("manifest_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionDeliveryManifestV1":
        """Restore a delivery manifest from deterministic JSON."""
        value = json.loads(text)
        if not isinstance(value, Mapping):
            raise ValueError("delivery manifest JSON must contain an object")
        return cls.from_dict(value)


@dataclass(frozen=True, slots=True)
class ReconstructionDeliveredGroupV1:
    """Process-local delivered streams plus compact projection evidence."""

    manifest: ReconstructionDeliveryManifestV1
    streams: tuple[SyntheticEventStreamV1, ...]
    schema_version: str = RECONSTRUCTION_DELIVERED_GROUP_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_DELIVERED_GROUP_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction delivered group")
        if not isinstance(self.manifest, ReconstructionDeliveryManifestV1):
            raise TypeError("delivered group requires a v1 manifest")
        streams = tuple(sorted(self.streams, key=lambda item: item.symbol))
        object.__setattr__(self, "streams", streams)
        if self.manifest.status is ReconstructionDeliveryStatus.REFUSED:
            if streams:
                raise ValueError(
                    "refused delivered group cannot expose streams"
                )
            return
        if not streams or len({item.symbol for item in streams}) != len(
            streams
        ):
            raise ValueError("delivered group requires unique symbol streams")
        if any(
            item.run_id != self.manifest.run_id
            or item.ensemble_member_id != self.manifest.ensemble_member_id
            for item in streams
        ):
            raise ValueError("delivered streams differ from manifest scope")
        observed = sum(item.observed_event_count for item in streams)
        synthetic = sum(item.synthetic_event_count for item in streams)
        output_hash = reconstruction_streams_content_sha256(streams)
        if (
            observed != self.manifest.observed_event_count
            or synthetic != self.manifest.synthetic_event_count
            or output_hash != self.manifest.output_content_sha256
        ):
            raise ValueError("delivered stream content does not reconcile")

    @property
    def status(self) -> ReconstructionDeliveryStatus:
        """Return the terminal projection status."""
        return self.manifest.status

    def metadata(self) -> dict[str, JSONValue]:
        """Return bounded metadata without event rows."""
        return {
            "schema_version": self.schema_version,
            "manifest": self.manifest.to_dict(),
            "stream_ids": {
                item.symbol: item.stream_id for item in self.streams
            },
            "event_rows_inline": False,
        }


def project_modern_reference_delivery(
    group: CrossCurrencyReconciledGroupV1,
    *,
    delivery_profile_id: str,
) -> ReconstructionDeliveredGroupV1:
    """Apply the explicit no-op reference delivery without broker evidence."""
    if not isinstance(group, CrossCurrencyReconciledGroupV1):
        raise TypeError("modern delivery requires a reconciled group")
    if group.status is not CrossCurrencyGroupStatus.RECONCILED:
        raise ValueError("modern delivery refuses an unreconciled group")
    streams = group.streams
    content_hash = reconstruction_streams_content_sha256(streams)
    synthetic_ids = tuple(
        event.event_id
        for stream in streams
        for event in stream.events
        if event.origin is SyntheticEventOrigin.SYNTHETIC
    )
    manifest = ReconstructionDeliveryManifestV1(
        run_id=group.run_id,
        window_id=group.window_id,
        synchronization_unit_id=group.synchronization_unit_id,
        ensemble_member_id=group.ensemble_member_id,
        input_group_id=group.group_id,
        delivery_mode=ReconstructionDeliveryMode.MODERN_REFERENCE,
        delivery_profile_id=delivery_profile_id,
        status=ReconstructionDeliveryStatus.APPLIED,
        reason_codes=(),
        input_content_sha256=content_hash,
        output_content_sha256=content_hash,
        observed_event_count=sum(item.observed_event_count for item in streams),
        synthetic_event_count=len(synthetic_ids),
        identity_event_count=len(synthetic_ids),
        identity_lineage_sha256=_text_sequence_sha256(synthetic_ids),
    )
    return ReconstructionDeliveredGroupV1(manifest=manifest, streams=streams)


def reconstruction_streams_content_sha256(
    streams: Sequence[SyntheticEventStreamV1],
) -> str:
    """Hash ordered exact stream rows independently of file placement."""
    payload: list[JSONValue] = [
        event.to_dict()
        for stream in sorted(streams, key=lambda item: item.symbol)
        for event in stream.events
    ]
    return hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()


def _text_sequence_sha256(values: Sequence[str]) -> str:
    digest = hashlib.sha256(b"histdatacom-delivery-identity-lineage-v1\n")
    for value in values:
        digest.update(value.encode("utf-8"))
        digest.update(b"\n")
    return digest.hexdigest()


def _stable_id(namespace: str, payload: Mapping[str, JSONValue]) -> str:
    content = canonical_contract_json(payload).encode("utf-8")
    return f"{namespace}:sha256:{hashlib.sha256(content).hexdigest()}"


def _required_text(value: Any) -> str:
    normalized = str(value).strip() if value is not None else ""
    if not normalized:
        raise ValueError("required delivery text is empty")
    return normalized


def _bounded_text(value: Any) -> str:
    normalized = _required_text(value)
    if len(normalized.encode("utf-8")) > 1_024:
        raise ValueError("delivery text exceeds bounded limit")
    return normalized


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _required_sha256(value: Any) -> str:
    normalized = _required_text(value).lower()
    if len(normalized) != 64 or any(
        item not in "0123456789abcdef" for item in normalized
    ):
        raise ValueError("delivery hash must be a lowercase sha256 digest")
    return normalized


def _optional_sha256(value: Any) -> str | None:
    normalized = _optional_text(value)
    return _required_sha256(normalized) if normalized is not None else None


def _nonnegative_int(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("delivery count must be a nonnegative integer")
    return value


def _strict_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError("delivery value must be a sequence")
    return tuple(str(item) for item in value)


def _require_schema(data: Mapping[str, Any], expected: str) -> None:
    if data.get("schema_version") != expected:
        raise ValueError("unsupported delivery schema version")


__all__ = [
    "MODERN_REFERENCE_DELIVERY_ENGINE_ID",
    "MODERN_REFERENCE_DELIVERY_ENGINE_VERSION",
    "RECONSTRUCTION_DELIVERED_GROUP_SCHEMA_VERSION",
    "RECONSTRUCTION_DELIVERY_MANIFEST_SCHEMA_VERSION",
    "ReconstructionDeliveredGroupV1",
    "ReconstructionDeliveryManifestV1",
    "ReconstructionDeliveryMode",
    "ReconstructionDeliveryStatus",
    "project_modern_reference_delivery",
    "reconstruction_streams_content_sha256",
]
