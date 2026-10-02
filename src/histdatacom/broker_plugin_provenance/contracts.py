"""Closed canonical host-chain contracts, independent of provider truth."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import Enum
from typing import ClassVar

from ._wire import MAX_BYTES, Artifact, digest, load_json

BROKER_PROVENANCE_ALGORITHM = "sha256-canonical-json-chain-v1"
PAYLOAD_CHUNK_BYTES = 32_768


class BrokerProvenanceNativeFamily(str, Enum):
    LIFECYCLE_V1 = "lifecycle_v1"
    LEGACY_CAPTURE_V1 = "legacy_capture_v1"


class BrokerProvenanceEntryKind(str, Enum):
    NATIVE_RECORD = "native_record"
    HOST_OBSERVATION = "host_observation"
    ADMITTED_INGRESS = "admitted_ingress"
    TERMINAL = "terminal"


class BrokerProvenanceConformanceStatus(str, Enum):
    UNAVAILABLE = "unavailable"
    NOT_APPLICABLE = "not_applicable"
    REFERENCE_HOST_ONLY = "reference_host_only"
    CANDIDATE_QUALIFIED = "candidate_qualified"
    CANDIDATE_FAILED = "candidate_failed"


class BrokerProvenanceVerificationReason(str, Enum):
    VERIFIED = "verified_anchored_structure"
    UNANCHORED = "self_consistent_unanchored"
    PARTIAL = "partial_or_unsealed"
    INVALID_HEADER = "invalid_header"
    INVALID_ENTRY = "invalid_entry"
    SEQUENCE = "invalid_host_sequence"
    EPOCH = "invalid_connection_epoch"
    CHAIN_DIGEST = "chain_digest_mismatch"
    CHECKPOINT = "checkpoint_mismatch"
    TERMINAL = "terminal_mismatch"
    SEAL = "seal_mismatch"
    ANCHOR_MISMATCH = "external_anchor_mismatch"
    LIMIT = "bounded_evidence_limit"


def _count(value: int, maximum: int = 2**63 - 1, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ValueError("invalid provenance integer")


def _identity(value: str) -> None:
    if (
        type(value) is not str
        or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:+-]{0,511}", value) is None
    ):
        raise ValueError("invalid provenance public identity")


def _artifact(value: str, kind: str) -> None:
    if (
        re.fullmatch(
            r"broker-provenance-" + kind + r":sha256:[a-f0-9]{64}", value
        )
        is None
    ):
        raise ValueError("wrong provenance artifact family")


def payload_chunks(text: str) -> tuple[str, ...]:
    """Detach exact canonical native/observation bytes without normalizing."""
    if type(text) is not str or not text or not text.isascii():
        raise ValueError("canonical ASCII provenance payload required")
    if type(load_json(text)) is not dict:
        raise ValueError("provenance payload must be a canonical object")
    return tuple(
        text[index : index + PAYLOAD_CHUNK_BYTES]
        for index in range(0, len(text), PAYLOAD_CHUNK_BYTES)
    )


def _payload(chunks: tuple[str, ...]) -> str:
    if (
        not chunks
        or len(chunks) > MAX_BYTES // PAYLOAD_CHUNK_BYTES
        or any(len(part) != PAYLOAD_CHUNK_BYTES for part in chunks[:-1])
        or not 1 <= len(chunks[-1]) <= PAYLOAD_CHUNK_BYTES
    ):
        raise ValueError("noncanonical provenance payload chunks")
    text = "".join(chunks)
    if payload_chunks(text) != chunks:
        raise ValueError("noncanonical provenance payload")
    return text


@dataclass(frozen=True, slots=True)
class BrokerProvenancePolicyV1(Artifact):
    checkpoint_interval: int = 64
    max_entries: int = 1_000_000
    max_entry_bytes: int = MAX_BYTES
    max_capture_bytes: int = 268_435_456
    max_checkpoints: int = 65_536
    KIND: ClassVar[str] = "policy"

    def _validate(self) -> None:
        _count(self.checkpoint_interval, 1_000_000, 1)
        _count(self.max_entries, 1_000_000, 1)
        _count(self.max_entry_bytes, MAX_BYTES, 256)
        _count(self.max_capture_bytes, 1_073_741_824, 1024)
        _count(self.max_checkpoints, 65_536, 1)
        if (
            self.max_entries + self.checkpoint_interval - 1
        ) // self.checkpoint_interval > self.max_checkpoints:
            raise ValueError("provenance checkpoint inventory exceeds bound")


@dataclass(frozen=True, slots=True)
class BrokerProvenanceHeaderV1(Artifact):
    family: BrokerProvenanceNativeFamily
    capture_id: str
    native_header_id: str
    native_header_sha256: str
    plugin_id: str
    plugin_version: str | None
    provider_id: str
    feed_id: str | None
    configuration_id: str
    configuration_sha256: str
    distribution_name: str | None
    distribution_version: str | None
    implementation_sha256: str | None
    registration_sha256: str | None
    sdk_version: str | None
    event_schema_version: str
    permission_manifest_id: str | None
    permission_context_id: str | None
    permission_decision_id: str | None
    permission_grant_id: str | None
    provider_decision_id: str
    host_version: str
    host_python_version: str
    environment_id: str
    started_at_utc_ns: int
    started_at_monotonic_ns: int
    conformance_version: str | None
    conformance_status: BrokerProvenanceConformanceStatus
    conformance_receipt_id: str | None
    policy: BrokerProvenancePolicyV1 = field(
        default_factory=BrokerProvenancePolicyV1
    )
    algorithm: str = BROKER_PROVENANCE_ALGORITHM
    KIND: ClassVar[str] = "header"

    def _validate(self) -> None:
        for value in (
            self.capture_id,
            self.native_header_id,
            self.plugin_id,
            self.provider_id,
            self.configuration_id,
            self.event_schema_version,
            self.provider_decision_id,
            self.host_version,
            self.host_python_version,
            self.environment_id,
        ):
            _identity(value)
        for optional_value in (
            self.plugin_version,
            self.feed_id,
            self.distribution_name,
            self.distribution_version,
            self.sdk_version,
            self.permission_manifest_id,
            self.permission_context_id,
            self.permission_decision_id,
            self.permission_grant_id,
            self.conformance_version,
            self.conformance_receipt_id,
        ):
            if optional_value is not None:
                _identity(optional_value)
        digest(self.native_header_sha256)
        digest(self.configuration_sha256)
        for optional_digest in (
            self.implementation_sha256,
            self.registration_sha256,
        ):
            if optional_digest is not None:
                digest(optional_digest)
        sdk_fields = (
            self.distribution_name,
            self.distribution_version,
            self.implementation_sha256,
            self.registration_sha256,
            self.sdk_version,
            self.permission_manifest_id,
            self.permission_context_id,
            self.permission_decision_id,
            self.permission_grant_id,
        )
        if self.family is BrokerProvenanceNativeFamily.LIFECYCLE_V1:
            if any(value is None for value in sdk_fields):
                raise ValueError(
                    "SDK provenance requires exact installed facts"
                )
        elif any(value is not None for value in sdk_fields):
            raise ValueError("legacy provenance cannot invent SDK facts")
        unavailable = self.conformance_status in (
            BrokerProvenanceConformanceStatus.UNAVAILABLE,
            BrokerProvenanceConformanceStatus.NOT_APPLICABLE,
        )
        if unavailable != (
            self.conformance_version is None
            and self.conformance_receipt_id is None
        ) or (
            not unavailable
            and (
                self.conformance_version is None
                or self.conformance_receipt_id is None
            )
        ):
            raise ValueError("conformance evidence must be explicit")
        _count(self.started_at_utc_ns)
        _count(self.started_at_monotonic_ns)
        if self.algorithm != BROKER_PROVENANCE_ALGORITHM:
            raise ValueError("unsupported provenance chain algorithm")


@dataclass(frozen=True, slots=True)
class BrokerProvenanceEntryV1(Artifact):
    header_id: str
    sequence: int
    epoch: int
    kind: BrokerProvenanceEntryKind
    native_record_id: str | None
    payload_chunks: tuple[str, ...]
    KIND: ClassVar[str] = "entry"

    def _validate(self) -> None:
        _artifact(self.header_id, "header")
        _count(self.sequence, 999_999)
        _count(self.epoch, 999_999)
        if self.native_record_id is not None:
            _identity(self.native_record_id)
        if (
            self.kind is BrokerProvenanceEntryKind.NATIVE_RECORD
            and self.native_record_id is None
        ):
            raise ValueError("native provenance requires record identity")
        if (
            self.kind is BrokerProvenanceEntryKind.TERMINAL
            and self.native_record_id is not None
        ):
            raise ValueError("terminal is not a native record")
        _payload(self.payload_chunks)

    @property
    def payload_json(self) -> str:
        return "".join(self.payload_chunks)


@dataclass(frozen=True, slots=True)
class BrokerProvenanceLinkV1(Artifact):
    entry: BrokerProvenanceEntryV1
    previous_sha256: str
    sha256: str
    KIND: ClassVar[str] = "link"

    def _validate(self) -> None:
        digest(self.previous_sha256)
        digest(self.sha256)


@dataclass(frozen=True, slots=True)
class BrokerProvenanceCheckpointV1(Artifact):
    header_id: str
    entry_count: int
    root_sha256: str
    epoch: int
    KIND: ClassVar[str] = "checkpoint"

    def _validate(self) -> None:
        _artifact(self.header_id, "header")
        _count(self.entry_count, 1_000_000, 1)
        _count(self.epoch, 999_999)
        digest(self.root_sha256)


@dataclass(frozen=True, slots=True)
class BrokerProvenanceTerminalV1(Artifact):
    header_id: str
    stopped_at_utc_ns: int
    stopped_at_monotonic_ns: int
    native_manifest_id: str
    native_manifest_sha256: str
    health_audit_id: str
    health_audit_sha256: str
    permission_execution_id: str | None
    permission_execution_sha256: str | None
    native_complete: bool
    observations_complete: bool
    KIND: ClassVar[str] = "terminal"

    def _validate(self) -> None:
        _artifact(self.header_id, "header")
        _count(self.stopped_at_utc_ns)
        _count(self.stopped_at_monotonic_ns)
        _identity(self.native_manifest_id)
        _identity(self.health_audit_id)
        digest(self.native_manifest_sha256)
        digest(self.health_audit_sha256)
        if (self.permission_execution_id is None) != (
            self.permission_execution_sha256 is None
        ):
            raise ValueError("partial permission execution identity")
        if self.permission_execution_id is not None:
            _identity(self.permission_execution_id)
        if self.permission_execution_sha256 is not None:
            digest(self.permission_execution_sha256)


@dataclass(frozen=True, slots=True)
class BrokerProvenanceSealV1(Artifact):
    header_id: str
    terminal: BrokerProvenanceTerminalV1
    root_sha256: str
    entry_count: int
    checkpoint_count: int
    checkpoint_sha256: str
    last_epoch: int
    KIND: ClassVar[str] = "seal"

    def _validate(self) -> None:
        _artifact(self.header_id, "header")
        if self.terminal.header_id != self.header_id:
            raise ValueError("seal terminal header differs")
        digest(self.root_sha256)
        digest(self.checkpoint_sha256)
        _count(self.entry_count, 1_000_000, 1)
        _count(self.checkpoint_count, 65_536, 1)
        _count(self.last_epoch, 999_999)


@dataclass(frozen=True, slots=True)
class BrokerProvenanceVerificationV1:
    """Structural result only; not a policy grant or scientific admission."""

    header: BrokerProvenanceHeaderV1
    seal: BrokerProvenanceSealV1 | None
    reason: BrokerProvenanceVerificationReason
    root_sha256: str
    entry_count: int
    checkpoint_count: int

    @property
    def anchored(self) -> bool:
        return self.reason is BrokerProvenanceVerificationReason.VERIFIED

    @property
    def complete(self) -> bool:
        return self.anchored

    @property
    def structurally_complete(self) -> bool:
        return self.reason in (
            BrokerProvenanceVerificationReason.VERIFIED,
            BrokerProvenanceVerificationReason.UNANCHORED,
        )
