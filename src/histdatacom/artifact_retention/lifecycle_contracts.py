"""Closed append-only lifecycle observations, never caller deletion authority."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import ClassVar, Literal

from .canonical import RetentionContract
from .contracts import (
    MAX_PAYLOAD_BYTES,
    CollectionOutcomeV1,
    CollectionReceiptV1,
    _digest,
    _object_id,
    _object_ids,
    _text,
    _time,
    _token,
)

Operation = Literal[
    "ingest_ascii_source", "produce_histdata_cache", "publish_histdata_catalog"
]
JournalEvent = Literal[
    "created",
    "operation_begin",
    "descriptor",
    "admitted",
    "completed",
    "aborted",
    "root",
    "hold",
    "plan",
    "apply_started",
    "unlink_intent",
    "unlink_observed",
    "tombstone",
    "collection_receipt",
]
_CONTROL_PATH = re.compile(
    r"(?:descriptors|holds|plans|receipts|recipes|roots|transactions)/[0-9a-f]{64}\.json\Z"
)
_PAYLOAD_PATH = re.compile(r"objects/[0-9a-f]{32}\.(?:csv|data|json|scratch)\Z")


def _identity(value: str, kind: str) -> None:
    if not re.fullmatch(f"retention-{kind}:sha256:[0-9a-f]{{64}}", value):
        raise ValueError("invalid exact lifecycle identity")


def _workspace(value: str | None) -> None:
    if value is not None:
        _text(value, maximum=4096)
        if not value.startswith("/") or ".." in value.split("/"):
            raise ValueError("invalid generated native workspace locator")


@dataclass(frozen=True, slots=True)
class StoreCreationV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.store-creation.v1"
    KIND: ClassVar[str] = "store-creation"
    store_id: str
    marker_sha256: str
    policy_id: str
    recorded_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _digest(self.marker_sha256)
        _identity(self.policy_id, "policy")
        _time(self.recorded_ns)


@dataclass(frozen=True, slots=True)
class OperationBeginV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.operation-begin.v1"
    KIND: ClassVar[str] = "operation-begin"
    store_id: str
    transaction_id: str
    operation: Operation
    input_object_ids: tuple[str, ...]
    scratch_object_ids: tuple[str, ...]
    external_workspace: str | None
    started_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _token(self.transaction_id)
        _object_ids(self.input_object_ids)
        _object_ids(self.scratch_object_ids)
        _workspace(self.external_workspace)
        _time(self.started_ns)
        if self.operation == "ingest_ascii_source":
            if (
                self.input_object_ids
                or self.scratch_object_ids
                or self.external_workspace
            ):
                raise ValueError("source ingestion has no native work scratch")
        elif (
            len(self.scratch_object_ids) != 1 or self.external_workspace is None
        ):
            raise ValueError(
                "producer operation requires its closed work record"
            )


@dataclass(frozen=True, slots=True)
class ManagedTransactionScratchV1(RetentionContract):
    """A store-emitted work record, consumed by actual terminal publication.

    Input IDs name the historical transaction inputs, not future live byte edges
    after completion. External workspace is provenance, not a deletion target.
    """

    SCHEMA: ClassVar[str] = "histdatacom.retention.transaction-scratch.v1"
    KIND: ClassVar[str] = "transaction-scratch"
    store_id: str
    transaction_id: str
    operation: Literal["produce_histdata_cache", "publish_histdata_catalog"]
    input_object_ids: tuple[str, ...]
    external_workspace: str
    started_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _token(self.transaction_id)
        _object_ids(self.input_object_ids)
        if not self.input_object_ids:
            raise ValueError("native transaction needs actual input identities")
        _workspace(self.external_workspace)
        _time(self.started_ns)


@dataclass(frozen=True, slots=True)
class AdmissionReceiptV1(RetentionContract):
    """Observed native publication, not scientific qualification or a live root."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.admission-receipt.v1"
    KIND: ClassVar[str] = "admission-receipt"
    store_id: str
    transaction_id: str
    operation: Operation
    primary_object_id: str
    output_object_ids: tuple[str, ...]
    scratch_object_ids: tuple[str, ...]
    verification_sha256: str
    started_ns: int
    completed_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _token(self.transaction_id)
        _object_id(self.primary_object_id)
        _object_ids(self.output_object_ids)
        _object_ids(self.scratch_object_ids)
        _digest(self.verification_sha256)
        _time(self.started_ns)
        _time(self.completed_ns)
        if self.completed_ns < self.started_ns:
            raise ValueError("admission completion clock regressed")
        if self.primary_object_id not in self.output_object_ids or set(
            self.output_object_ids
        ) & set(self.scratch_object_ids):
            raise ValueError("admission outputs and work inventory differ")


@dataclass(frozen=True, slots=True)
class OperationAbortedV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.operation-aborted.v1"
    KIND: ClassVar[str] = "operation-aborted"
    store_id: str
    transaction_id: str
    scratch_object_ids: tuple[str, ...]
    verification_sha256: str
    detail: str
    completed_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _token(self.transaction_id)
        _object_ids(self.scratch_object_ids)
        _digest(self.verification_sha256)
        _text(self.detail)
        _time(self.completed_ns)


@dataclass(frozen=True, slots=True)
class RootRegistrationV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.root-registration.v1"
    KIND: ClassVar[str] = "root-registration"
    store_id: str
    object_id: str
    reason: str
    verification_sha256: str
    recorded_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _object_id(self.object_id)
        _text(self.reason)
        _digest(self.verification_sha256)
        _time(self.recorded_ns)


@dataclass(frozen=True, slots=True)
class ApplyStartedV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.apply-started.v1"
    KIND: ClassVar[str] = "apply-started"
    store_id: str
    plan_id: str
    snapshot_id: str
    candidate_object_ids: tuple[str, ...]
    started_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _identity(self.plan_id, "collection-plan")
        _identity(self.snapshot_id, "snapshot")
        _object_ids(self.candidate_object_ids)
        _time(self.started_ns)


@dataclass(frozen=True, slots=True)
class PayloadObservationV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.payload-observation.v1"
    KIND: ClassVar[str] = "payload-observation"
    relative_path: str
    sha256: str
    size_bytes: int
    device: int
    inode: int
    mode: int
    mtime_ns: int
    ctime_ns: int

    def _validate(self) -> None:
        if not _PAYLOAD_PATH.fullmatch(self.relative_path):
            raise ValueError("invalid observed managed payload path")
        _digest(self.sha256)
        if (
            not 0 <= self.size_bytes <= MAX_PAYLOAD_BYTES
            or self.device < 0
            or self.inode <= 0
            or not 0 < self.mode <= 0o177777
        ):
            raise ValueError("invalid physical payload observation")
        _time(self.mtime_ns)
        _time(self.ctime_ns)


@dataclass(frozen=True, slots=True)
class UnlinkIntentV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.unlink-intent.v1"
    KIND: ClassVar[str] = "unlink-intent"
    store_id: str
    plan_id: str
    object_id: str
    observation: PayloadObservationV1
    started_ns: int

    def _validate(self) -> None:
        _token(self.store_id)
        _identity(self.plan_id, "collection-plan")
        _object_id(self.object_id)
        _time(self.started_ns)


@dataclass(frozen=True, slots=True)
class UnlinkObservedV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.unlink-observed.v1"
    KIND: ClassVar[str] = "unlink-observed"
    store_id: str
    plan_id: str
    object_id: str
    intent_id: str
    outcome: Literal["unlinked", "indeterminate"]
    size_bytes_unlinked: int
    observed_ns: int
    detail: str

    def _validate(self) -> None:
        _token(self.store_id)
        _identity(self.plan_id, "collection-plan")
        _identity(self.intent_id, "unlink-intent")
        _object_id(self.object_id)
        _time(self.observed_ns)
        _text(self.detail)
        if not 0 <= self.size_bytes_unlinked <= MAX_PAYLOAD_BYTES:
            raise ValueError("invalid observed unlink byte count")
        if self.outcome != "unlinked" and self.size_bytes_unlinked:
            raise ValueError("indeterminate unlink cannot claim byte count")


@dataclass(frozen=True, slots=True)
class JournalEntryV1(RetentionContract):
    """Contiguous exact control chain; not an external signed transparency log."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.journal-entry.v1"
    KIND: ClassVar[str] = "journal-entry"
    store_id: str
    sequence: int
    previous_entry_id: str | None
    recorded_ns: int
    event: JournalEvent
    record_path: str
    record_sha256: str
    record_id: str

    def _validate(self) -> None:
        _token(self.store_id)
        if not 0 <= self.sequence < 16_384:
            raise ValueError("journal sequence exceeds closed store bound")
        if self.sequence == 0:
            if self.previous_entry_id is not None or self.event != "created":
                raise ValueError("journal genesis must be store creation")
        else:
            if self.previous_entry_id is None or self.event == "created":
                raise ValueError("journal link/genesis mismatch")
            _identity(self.previous_entry_id, "journal-entry")
        _time(self.recorded_ns)
        if not _CONTROL_PATH.fullmatch(self.record_path):
            raise ValueError("journal references an unknown control path")
        _digest(self.record_sha256)
        if self.record_path.rsplit("/", 1)[1] != self.record_sha256 + ".json":
            raise ValueError("journal control filename/hash mismatch")
        if not re.fullmatch(
            r"retention-[a-z-]+:sha256:[0-9a-f]{64}", self.record_id
        ):
            raise ValueError("journal requires an exact control artifact ID")


@dataclass(frozen=True, slots=True)
class CollectionInterruptedEvidenceV1(RetentionContract):
    """Actual incomplete evidence, including unnormalized clock observations."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.collection-interrupted.v1"
    KIND: ClassVar[str] = "collection-interrupted"
    store_id: str
    plan_id: str
    outcomes: tuple[CollectionOutcomeV1, ...]
    observed_time_ns: int | None
    receipt: CollectionReceiptV1 | None
    receipt_persisted: bool
    detail: str
    status: Literal["interrupted"] = "interrupted"
    unaccepted_receipt_id: str | None = None

    def _validate(self) -> None:
        _token(self.store_id)
        _identity(self.plan_id, "collection-plan")
        _object_ids(tuple(item.object_id for item in self.outcomes))
        _text(self.detail)
        if self.unaccepted_receipt_id is not None:
            _identity(self.unaccepted_receipt_id, "collection-receipt")
            if self.receipt is not None or not self.receipt_persisted:
                raise ValueError(
                    "unaccepted receipt identity requires confirmed publication, not acceptance"
                )
        if (
            self.receipt_persisted
            and self.receipt is None
            and self.unaccepted_receipt_id is None
        ):
            raise ValueError("missing receipt cannot be reported persisted")
        if self.receipt is not None and (
            self.receipt.store_id != self.store_id
            or self.receipt.plan_id != self.plan_id
            or self.receipt.outcomes != self.outcomes
            or self.receipt.status == "complete"
        ):
            raise ValueError(
                "interrupted evidence cannot relabel a complete/different receipt"
            )
