"""Closed retention evidence contracts; only locked store APIs grant authority.

These records can be constructed for inspection and synthetic graph tests. A
caller-authored record, hash or successful decode never grants deletion rights.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import ClassVar, Literal

from .canonical import MAX_INTEGER, RetentionContract

MAX_OBJECTS = 1024
MAX_EDGES = 8192
MAX_ROOTS = 128
MAX_PAYLOAD_BYTES = 256 * 1024 * 1024
MAX_GRAPH_DEPTH = 64
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_TOKEN = re.compile(r"[0-9a-f]{32}\Z")
_OBJECT_ID = re.compile(r"retention-object:sha256:[0-9a-f]{64}\Z")
_PAYLOAD_PATH = re.compile(r"objects/[0-9a-f]{32}\.(?:csv|data|json|scratch)\Z")


def _digest(value: str) -> None:
    if not _DIGEST.fullmatch(value):
        raise ValueError("expected an exact lowercase SHA-256 digest")


def _token(value: str) -> None:
    if not _TOKEN.fullmatch(value):
        raise ValueError("expected a store-generated 128-bit token")


def _object_id(value: str) -> None:
    if not _OBJECT_ID.fullmatch(value):
        raise ValueError("expected an exact retention object ID")


def _time(value: int) -> None:
    if not 0 < value <= MAX_INTEGER:
        raise ValueError("retention time must be positive nanoseconds")


def _text(value: str, *, maximum: int = 512) -> None:
    if not value or len(value) > maximum or any(ord(c) < 32 for c in value):
        raise ValueError("expected bounded nonempty retention text")


def _ordered(values: tuple[str, ...], *, maximum: int = MAX_OBJECTS) -> None:
    if len(values) > maximum or tuple(sorted(set(values))) != values:
        raise ValueError("retention members must be unique, sorted and bounded")


def _object_ids(values: tuple[str, ...], *, maximum: int = MAX_OBJECTS) -> None:
    _ordered(values, maximum=maximum)
    for value in values:
        _object_id(value)


class RetentionClass(str, Enum):
    """Classification never supersedes native dependencies or a hold."""

    IMMUTABLE_SOURCE = "immutable_source"
    RELEASE_CERTIFICATION = "release_certification"
    REPRODUCIBILITY_DEPENDENCY = "reproducibility_dependency"
    MIGRATION_PREDECESSOR = "migration_predecessor"
    PUBLISHED_DERIVED = "published_derived"
    RESTRICTED_PROVIDER = "restricted_provider"
    REPLACEABLE_CACHE = "replaceable_cache"
    SCRATCH = "scratch"
    QUARANTINED_CORRUPT = "quarantined_corrupt"
    LEGAL_POLICY_HOLD = "legal_policy_hold"


class DependencyRelation(str, Enum):
    """Every listed edge requires live target bytes, irrespective of its name."""

    NATIVE_ARTIFACT = "native_artifact"
    SOURCE = "source"
    RECIPE = "recipe"
    MIGRATION_PREDECESSOR = "migration_predecessor"
    PACKED_MEMBER = "packed_member"
    VERIFICATION_EVIDENCE = "verification_evidence"


@dataclass(frozen=True, slots=True)
class RetentionPolicyV1(RetentionContract):
    """Immutable policy with fixed bounded-store limits and a scratch TTL."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.policy.v1"
    KIND: ClassVar[str] = "policy"
    scratch_ttl_ns: int = 86_400_000_000_000

    def _validate(self) -> None:
        if not 0 <= self.scratch_ttl_ns <= 365 * 86_400_000_000_000:
            raise ValueError("scratch TTL must be between zero and 365 days")


@dataclass(frozen=True, slots=True)
class StoreMarkerV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.store.v1"
    KIND: ClassVar[str] = "store"
    store_id: str
    absolute_path: str
    device: int
    inode: int
    created_ns: int
    policy: RetentionPolicyV1
    state: Literal["initializing", "ready"]

    def _validate(self) -> None:
        _token(self.store_id)
        _text(self.absolute_path, maximum=4096)
        if not self.absolute_path.startswith("/"):
            raise ValueError("retention store requires an absolute POSIX path")
        if self.device < 0 or self.inode <= 0:
            raise ValueError("invalid retention store filesystem identity")
        _time(self.created_ns)


@dataclass(frozen=True, slots=True)
class ManagedPayloadRefV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.payload.v1"
    KIND: ClassVar[str] = "payload"
    relative_path: str
    sha256: str
    size_bytes: int

    def _validate(self) -> None:
        if not _PAYLOAD_PATH.fullmatch(self.relative_path):
            raise ValueError("unsafe or unknown managed payload path")
        _digest(self.sha256)
        if not 0 <= self.size_bytes <= MAX_PAYLOAD_BYTES:
            raise ValueError("managed payload size exceeds bounds")


@dataclass(frozen=True, slots=True)
class DependencyV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.dependency.v1"
    KIND: ClassVar[str] = "dependency"
    target_object_id: str
    relation: DependencyRelation

    def _validate(self) -> None:
        _object_id(self.target_object_id)


@dataclass(frozen=True, slots=True)
class NativeObjectV1(RetentionContract):
    """Immutable output of a closed native adapter, not a graph-import API."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.object.v1"
    KIND: ClassVar[str] = "object"
    adapter_id: str
    native_schema: str
    native_id: str
    payload_ref: ManagedPayloadRefV1
    retention_class: RetentionClass
    live_dependencies: tuple[DependencyV1, ...]
    historical_subject_ids: tuple[str, ...]
    transaction_id: str
    created_ns: int

    def _validate(self) -> None:
        for value in (self.adapter_id, self.native_schema, self.native_id):
            _text(value)
        _token(self.transaction_id)
        _time(self.created_ns)
        keys = tuple(
            (edge.target_object_id, edge.relation.value)
            for edge in self.live_dependencies
        )
        if len(keys) > MAX_EDGES or tuple(sorted(set(keys))) != keys:
            raise ValueError("dependencies must be unique, sorted and bounded")
        _ordered(self.historical_subject_ids)
        for value in self.historical_subject_ids:
            _text(value)


@dataclass(frozen=True, slots=True)
class AsciiTickRecipeV1(RetentionContract):
    """Closed ASCII/T recipe; source and executable identity are mandatory."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.ascii-tick-recipe.v1"
    KIND: ClassVar[str] = "ascii-tick-recipe"
    raw_source_object_id: str
    symbol: str
    period: str
    implementation_sha256: str
    backend_id: str

    def _validate(self) -> None:
        _object_id(self.raw_source_object_id)
        if not re.fullmatch(r"[A-Z]{6}", self.symbol):
            raise ValueError("ASCII tick recipe requires a six-letter symbol")
        if not re.fullmatch(r"(?:19|20)[0-9]{2}(?:0[1-9]|1[0-2])", self.period):
            raise ValueError("ASCII tick recipe requires a YYYYMM period")
        _digest(self.implementation_sha256)
        _text(self.backend_id)


@dataclass(frozen=True, slots=True)
class CacheRegenerationV1(RetentionContract):
    """Stable proof content; actual execution times belong in the store journal.

    Output digests identify a historical result, not a requires-bytes edge to
    the disposable cache. The source and recipe remain live dependencies.
    """

    SCHEMA: ClassVar[str] = "histdatacom.retention.cache-regeneration.v1"
    KIND: ClassVar[str] = "cache-regeneration"
    recipe_object_id: str
    source_object_id: str
    cache_object_id: str
    historical_output_sha256: str
    historical_output_size_bytes: int
    rebuilt_sha256: str
    rebuilt_size_bytes: int
    native_partition_id: str
    native_readback_sha256: str
    implementation_sha256: str
    backend_id: str

    def _validate(self) -> None:
        for value in (
            self.recipe_object_id,
            self.source_object_id,
            self.cache_object_id,
        ):
            _object_id(value)
        for value in (
            self.historical_output_sha256,
            self.rebuilt_sha256,
            self.native_readback_sha256,
            self.implementation_sha256,
        ):
            _digest(value)
        if (
            self.historical_output_sha256 != self.rebuilt_sha256
            or self.historical_output_size_bytes != self.rebuilt_size_bytes
            or not 0 < self.rebuilt_size_bytes <= MAX_PAYLOAD_BYTES
        ):
            raise ValueError("cache regeneration requires exact bounded bytes")
        _text(self.native_partition_id)
        _text(self.backend_id)


@dataclass(frozen=True, slots=True)
class TransactionCompletionV1(RetentionContract):
    """Emitted after native verified publication or a store-owned terminal abort.

    Published object IDs are historical outputs, not requires-bytes edges.
    A permanent catalog stays live through its own retention class; a proven
    orphan cache may be collected without invalidating its publication history.
    """

    SCHEMA: ClassVar[str] = "histdatacom.retention.transaction-completion.v1"
    KIND: ClassVar[str] = "transaction-completion"
    transaction_id: str
    outcome: Literal["published", "aborted"]
    scratch_object_ids: tuple[str, ...]
    published_object_ids: tuple[str, ...]
    verification_sha256: str
    completed_ns: int

    def _validate(self) -> None:
        _token(self.transaction_id)
        _object_ids(self.scratch_object_ids)
        _object_ids(self.published_object_ids, maximum=MAX_ROOTS)
        _digest(self.verification_sha256)
        _time(self.completed_ns)
        if bool(self.published_object_ids) != (self.outcome == "published"):
            raise ValueError(
                "published outcome requires exact published outputs"
            )


@dataclass(frozen=True, slots=True)
class RetentionHoldV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.hold.v1"
    KIND: ClassVar[str] = "hold"
    object_ids: tuple[str, ...]
    reason: str
    recorded_ns: int

    def _validate(self) -> None:
        _object_ids(self.object_ids)
        if not self.object_ids:
            raise ValueError("empty retention hold")
        _text(self.reason)
        _time(self.recorded_ns)


@dataclass(frozen=True, slots=True)
class CollectionTombstoneV1(RetentionContract):
    """A completed unlink observation, verified against the durable journal."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.collection-tombstone.v1"
    KIND: ClassVar[str] = "collection-tombstone"
    object_id: str
    plan_id: str
    journal_entry_id: str
    payload_sha256: str
    size_bytes_unlinked: int
    unlinked_ns: int

    def _validate(self) -> None:
        _object_id(self.object_id)
        if not re.fullmatch(
            r"retention-collection-plan:sha256:[0-9a-f]{64}", self.plan_id
        ):
            raise ValueError("invalid tombstone plan ID")
        _text(self.journal_entry_id)
        _digest(self.payload_sha256)
        if not 0 <= self.size_bytes_unlinked <= MAX_PAYLOAD_BYTES:
            raise ValueError("invalid observed tombstone size")
        _time(self.unlinked_ns)


@dataclass(frozen=True, slots=True)
class StoreSnapshotV1(RetentionContract):
    """Complete verified observation; apply never trusts a caller snapshot."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.snapshot.v1"
    KIND: ClassVar[str] = "snapshot"
    store_id: str
    policy: RetentionPolicyV1
    marker_sha256: str
    revision_sha256: str
    descriptors: tuple[NativeObjectV1, ...]
    present_object_ids: tuple[str, ...]
    root_object_ids: tuple[str, ...]
    holds: tuple[RetentionHoldV1, ...]
    completions: tuple[TransactionCompletionV1, ...]
    regenerations: tuple[CacheRegenerationV1, ...]
    tombstones: tuple[CollectionTombstoneV1, ...]
    blockers: tuple[str, ...]

    def _validate(self) -> None:
        _token(self.store_id)
        _digest(self.marker_sha256)
        _digest(self.revision_sha256)
        _object_ids(tuple(item.artifact_id for item in self.descriptors))
        if len(
            {item.payload_ref.relative_path for item in self.descriptors}
        ) != len(self.descriptors):
            raise ValueError("duplicate physical managed payload location")
        if (
            sum(item.payload_ref.size_bytes for item in self.descriptors)
            > MAX_PAYLOAD_BYTES
        ):
            raise ValueError(
                "managed inventory exceeds aggregate payload bound"
            )
        if (
            sum(len(item.live_dependencies) for item in self.descriptors)
            > MAX_EDGES
        ):
            raise ValueError("managed inventory exceeds edge bound")
        _object_ids(self.present_object_ids)
        _object_ids(self.root_object_ids, maximum=MAX_ROOTS)
        for records in (
            self.holds,
            self.completions,
            self.regenerations,
            self.tombstones,
        ):
            _ordered(tuple(item.artifact_id for item in records))
        _ordered(self.blockers)
        for value in self.blockers:
            _text(value, maximum=4096)


@dataclass(frozen=True, slots=True)
class CollectionDecisionV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.collection-decision.v1"
    KIND: ClassVar[str] = "collection-decision"
    object_id: str
    action: Literal["keep", "delete", "block", "already_collected"]
    reason: str
    protected_path: tuple[str, ...]

    def _validate(self) -> None:
        _object_id(self.object_id)
        _text(self.reason)
        if len(self.protected_path) > MAX_GRAPH_DEPTH + 1:
            raise ValueError("protected witness exceeds graph-depth bound")
        for value in self.protected_path:
            _object_id(value)
        if len(set(self.protected_path)) != len(self.protected_path):
            raise ValueError("cyclic protected witness")
        if self.protected_path and self.protected_path[-1] != self.object_id:
            raise ValueError("protected witness must end at the object")
        if self.action != "keep" and self.protected_path:
            raise ValueError("only a kept object has a protected witness")


@dataclass(frozen=True, slots=True)
class CollectionPlanV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.collection-plan.v1"
    KIND: ClassVar[str] = "collection-plan"
    store_id: str
    snapshot_id: str
    policy_id: str
    cutoff_ns: int
    decisions: tuple[CollectionDecisionV1, ...]
    blockers: tuple[str, ...]

    def _validate(self) -> None:
        _token(self.store_id)
        for value, kind in (
            (self.snapshot_id, "snapshot"),
            (self.policy_id, "policy"),
        ):
            if not re.fullmatch(
                f"retention-{kind}:sha256:[0-9a-f]{{64}}", value
            ):
                raise ValueError("invalid collection plan evidence ID")
        _time(self.cutoff_ns)
        _object_ids(tuple(item.object_id for item in self.decisions))
        _ordered(self.blockers)
        for value in self.blockers:
            _text(value, maximum=4096)
        if self.blockers and any(
            item.action == "delete" for item in self.decisions
        ):
            raise ValueError("a blocked plan cannot authorize any deletion")


@dataclass(frozen=True, slots=True)
class CollectionOutcomeV1(RetentionContract):
    SCHEMA: ClassVar[str] = "histdatacom.retention.collection-outcome.v1"
    KIND: ClassVar[str] = "collection-outcome"
    object_id: str
    outcome: Literal["unlinked", "failed", "indeterminate", "not_attempted"]
    size_bytes_unlinked: int
    journal_entry_id: str
    detail: str

    def _validate(self) -> None:
        _object_id(self.object_id)
        _text(self.journal_entry_id)
        _text(self.detail)
        if not 0 <= self.size_bytes_unlinked <= MAX_PAYLOAD_BYTES:
            raise ValueError("invalid observed unlink size")
        if self.outcome != "unlinked" and self.size_bytes_unlinked:
            raise ValueError("unknown or unattempted unlink cannot claim bytes")


@dataclass(frozen=True, slots=True)
class CollectionReceiptV1(RetentionContract):
    """Observed outcome, not atomicity, disk-space recovery or retry authority."""

    SCHEMA: ClassVar[str] = "histdatacom.retention.collection-receipt.v1"
    KIND: ClassVar[str] = "collection-receipt"
    store_id: str
    plan_id: str
    started_ns: int
    finished_ns: int
    status: Literal["complete", "partial", "indeterminate"]
    outcomes: tuple[CollectionOutcomeV1, ...]
    protected_replay: Literal["verified", "failed", "not_attempted"]

    def _validate(self) -> None:
        _token(self.store_id)
        if not re.fullmatch(
            r"retention-collection-plan:sha256:[0-9a-f]{64}", self.plan_id
        ):
            raise ValueError("invalid collection receipt plan ID")
        _time(self.started_ns)
        _time(self.finished_ns)
        if self.finished_ns < self.started_ns:
            raise ValueError("collection receipt clock regressed")
        _object_ids(tuple(item.object_id for item in self.outcomes))
        if self.status == "complete" and (
            self.protected_replay != "verified"
            or any(item.outcome != "unlinked" for item in self.outcomes)
        ):
            raise ValueError("incomplete observation cannot claim completion")
