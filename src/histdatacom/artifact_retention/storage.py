"""Closed managed retention lifecycle; serialized declarations are not authority.

Native replay uses newly generated unmanaged workspaces, retained after use.
This store is bounded over its lifetime (including historical descriptors),
not an unbounded database or a permission to remove those external workspaces.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile
import time
from typing import Any
import uuid

from .canonical import (
    MAX_WIRE_BYTES,
    RetentionContract,
    canonical_json,
    load_json,
    sha256,
)
from .contracts import (
    MAX_EDGES,
    MAX_OBJECTS,
    MAX_PAYLOAD_BYTES,
    MAX_ROOTS,
    AsciiTickRecipeV1,
    CacheRegenerationV1,
    CollectionPlanV1,
    CollectionReceiptV1,
    CollectionTombstoneV1,
    DependencyRelation,
    DependencyV1,
    ManagedPayloadRefV1,
    NativeObjectV1,
    RetentionClass,
    RetentionHoldV1,
    RetentionPolicyV1,
    StoreMarkerV1,
    StoreSnapshotV1,
    TransactionCompletionV1,
    _object_id,
    _object_ids,
    _text,
)
from .lifecycle_contracts import (
    AdmissionReceiptV1,
    ApplyStartedV1,
    JournalEntryV1,
    JournalEvent,
    ManagedTransactionScratchV1,
    Operation,
    OperationAbortedV1,
    OperationBeginV1,
    RootRegistrationV1,
    StoreCreationV1,
    UnlinkIntentV1,
    UnlinkObservedV1,
)
from . import native
from .planning import _derive_plan
from .secure_fs import (
    LOCK_NAME,
    MAX_CONTROL_BYTES,
    MAX_STORE_FILES,
    FileObservation,
    StoreSession,
    create_store_filesystem,
)
from histdatacom.managed_artifact_boundary import (
    MANAGED_ARTIFACT_MARKER,
    assert_unmanaged_mutation_paths,
)

MAX_NATIVE_EXECUTIONS = 16
MAX_PROJECTED_CACHE_BYTES = 1024 * 1024 * 1024
_NATIVE_AUXILIARY_BYTES = 10 * 1024 * 1024
_CONTROL_ALLOWANCE = 128 * 1024
_METADATA_HEADROOM = 8 * 1024 * 1024
_SCRATCH_ADAPTER = "managed-transaction-scratch.v1"
# Exclusion from operative identity is narrow, never exclusion from auditing.
_HISTORICAL_ONLY_EVENTS = frozenset({"plan", "collection_receipt"})
_EVENTS: dict[str, tuple[str, type[RetentionContract]]] = {
    "created": ("transactions", StoreCreationV1),
    "operation_begin": ("transactions", OperationBeginV1),
    "descriptor": ("descriptors", NativeObjectV1),
    "admitted": ("receipts", AdmissionReceiptV1),
    "completed": ("transactions", TransactionCompletionV1),
    "aborted": ("transactions", OperationAbortedV1),
    "root": ("roots", RootRegistrationV1),
    "hold": ("holds", RetentionHoldV1),
    "plan": ("plans", CollectionPlanV1),
    "apply_started": ("transactions", ApplyStartedV1),
    "unlink_intent": ("transactions", UnlinkIntentV1),
    "unlink_observed": ("receipts", UnlinkObservedV1),
    "tombstone": ("receipts", CollectionTombstoneV1),
    "collection_receipt": ("receipts", CollectionReceiptV1),
}
_SCHEMAS = {value[1].SCHEMA: value[1] for value in _EVENTS.values()}


class RetentionStoreError(ValueError):
    """Complete current managed authority could not be established."""


@dataclass(slots=True)
class _State:
    session: StoreSession
    observations: dict[str, FileObservation]
    records: dict[str, RetentionContract]
    journal: list[JournalEntryV1]

    @property
    def descriptors(self) -> tuple[NativeObjectV1, ...]:
        return tuple(
            sorted(
                (
                    record
                    for record in self.records.values()
                    if type(record) is NativeObjectV1
                ),
                key=lambda item: item.artifact_id,
            )
        )

    def of_type(self, cls: type) -> tuple[Any, ...]:
        return tuple(
            sorted(
                (
                    record
                    for record in self.records.values()
                    if type(record) is cls
                ),
                key=lambda item: item.artifact_id,
            )
        )

    def entry_for(self, identity: str) -> JournalEntryV1:
        matches = [
            entry for entry in self.journal if entry.record_id == identity
        ]
        if len(matches) != 1:
            raise RetentionStoreError(
                "control lacks exactly one journal witness"
            )
        return matches[0]


def _now(state: _State) -> int:
    value = time.time_ns()
    minimum = max(
        [
            state.session.marker.created_ns,
            *(entry.recorded_ns for entry in state.journal),
        ]
    )
    if type(value) is not int or not minimum <= value <= 2**63 - 1:
        raise RetentionStoreError("managed operation current clock regressed")
    return value


def _control_path(
    directory: str, record: RetentionContract
) -> tuple[str, bytes]:
    raw = record.to_json().encode("ascii")
    return f"{directory}/{sha256(raw)}.json", raw


def _append(
    state: _State, event: JournalEvent, record: RetentionContract
) -> JournalEntryV1:
    directory, kind = _EVENTS[event]
    if type(record) is not kind:
        raise RetentionStoreError("wrong exact lifecycle event record")
    path, raw = _control_path(directory, record)
    if path in state.observations:
        raise RetentionStoreError("lifecycle record is already published")
    observed_ns = _now(state)
    entry = JournalEntryV1(
        state.session.marker.store_id,
        len(state.journal),
        state.journal[-1].artifact_id if state.journal else None,
        observed_ns,
        event,
        path,
        sha256(raw),
        record.artifact_id,
    )
    journal_path, journal_raw = _control_path("journal", entry)
    _reserve_control(state, len(raw) + len(journal_raw), 2)
    # An exception leaves an orphan/partial member; never delete or adopt it.
    observation = state.session.write_immutable(path, raw)
    journal_observation = state.session.write_immutable(
        journal_path, journal_raw
    )
    state.observations[path] = observation
    state.observations[journal_path] = journal_observation
    state.records[path] = record
    state.journal.append(entry)
    return entry


def _reserve_control(state: _State, byte_count: int, file_count: int) -> None:
    control_bytes = sum(
        observation.size_bytes
        for path, observation in state.observations.items()
        if not path.startswith("objects/")
    )
    if (
        control_bytes + byte_count > MAX_CONTROL_BYTES
        or len(state.observations) + file_count > MAX_STORE_FILES
    ):
        raise RetentionStoreError("managed control lifetime capacity exceeded")


def _load_state(session: StoreSession) -> _State:
    observations = {item.relative_path: item for item in session.inventory()}
    records: dict[str, RetentionContract] = {}
    journal: list[JournalEntryV1] = []
    for path, observation in observations.items():
        if path in (MANAGED_ARTIFACT_MARKER, LOCK_NAME) or path.startswith(
            "objects/"
        ):
            continue
        if path.startswith(("tmp/", "recipes/")):
            raise RetentionStoreError(
                "uncommitted or unsupported managed control member"
            )
        raw = session.read_bytes(path)
        if (
            sha256(raw) != observation.sha256
            or path.rsplit("/", 1)[1] != sha256(raw) + ".json"
        ):
            raise RetentionStoreError("managed control identity changed")
        if path.startswith("journal/"):
            journal.append(JournalEntryV1.from_json(raw.decode("ascii")))
            continue
        value = load_json(raw.decode("ascii"))
        schema = value.get("schema_version") if type(value) is dict else None
        kind = _SCHEMAS.get(schema) if type(schema) is str else None
        if kind is None:
            raise RetentionStoreError("unknown managed control schema")
        records[path] = kind.from_json(raw.decode("ascii"))
    journal.sort(key=lambda item: item.sequence)
    used: set[str] = set()
    previous: JournalEntryV1 | None = None
    for sequence, entry in enumerate(journal):
        directory, kind = _EVENTS[entry.event]
        record = records.get(entry.record_path)
        if (
            entry.store_id != session.marker.store_id
            or entry.sequence != sequence
            or entry.previous_entry_id
            != (previous.artifact_id if previous else None)
            or entry.recorded_ns
            < (previous.recorded_ns if previous else session.marker.created_ns)
            or entry.record_path in used
            or type(record) is not kind
            or not entry.record_path.startswith(directory + "/")
            or entry.record_sha256 != observations[entry.record_path].sha256
            or entry.record_id != record.artifact_id
            or getattr(record, "store_id", session.marker.store_id)
            != session.marker.store_id
        ):
            raise RetentionStoreError(
                "broken, forked or foreign lifecycle journal"
            )
        used.add(entry.record_path)
        assert record is not None
        for attribute in (
            "created_ns",
            "recorded_ns",
            "started_ns",
            "completed_ns",
            "observed_ns",
            "unlinked_ns",
            "finished_ns",
        ):
            recorded_time = getattr(record, attribute, None)
            if (
                recorded_time is not None
                and not session.marker.created_ns
                <= recorded_time
                <= entry.recorded_ns
            ):
                raise RetentionStoreError(
                    "lifecycle record time is outside its real journal observation"
                )
        previous = entry
    if not journal or used != set(records):
        raise RetentionStoreError(
            "missing genesis or uncommitted control record"
        )
    genesis = records[journal[0].record_path]
    if (
        type(genesis) is not StoreCreationV1
        or genesis.marker_sha256 != observations[MANAGED_ARTIFACT_MARKER].sha256
        or genesis.policy_id != session.marker.policy.artifact_id
    ):
        raise RetentionStoreError(
            "store creation does not bind immutable marker"
        )
    state = _State(session, observations, records, journal)
    descriptors = state.descriptors
    if (
        len(descriptors) > MAX_OBJECTS
        or sum(item.payload_ref.size_bytes for item in descriptors)
        > MAX_PAYLOAD_BYTES
        or sum(len(item.live_dependencies) for item in descriptors) > MAX_EDGES
        or len({item.object_id for item in state.of_type(RootRegistrationV1)})
        > MAX_ROOTS
    ):
        raise RetentionStoreError(
            "managed lifetime inventory exceeds closed bounds"
        )
    _now(state)
    return state


def _catalog_cost(state: _State, item: NativeObjectV1) -> int:
    raw = state.session.read_bytes(
        item.payload_ref.relative_path, maximum=native.MAX_NATIVE_JSON_BYTES
    )
    payload = native._load_native(raw)
    versions = payload.get("versions")
    if (
        type(versions) is not list
        or len(versions) != 1
        or type(versions[0]) is not dict
    ):
        raise RetentionStoreError("unsupported catalog topology")
    partitions = versions[0].get("partitions")
    if (
        type(partitions) is not list
        or not 0 < len(partitions) <= native.MAX_PARTITIONS
    ):
        raise RetentionStoreError("unsupported native partition topology")
    return len(partitions)


def _native_cost(state: _State, item: NativeObjectV1) -> int:
    if item.adapter_id in (native.CACHE_ADAPTER, native.REGENERATION_ADAPTER):
        return 1
    if item.adapter_id == native.CATALOG_ADAPTER:
        return _catalog_cost(state, item)
    if item.adapter_id in (
        native.ASCII_SOURCE_ADAPTER,
        native.ASCII_RECIPE_ADAPTER,
        _SCRATCH_ADAPTER,
    ):
        return 0
    raise RetentionStoreError("unsupported native adapter; work bound unknown")


def _inspection_cost(state: _State) -> int:
    return sum(
        _native_cost(state, item)
        for item in state.descriptors
        if item.payload_ref.relative_path in state.observations
    )


class _NativeBudget:
    """Prospective execution/disk bound, not an OS reservation or RSS estimate."""

    def __init__(
        self,
        state: _State,
        executions: int,
        *,
        control_bytes: int = MAX_WIRE_BYTES,
        control_files: int = 8,
    ) -> None:
        if (
            type(executions) is not int
            or not 0 <= executions <= MAX_NATIVE_EXECUTIONS
            or executions * native.MAX_CACHE_BYTES > MAX_PROJECTED_CACHE_BYTES
        ):
            raise RetentionStoreError(
                "native execution or projected-cache budget exceeded"
            )
        _reserve_control(state, control_bytes, control_files)
        self.limit = executions
        self.used = 0
        self.root = Path(state.session.path)
        self.store_id = state.session.marker.store_id
        self.workspace: Path | None = None
        self.actual_bytes = 0
        projected = (
            executions * (native.MAX_CACHE_BYTES + _NATIVE_AUXILIARY_BYTES)
            + control_bytes
            + _METADATA_HEADROOM
        )
        if shutil.disk_usage(self.root.parent).free < projected:
            raise RetentionStoreError(
                "insufficient native workspace/control headroom"
            )

    def ensure_workspace(self) -> Path:
        if self.workspace is None:
            # This outer directory is itself a write, before the native
            # producer's independently guarded child workspace exists.
            assert_unmanaged_mutation_paths((self.root.parent,))
            self.workspace = Path(
                tempfile.mkdtemp(
                    prefix=f".retention-work-{self.store_id}-",
                    dir=self.root.parent,
                )
            )
        return self.workspace

    def invoke(self, cost: int, function: Any) -> Any:
        if type(cost) is not int or cost < 0 or self.used + cost > self.limit:
            raise RetentionStoreError(
                "native execution exceeded prospective topology"
            )
        self.used += cost
        if cost:
            self.ensure_workspace()
        path = (self.workspace or self.root.parent) / (
            "native-" + uuid.uuid4().hex
        )
        try:
            return function(path)
        finally:
            if path.exists():
                total = entries = 0
                pending = [path]
                while pending:
                    current = pending.pop()
                    info = current.lstat()
                    entries += 1
                    if entries > max(1, cost) * 4096 or stat.S_ISLNK(
                        info.st_mode
                    ):
                        raise RetentionStoreError(
                            "native workspace exceeded admitted shape"
                        )
                    if stat.S_ISDIR(info.st_mode):
                        with os.scandir(current) as children:
                            for child in children:
                                if (
                                    entries + len(pending)
                                    >= max(1, cost) * 4096
                                ):
                                    raise RetentionStoreError(
                                        "native workspace entry bound exceeded"
                                    )
                                pending.append(Path(child.path))
                    elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
                        total += info.st_size
                    else:
                        raise RetentionStoreError(
                            "native workspace has unsupported member"
                        )
                if total > cost * (
                    native.MAX_CACHE_BYTES + _NATIVE_AUXILIARY_BYTES
                ):
                    raise RetentionStoreError(
                        "native workspace actual byte bound exceeded"
                    )
                self.actual_bytes += total
                if self.actual_bytes > self.limit * (
                    native.MAX_CACHE_BYTES + _NATIVE_AUXILIARY_BYTES
                ):
                    raise RetentionStoreError(
                        "aggregate native workspace byte bound exceeded"
                    )


def _lifecycle_blockers(state: _State) -> set[str]:
    blockers: set[str] = set()
    objects = {item.artifact_id: item for item in state.descriptors}
    begins: dict[str, OperationBeginV1] = {}
    for begin in state.of_type(OperationBeginV1):
        if begin.transaction_id in begins:
            raise RetentionStoreError("duplicate managed transaction begin")
        begins[begin.transaction_id] = begin
    admissions = {
        item.transaction_id: item for item in state.of_type(AdmissionReceiptV1)
    }
    aborts = {
        item.transaction_id: item for item in state.of_type(OperationAbortedV1)
    }
    completions = {
        item.transaction_id: item
        for item in state.of_type(TransactionCompletionV1)
    }
    for cls, mapping in (
        (AdmissionReceiptV1, admissions),
        (OperationAbortedV1, aborts),
        (TransactionCompletionV1, completions),
    ):
        if (
            len(mapping) != len(state.of_type(cls))
            or set(mapping) - begins.keys()
        ):
            raise RetentionStoreError(
                "ambiguous or unbegun transaction terminal"
            )
    if set(admissions) & set(aborts):
        raise RetentionStoreError(
            "transaction has conflicting terminal records"
        )
    for item in objects.values():
        if item.transaction_id not in begins:
            raise RetentionStoreError(
                "descriptor lacks a store-owned transaction"
            )
        begin = begins[item.transaction_id]
        if (
            item.created_ns < begin.started_ns
            or state.entry_for(item.artifact_id).sequence
            <= state.entry_for(begin.artifact_id).sequence
        ):
            raise RetentionStoreError(
                "descriptor precedes its transaction begin"
            )
    for transaction_id, begin in begins.items():
        owned = {
            item.artifact_id
            for item in objects.values()
            if item.transaction_id == transaction_id
        }
        admission, abort, completion = (
            admissions.get(transaction_id),
            aborts.get(transaction_id),
            completions.get(transaction_id),
        )
        if admission is None and abort is None:
            blockers.add(f"pending_transaction:{transaction_id}")
            continue
        terminal = admission or abort
        assert terminal is not None
        if (
            terminal.completed_ns < begin.started_ns
            or terminal.scratch_object_ids != begin.scratch_object_ids
        ):
            raise RetentionStoreError(
                "transaction timing/scratch terminal mismatch"
            )
        if admission:
            if (
                admission.operation != begin.operation
                or admission.started_ns != begin.started_ns
                or owned
                != set(admission.output_object_ids)
                | set(begin.scratch_object_ids)
            ):
                raise RetentionStoreError(
                    "admission does not cover exact transaction outputs"
                )
            outputs = tuple(
                objects[identity] for identity in admission.output_object_ids
            )
            adapters = sorted(item.adapter_id for item in outputs)
            expected = {
                "ingest_ascii_source": [native.ASCII_SOURCE_ADAPTER],
                "produce_histdata_cache": sorted(
                    [
                        native.ASCII_RECIPE_ADAPTER,
                        native.CACHE_ADAPTER,
                        native.REGENERATION_ADAPTER,
                    ]
                ),
                "publish_histdata_catalog": [native.CATALOG_ADAPTER],
            }[begin.operation]
            primary_adapter = {
                "ingest_ascii_source": native.ASCII_SOURCE_ADAPTER,
                "produce_histdata_cache": native.CACHE_ADAPTER,
                "publish_histdata_catalog": native.CATALOG_ADAPTER,
            }[begin.operation]
            if (
                adapters != expected
                or objects[admission.primary_object_id].adapter_id
                != primary_adapter
            ):
                raise RetentionStoreError(
                    "closed operation output family differs"
                )
            if begin.operation == "produce_histdata_cache":
                recipe_object = next(
                    item
                    for item in outputs
                    if item.adapter_id == native.ASCII_RECIPE_ADAPTER
                )
                cache_object = objects[admission.primary_object_id]
                proof_object = next(
                    item
                    for item in outputs
                    if item.adapter_id == native.REGENERATION_ADAPTER
                )
                if (
                    len(begin.input_object_ids) != 1
                    or recipe_object.live_dependencies
                    != _edges(
                        (begin.input_object_ids[0], DependencyRelation.SOURCE)
                    )
                    or cache_object.live_dependencies
                    != _edges(
                        (begin.input_object_ids[0], DependencyRelation.SOURCE),
                        (recipe_object.artifact_id, DependencyRelation.RECIPE),
                    )
                    or proof_object.live_dependencies
                    != cache_object.live_dependencies
                    or proof_object.historical_subject_ids
                    != (cache_object.artifact_id,)
                ):
                    raise RetentionStoreError(
                        "native cache outputs differ from exact transaction inputs"
                    )
            elif begin.operation == "publish_histdata_catalog":
                actual_caches = tuple(
                    sorted(
                        edge.target_object_id
                        for edge in objects[
                            admission.primary_object_id
                        ].live_dependencies
                        if edge.relation is DependencyRelation.NATIVE_ARTIFACT
                    )
                )
                if actual_caches != begin.input_object_ids:
                    raise RetentionStoreError(
                        "catalog output differs from exact transaction selection"
                    )
        elif owned != set(begin.scratch_object_ids):
            raise RetentionStoreError(
                "closed abort has unexplained published outputs"
            )
        if begin.operation != "ingest_ascii_source":
            if completion is None:
                blockers.add(f"missing_native_completion:{transaction_id}")
            elif (
                completion.scratch_object_ids != begin.scratch_object_ids
                or completion.completed_ns != terminal.completed_ns
                or completion.verification_sha256
                != terminal.verification_sha256
                or completion.outcome
                != ("published" if admission else "aborted")
                or completion.published_object_ids
                != ((admission.primary_object_id,) if admission else ())
            ):
                raise RetentionStoreError(
                    "native completion differs from actual admission/abort"
                )
        elif completion is not None:
            raise RetentionStoreError(
                "source ingestion has unexpected producer completion"
            )
        terminal_sequence = state.entry_for(terminal.artifact_id).sequence
        begin_sequence = state.entry_for(begin.artifact_id).sequence
        if terminal_sequence <= begin_sequence or any(
            not begin_sequence
            < state.entry_for(identity).sequence
            < terminal_sequence
            or objects[identity].created_ns > terminal.completed_ns
            for identity in owned
        ):
            raise RetentionStoreError(
                "transaction terminal precedes exact owned descriptors"
            )
        if (
            completion is not None
            and not max(
                [
                    begin_sequence,
                    *(state.entry_for(identity).sequence for identity in owned),
                ]
            )
            < state.entry_for(completion.artifact_id).sequence
            < terminal_sequence
        ):
            raise RetentionStoreError(
                "completion is outside native publication sequence"
            )
        for identity in begin.input_object_ids:
            if (
                identity not in objects
                or state.entry_for(identity).sequence >= begin_sequence
            ):
                raise RetentionStoreError(
                    "transaction names unavailable or later inputs"
                )
        if begin.operation == "produce_histdata_cache" and (
            len(begin.input_object_ids) != 1
            or objects[begin.input_object_ids[0]].adapter_id
            != native.ASCII_SOURCE_ADAPTER
        ):
            raise RetentionStoreError(
                "cache operation has different native input family"
            )
        if begin.operation == "publish_histdata_catalog" and (
            not begin.input_object_ids
            or any(
                objects[identity].adapter_id != native.CACHE_ADAPTER
                for identity in begin.input_object_ids
            )
        ):
            raise RetentionStoreError(
                "catalog operation has different native input family"
            )
    payloads = {
        path for path in state.observations if path.startswith("objects/")
    }
    expected_payloads = {
        item.payload_ref.relative_path for item in objects.values()
    }
    if payloads - expected_payloads:
        blockers.add("unregistered_live_payload")
    for record in (
        *state.of_type(RootRegistrationV1),
        *state.of_type(RetentionHoldV1),
    ):
        identities = (
            (record.object_id,)
            if type(record) is RootRegistrationV1
            else record.object_ids
        )
        for identity in identities:
            if (
                identity not in objects
                or state.entry_for(identity).sequence
                >= state.entry_for(record.artifact_id).sequence
                or objects[identity].created_ns > record.recorded_ns
            ):
                raise RetentionStoreError(
                    "root/hold predates or invents its managed object"
                )
    _verify_collection_history(state, objects, blockers)
    return blockers


def _verify_collection_history(
    state: _State, objects: dict[str, NativeObjectV1], blockers: set[str]
) -> None:
    plans = {item.artifact_id: item for item in state.of_type(CollectionPlanV1)}
    starts = {item.plan_id: item for item in state.of_type(ApplyStartedV1)}
    if len(starts) != len(state.of_type(ApplyStartedV1)):
        raise RetentionStoreError(
            "collection plan was attempted more than once"
        )
    intents = {item.artifact_id: item for item in state.of_type(UnlinkIntentV1)}
    observed = {
        item.intent_id: item for item in state.of_type(UnlinkObservedV1)
    }
    tombstones = {
        item.object_id: item for item in state.of_type(CollectionTombstoneV1)
    }
    receipts = {
        item.plan_id: item for item in state.of_type(CollectionReceiptV1)
    }
    if (
        len(observed) != len(state.of_type(UnlinkObservedV1))
        or len(tombstones) != len(state.of_type(CollectionTombstoneV1))
        or len(receipts) != len(state.of_type(CollectionReceiptV1))
    ):
        raise RetentionStoreError(
            "ambiguous collection outcome/tombstone/receipt"
        )
    if len(
        {(item.plan_id, item.object_id) for item in intents.values()}
    ) != len(intents):
        raise RetentionStoreError(
            "multiple intents for the same collection object"
        )
    for plan_id, start in starts.items():
        plan = plans.get(plan_id)
        if (
            plan is None
            or start.snapshot_id != plan.snapshot_id
            or start.candidate_object_ids
            != tuple(
                item.object_id
                for item in plan.decisions
                if item.action == "delete"
            )
            or plan.blockers
        ):
            raise RetentionStoreError(
                "collection attempt differs from its retained plan"
            )
        if (
            state.entry_for(plan_id).sequence
            >= state.entry_for(start.artifact_id).sequence
            or start.started_ns < plan.cutoff_ns
        ):
            raise RetentionStoreError(
                "collection began before its plan or cutoff"
            )
        receipt = receipts.get(plan_id)
        if receipt is None or receipt.status != "complete":
            blockers.add(f"incomplete_collection_attempt:{plan_id}")
        if (
            receipt is not None
            and tuple(item.object_id for item in receipt.outcomes)
            != start.candidate_object_ids
        ):
            raise RetentionStoreError(
                "collection receipt omits candidate outcomes"
            )
        if receipt is not None:
            if (
                receipt.started_ns != start.started_ns
                or state.entry_for(receipt.artifact_id).sequence
                <= state.entry_for(start.artifact_id).sequence
            ):
                raise RetentionStoreError(
                    "receipt precedes or misstates actual collection begin"
                )
            for outcome in receipt.outcomes:
                intent = next(
                    (
                        item
                        for item in intents.values()
                        if (item.plan_id, item.object_id)
                        == (plan_id, outcome.object_id)
                    ),
                    None,
                )
                result = observed.get(intent.artifact_id) if intent else None
                if outcome.outcome == "not_attempted":
                    if (
                        intent is not None
                        or outcome.journal_entry_id
                        != state.entry_for(start.artifact_id).artifact_id
                    ):
                        raise RetentionStoreError(
                            "unattempted outcome has an actual intent"
                        )
                    continue
                if intent is None:
                    raise RetentionStoreError(
                        "receipt outcome lacks durable intent"
                    )
                if result is None:
                    raise RetentionStoreError(
                        "persisted receipt outcome lacks an actual observation"
                    )
                if (
                    outcome.outcome != result.outcome
                    or outcome.size_bytes_unlinked != result.size_bytes_unlinked
                    or outcome.journal_entry_id
                    != state.entry_for(result.artifact_id).artifact_id
                    or result.observed_ns > receipt.finished_ns
                    or state.entry_for(result.artifact_id).sequence
                    >= state.entry_for(receipt.artifact_id).sequence
                ):
                    raise RetentionStoreError(
                        "receipt differs from exact actual unlink observation"
                    )
                if outcome.outcome == "unlinked":
                    tombstone = tombstones.get(outcome.object_id)
                    if (
                        tombstone is None
                        or state.entry_for(tombstone.artifact_id).sequence
                        >= state.entry_for(receipt.artifact_id).sequence
                    ):
                        raise RetentionStoreError(
                            "receipt claims unlink without preceding tombstone"
                        )
    for identity, intent in intents.items():
        start, item = starts.get(intent.plan_id), objects.get(intent.object_id)
        if (
            start is None
            or item is None
            or intent.object_id not in start.candidate_object_ids
            or intent.started_ns < start.started_ns
            or (
                intent.observation.relative_path,
                intent.observation.sha256,
                intent.observation.size_bytes,
            )
            != (
                item.payload_ref.relative_path,
                item.payload_ref.sha256,
                item.payload_ref.size_bytes,
            )
        ):
            raise RetentionStoreError(
                "unlink intent lacks exact admitted plan/payload"
            )
        if (
            state.entry_for(intent.artifact_id).sequence
            <= state.entry_for(start.artifact_id).sequence
        ):
            raise RetentionStoreError(
                "unlink intent precedes its collection attempt"
            )
        result = observed.get(identity)
        if result is None:
            blockers.add(f"indeterminate_unlink:{identity}")
            continue
        if (
            result.plan_id != intent.plan_id
            or result.object_id != intent.object_id
            or result.observed_ns < intent.started_ns
            or state.entry_for(result.artifact_id).sequence
            <= state.entry_for(intent.artifact_id).sequence
            or (
                result.outcome == "unlinked"
                and result.size_bytes_unlinked != intent.observation.size_bytes
            )
        ):
            raise RetentionStoreError(
                "unlink observation differs from durable intent"
            )
        if result.outcome != "unlinked":
            blockers.add(f"indeterminate_unlink:{identity}")
            if intent.object_id in tombstones:
                raise RetentionStoreError(
                    "indeterminate unlink has an invalid tombstone"
                )
            continue
        tombstone = tombstones.get(intent.object_id)
        if tombstone is None:
            blockers.add(f"missing_unlink_tombstone:{identity}")
        elif (
            tombstone.plan_id != result.plan_id
            or tombstone.journal_entry_id
            != state.entry_for(result.artifact_id).artifact_id
            or tombstone.payload_sha256 != item.payload_ref.sha256
            or tombstone.size_bytes_unlinked != result.size_bytes_unlinked
            or tombstone.unlinked_ns != result.observed_ns
        ):
            raise RetentionStoreError(
                "tombstone differs from actual linked unlink observation"
            )
        elif (
            state.entry_for(tombstone.artifact_id).sequence
            <= state.entry_for(result.artifact_id).sequence
            or item.payload_ref.relative_path in state.observations
        ):
            raise RetentionStoreError(
                "tombstone precedes unlink or payload is still present"
            )
    if set(observed) - intents.keys() or set(receipts) - starts.keys():
        raise RetentionStoreError("unbegun collection terminal")
    witnessed = {
        item.object_id
        for item in observed.values()
        if item.outcome == "unlinked"
    }
    if set(tombstones) - witnessed:
        raise RetentionStoreError(
            "tombstone lacks completed unlink observation"
        )


def _verify_scratch(state: _State, item: NativeObjectV1) -> None:
    begins = [
        record
        for record in state.of_type(OperationBeginV1)
        if record.transaction_id == item.transaction_id
    ]
    if len(begins) != 1:
        raise RetentionStoreError(
            "managed scratch lacks one actual transaction"
        )
    begin = begins[0]
    if (
        begin.operation == "ingest_ascii_source"
        or begin.external_workspace is None
    ):
        raise RetentionStoreError("source ingestion cannot invent managed work")
    work = ManagedTransactionScratchV1(
        begin.store_id,
        begin.transaction_id,
        begin.operation,
        begin.input_object_ids,
        begin.external_workspace,
        begin.started_ns,
    )
    expected = work.to_json().encode("ascii")
    if item.payload_ref.sha256 != sha256(
        expected
    ) or item.payload_ref.size_bytes != len(expected):
        raise RetentionStoreError(
            "scratch bytes differ from exact retained work record"
        )
    if (
        item.payload_ref.relative_path in state.observations
        and state.session.read_bytes(item.payload_ref.relative_path) != expected
    ):
        raise RetentionStoreError("actual managed work payload differs")
    if (
        work.store_id != state.session.marker.store_id
        or item.artifact_id not in begin.scratch_object_ids
        or work.operation != begin.operation
        or work.input_object_ids != begin.input_object_ids
        or work.external_workspace != begin.external_workspace
        or work.started_ns != begin.started_ns
        or item.created_ns != work.started_ns
        or item.transaction_id != work.transaction_id
        or item.native_schema != work.SCHEMA
        or item.native_id != work.artifact_id
        or item.retention_class is not RetentionClass.SCRATCH
        or item.live_dependencies
        or item.historical_subject_ids != work.input_object_ids
        or not item.payload_ref.relative_path.endswith(".scratch")
    ):
        raise RetentionStoreError(
            "scratch descriptor was not emitted by closed lifecycle"
        )


def _snapshot(state: _State, budget: _NativeBudget) -> StoreSnapshotV1:
    blockers = _lifecycle_blockers(state)
    descriptors = state.descriptors
    present: list[str] = []
    regenerations: list[CacheRegenerationV1] = []
    for item in descriptors:
        if item.adapter_id == _SCRATCH_ADAPTER:
            _verify_scratch(state, item)
        observation = state.observations.get(item.payload_ref.relative_path)
        if observation is None:
            continue
        present.append(item.artifact_id)
        if (
            observation.sha256 != item.payload_ref.sha256
            or observation.size_bytes != item.payload_ref.size_bytes
        ):
            blockers.add(f"payload_hash_size_mismatch:{item.artifact_id}")
            continue
        try:
            if item.adapter_id == _SCRATCH_ADAPTER:
                _verify_scratch(state, item)
            else:
                cost = _native_cost(state, item)
                budget.invoke(
                    cost,
                    lambda scratch: native.admit_native_object(
                        Path(state.session.path),
                        item,
                        descriptors,
                        scratch_directory=scratch,
                    ),
                )
            if item.adapter_id == native.REGENERATION_ADAPTER:
                regenerations.append(
                    CacheRegenerationV1.from_json(
                        state.session.read_bytes(
                            item.payload_ref.relative_path
                        ).decode("ascii")
                    )
                )
        except (ValueError, TypeError, OSError) as error:
            blockers.add(
                f"native_verification_failed:{item.artifact_id}:{type(error).__name__}"
            )
    if state.session.inventory() != tuple(
        sorted(state.observations.values(), key=lambda item: item.relative_path)
    ):
        raise RetentionStoreError(
            "managed inputs changed during native verification"
        )
    _now(state)
    operative = [
        entry
        for entry in state.journal
        if entry.event not in _HISTORICAL_ONLY_EVENTS
    ]
    operative_paths = {entry.record_path for entry in operative}
    revision = sha256(
        canonical_json(
            {
                "state_head": operative[-1].artifact_id,
                "files": [
                    {
                        field.name: getattr(item, field.name)
                        for field in fields(item)
                    }
                    for path, item in sorted(state.observations.items())
                    if path == MANAGED_ARTIFACT_MARKER
                    or path.startswith("objects/")
                    or path in operative_paths
                ],
            }
        ).encode("ascii")
    )
    return StoreSnapshotV1(
        state.session.marker.store_id,
        state.session.marker.policy,
        state.observations[MANAGED_ARTIFACT_MARKER].sha256,
        revision,
        descriptors,
        tuple(sorted(present)),
        tuple(
            sorted(
                {item.object_id for item in state.of_type(RootRegistrationV1)}
            )
        ),
        state.of_type(RetentionHoldV1),
        state.of_type(TransactionCompletionV1),
        tuple(sorted(regenerations, key=lambda item: item.artifact_id)),
        state.of_type(CollectionTombstoneV1),
        tuple(sorted(blockers)),
    )


def create_retention_store(
    root: str | Path, policy: RetentionPolicyV1 = RetentionPolicyV1()
) -> StoreMarkerV1:
    """Create a dedicated empty store; interrupted initialization is retained."""
    marker = create_store_filesystem(root, policy)
    with StoreSession(root) as session:
        state = _State(
            session,
            {item.relative_path: item for item in session.inventory()},
            {},
            [],
        )
        record = StoreCreationV1(
            marker.store_id,
            state.observations[MANAGED_ARTIFACT_MARKER].sha256,
            marker.policy.artifact_id,
            _now(state),
        )
        _append(state, "created", record)
        _load_state(session)
    return marker


def inspect_retention_store(root: str | Path) -> StoreSnapshotV1:
    """Actually replay complete supported live evidence in retained scratch."""
    with StoreSession(root) as session:
        state = _load_state(session)
        return _snapshot(
            state,
            _NativeBudget(
                state, _inspection_cost(state), control_bytes=0, control_files=0
            ),
        )


def plan_artifact_collection(
    root: str | Path, *, cutoff_ns: int | None = None
) -> CollectionPlanV1:
    """Persist a deterministic plan; no serialized snapshot authorizes apply."""
    with StoreSession(root) as session:
        state = _load_state(session)
        now = _now(state)
        cutoff = now if cutoff_ns is None else cutoff_ns
        if type(cutoff) is not int or not 0 < cutoff <= now:
            raise RetentionStoreError(
                "planning cutoff exceeds fresh current time"
            )
        budget = _NativeBudget(state, _inspection_cost(state))
        snapshot = _snapshot(state, budget)
        plan = _derive_plan(
            snapshot, cutoff_ns=cutoff, observed_now_ns=_now(state)
        )
        path, raw = _control_path("plans", plan)
        if path in state.observations:
            if session.read_bytes(path) != raw:
                raise RetentionStoreError("retained plan bytes differ")
            return plan
        _append(state, "plan", plan)
        return plan


def _reserve_objects(state: _State, count: int, byte_count: int) -> None:
    """Lifetime admission includes historical descriptors, never just live bytes."""
    if (
        len(state.descriptors) + count > MAX_OBJECTS
        or sum(item.payload_ref.size_bytes for item in state.descriptors)
        + byte_count
        > MAX_PAYLOAD_BYTES
    ):
        raise RetentionStoreError("managed object lifetime capacity exceeded")


def _require_clean(snapshot: StoreSnapshotV1) -> None:
    if snapshot.blockers:
        raise RetentionStoreError(
            "managed store has incomplete or invalid current evidence"
        )
    plan = _derive_plan(
        snapshot, cutoff_ns=1, observed_now_ns=max(1, time.time_ns())
    )
    if plan.blockers:
        raise RetentionStoreError(
            "managed store has unresolved native/graph evidence"
        )


def _edges(*values: tuple[str, DependencyRelation]) -> tuple[DependencyV1, ...]:
    return tuple(
        DependencyV1(identity, relation)
        for identity, relation in sorted(
            values, key=lambda value: (value[0], value[1].value)
        )
    )


def _new_object(
    state: _State,
    begin: OperationBeginV1,
    raw: bytes,
    extension: str,
    adapter: str,
    admission: native.NativeAdmission,
    retention_class: RetentionClass,
) -> NativeObjectV1:
    if type(raw) is not bytes or not raw or len(raw) > MAX_PAYLOAD_BYTES:
        raise RetentionStoreError("invalid generated managed payload")
    return NativeObjectV1(
        adapter,
        admission.native_schema,
        admission.native_id,
        ManagedPayloadRefV1(
            f"objects/{uuid.uuid4().hex}.{extension}", sha256(raw), len(raw)
        ),
        retention_class,
        admission.live_dependencies,
        admission.historical_subject_ids,
        begin.transaction_id,
        _now(state),
    )


def _publish_object(state: _State, item: NativeObjectV1, raw: bytes) -> None:
    _reserve_objects(state, 1, len(raw))
    if (
        sha256(raw) != item.payload_ref.sha256
        or len(raw) != item.payload_ref.size_bytes
    ):
        raise RetentionStoreError(
            "publication bytes differ from constructed descriptor"
        )
    observation = state.session.write_immutable(
        item.payload_ref.relative_path, raw
    )
    state.observations[item.payload_ref.relative_path] = observation
    _append(state, "descriptor", item)


def _begin(
    state: _State,
    operation: Operation,
    inputs: tuple[str, ...],
    budget: _NativeBudget,
) -> OperationBeginV1:
    now, transaction_id = _now(state), uuid.uuid4().hex
    if operation == "ingest_ascii_source":
        begin = OperationBeginV1(
            state.session.marker.store_id,
            transaction_id,
            operation,
            (),
            (),
            None,
            now,
        )
        _append(state, "operation_begin", begin)
        return begin
    # This exact work payload is read again before native terminal publication.
    work = ManagedTransactionScratchV1(
        state.session.marker.store_id,
        transaction_id,
        operation,
        inputs,
        str(budget.ensure_workspace()),
        now,
    )
    raw = work.to_json().encode("ascii")
    scratch = NativeObjectV1(
        _SCRATCH_ADAPTER,
        work.SCHEMA,
        work.artifact_id,
        ManagedPayloadRefV1(
            f"objects/{uuid.uuid4().hex}.scratch", sha256(raw), len(raw)
        ),
        RetentionClass.SCRATCH,
        (),
        inputs,
        transaction_id,
        now,
    )
    begin = OperationBeginV1(
        state.session.marker.store_id,
        transaction_id,
        operation,
        inputs,
        (scratch.artifact_id,),
        work.external_workspace,
        now,
    )
    _append(state, "operation_begin", begin)
    _publish_object(state, scratch, raw)
    return begin


def _abort_before_outputs(
    state: _State, begin: OperationBeginV1, error: Exception
) -> None:
    """Only an intact scratch-only transaction can be closed as aborted.

    Failed filesystem publication, unknown payloads, or any published output
    leave a pending transaction; absence is never adopted as a clean abort.
    """
    actual = _load_state(state.session)
    owned = tuple(
        item
        for item in actual.descriptors
        if item.transaction_id == begin.transaction_id
    )
    if tuple(
        sorted(item.artifact_id for item in owned)
    ) != begin.scratch_object_ids or _lifecycle_blockers(actual) != {
        f"pending_transaction:{begin.transaction_id}"
    }:
        return
    for item in owned:
        _verify_scratch(actual, item)
    now = _now(actual)
    digest = sha256(
        canonical_json(
            {
                "begin": begin.artifact_id,
                "scratch": [item.to_dict() for item in owned],
                "error_type": type(error).__name__,
            }
        ).encode("ascii")
    )
    terminal = OperationAbortedV1(
        actual.session.marker.store_id,
        begin.transaction_id,
        begin.scratch_object_ids,
        digest,
        "native operation refused before publishing outputs",
        now,
    )
    if begin.operation != "ingest_ascii_source":
        _append(
            actual,
            "completed",
            TransactionCompletionV1(
                begin.transaction_id,
                "aborted",
                begin.scratch_object_ids,
                (),
                digest,
                now,
            ),
        )
    _append(actual, "aborted", terminal)


def _finish(
    state: _State,
    begin: OperationBeginV1,
    primary: NativeObjectV1,
    budget: _NativeBudget,
) -> AdmissionReceiptV1:
    # Complete actual native verification precedes the terminal authority.
    snapshot = _snapshot(state, budget)
    if snapshot.blockers != (f"pending_transaction:{begin.transaction_id}",):
        raise RetentionStoreError(
            "new native publication failed current complete inspection"
        )
    outputs = tuple(
        sorted(
            item.artifact_id
            for item in state.descriptors
            if item.transaction_id == begin.transaction_id
            and item.artifact_id not in begin.scratch_object_ids
        )
    )
    now = _now(state)
    digest = sha256(snapshot.to_json().encode("ascii"))
    admission = AdmissionReceiptV1(
        state.session.marker.store_id,
        begin.transaction_id,
        begin.operation,
        primary.artifact_id,
        outputs,
        begin.scratch_object_ids,
        digest,
        begin.started_ns,
        now,
    )
    if begin.operation != "ingest_ascii_source":
        _append(
            state,
            "completed",
            TransactionCompletionV1(
                begin.transaction_id,
                "published",
                begin.scratch_object_ids,
                (primary.artifact_id,),
                digest,
                now,
            ),
        )
    _append(state, "admitted", admission)
    actual = _load_state(state.session)
    if _lifecycle_blockers(actual):
        raise RetentionStoreError(
            "publication did not reach a closed lifecycle terminal"
        )
    return admission


def ingest_ascii_tick_source(
    root: str | Path, source_path: str | Path, *, symbol: str, period: str
) -> AdmissionReceiptV1:
    """Copy bounded native-validated CSV; never remove or rewrite its source."""
    path = Path(source_path)
    original_identity = native._stat_identity(path.lstat())
    raw = native._read_regular(path, native.MAX_ASCII_BYTES)
    admission = native.source_admission(raw, symbol=symbol, period=period)
    with StoreSession(root) as session:
        state = _load_state(session)
        _reserve_objects(state, 1, len(raw))
        budget = _NativeBudget(
            state,
            2 * _inspection_cost(state),
            control_bytes=16 * _CONTROL_ALLOWANCE,
            control_files=16,
        )
        _require_clean(_snapshot(state, budget))
        begin = _begin(state, "ingest_ascii_source", (), budget)
        item = _new_object(
            state,
            begin,
            raw,
            "csv",
            native.ASCII_SOURCE_ADAPTER,
            admission,
            RetentionClass.IMMUTABLE_SOURCE,
        )
        _publish_object(state, item, raw)
        if (
            native._stat_identity(path.lstat()) != original_identity
            or native._read_regular(path, native.MAX_ASCII_BYTES) != raw
        ):
            raise RetentionStoreError(
                "external source changed during managed copy"
            )
        return _finish(state, begin, item, budget)


def produce_histdata_cache(
    root: str | Path, source_object_id: str
) -> AdmissionReceiptV1:
    """Run the closed native producer, then repeat it for exact regeneration."""
    _object_id(source_object_id)
    with StoreSession(root) as session:
        state = _load_state(session)
        _reserve_objects(
            state, 4, native.MAX_CACHE_BYTES + 3 * _CONTROL_ALLOWANCE
        )
        budget = _NativeBudget(
            state,
            2 * _inspection_cost(state) + 4,
            control_bytes=32 * _CONTROL_ALLOWANCE,
            control_files=32,
        )
        snapshot = _snapshot(state, budget)
        _require_clean(snapshot)
        source = next(
            (
                item
                for item in state.descriptors
                if item.artifact_id == source_object_id
            ),
            None,
        )
        if (
            source is None
            or source.adapter_id != native.ASCII_SOURCE_ADAPTER
            or source_object_id not in snapshot.present_object_ids
        ):
            raise RetentionStoreError(
                "cache requires a current managed native source"
            )
        match = re.fullmatch(
            r"retention-ascii-source:([A-Z]{6}):([0-9]{6}):sha256:[0-9a-f]{64}",
            source.native_id,
        )
        if match is None:
            raise RetentionStoreError(
                "verified native source dimensions are unavailable"
            )
        raw = native.read_managed_payload(
            Path(session.path),
            source.payload_ref,
            maximum=native.MAX_ASCII_BYTES,
        )
        recipe = AsciiTickRecipeV1(
            source_object_id,
            match[1],
            match[2],
            native.current_cache_implementation_sha256(),
            native.current_cache_backend_id(),
        )
        begin = _begin(
            state, "produce_histdata_cache", (source_object_id,), budget
        )
        try:
            execution = budget.invoke(
                1,
                lambda scratch: native.execute_ascii_cache_recipe(
                    raw, recipe, scratch_directory=scratch
                ),
            )
        except (ValueError, TypeError, OSError) as error:
            _abort_before_outputs(state, begin, error)
            raise
        recipe_raw = recipe.to_json().encode("ascii")
        recipe_object = _new_object(
            state,
            begin,
            recipe_raw,
            "json",
            native.ASCII_RECIPE_ADAPTER,
            native.NativeAdmission(
                recipe.SCHEMA,
                recipe.artifact_id,
                _edges((source_object_id, DependencyRelation.SOURCE)),
            ),
            RetentionClass.REPRODUCIBILITY_DEPENDENCY,
        )
        _publish_object(state, recipe_object, recipe_raw)
        from histdatacom.datasets.contracts import CanonicalObservedPartitionV2

        partition = CanonicalObservedPartitionV2.from_dict(
            native._load_native(execution.partition_json.encode("ascii"))
        )
        cache = _new_object(
            state,
            begin,
            execution.cache_bytes,
            "data",
            native.CACHE_ADAPTER,
            native.NativeAdmission(
                partition.schema_version,
                partition.partition_id,
                _edges(
                    (source_object_id, DependencyRelation.SOURCE),
                    (recipe_object.artifact_id, DependencyRelation.RECIPE),
                ),
            ),
            RetentionClass.REPLACEABLE_CACHE,
        )
        _publish_object(state, cache, execution.cache_bytes)
        proof = budget.invoke(
            1,
            lambda scratch: native.prove_cache_regeneration(
                Path(session.path),
                cache.artifact_id,
                state.descriptors,
                scratch_directory=scratch,
            ),
        )
        proof_raw = proof.to_json().encode("ascii")
        proof_object = _new_object(
            state,
            begin,
            proof_raw,
            "json",
            native.REGENERATION_ADAPTER,
            native.NativeAdmission(
                proof.SCHEMA,
                proof.artifact_id,
                _edges(
                    (source_object_id, DependencyRelation.SOURCE),
                    (recipe_object.artifact_id, DependencyRelation.RECIPE),
                ),
                (cache.artifact_id,),
            ),
            RetentionClass.REPRODUCIBILITY_DEPENDENCY,
        )
        _publish_object(state, proof_object, proof_raw)
        return _finish(state, begin, cache, budget)


def publish_histdata_cache_catalog(
    root: str | Path, cache_object_ids: tuple[str, ...], *, dataset_id: str
) -> AdmissionReceiptV1:
    """Publish a NEW native, unqualified observed catalog; no relocation API."""
    if (
        type(cache_object_ids) is not tuple
        or not 0 < len(cache_object_ids) <= native.MAX_PARTITIONS
    ):
        raise RetentionStoreError(
            "catalog selection must be a bounded exact tuple"
        )
    _object_ids(cache_object_ids)
    if type(dataset_id) is not str or not 0 < len(dataset_id) <= 256:
        raise RetentionStoreError("invalid bounded native dataset identity")
    from histdatacom.datasets.contracts import normalize_dataset_id

    if normalize_dataset_id(dataset_id) != dataset_id:
        raise RetentionStoreError("dataset identity is not canonical")
    with StoreSession(root) as session:
        state = _load_state(session)
        _reserve_objects(
            state, 2, native.MAX_NATIVE_JSON_BYTES + _CONTROL_ALLOWANCE
        )
        budget = _NativeBudget(
            state,
            2 * _inspection_cost(state) + 2 * len(cache_object_ids),
            control_bytes=24 * _CONTROL_ALLOWANCE,
            control_files=24,
        )
        snapshot = _snapshot(state, budget)
        _require_clean(snapshot)
        objects = {item.artifact_id: item for item in state.descriptors}
        if any(
            identity not in snapshot.present_object_ids
            or objects[identity].adapter_id != native.CACHE_ADAPTER
            for identity in cache_object_ids
        ):
            raise RetentionStoreError(
                "catalog selection requires current native cache payloads"
            )
        begin = _begin(
            state, "publish_histdata_catalog", cache_object_ids, budget
        )
        try:
            built = budget.invoke(
                len(cache_object_ids),
                lambda scratch: native.build_native_catalog(
                    Path(session.path),
                    cache_object_ids,
                    state.descriptors,
                    dataset_id=dataset_id,
                    scratch_directory=scratch,
                ),
            )
        except (ValueError, TypeError, OSError) as error:
            _abort_before_outputs(state, begin, error)
            raise
        item = _new_object(
            state,
            begin,
            built.canonical_bytes,
            "json",
            native.CATALOG_ADAPTER,
            built.admission,
            RetentionClass.PUBLISHED_DERIVED,
        )
        _publish_object(state, item, built.canonical_bytes)
        return _finish(state, begin, item, budget)


def register_retention_root(
    root: str | Path, object_id: str, *, reason: str
) -> RootRegistrationV1:
    """Add protection after actual current replay; no root replacement/removal."""
    _object_id(object_id)
    _text(reason)
    with StoreSession(root) as session:
        state = _load_state(session)
        roots = {item.object_id for item in state.of_type(RootRegistrationV1)}
        if object_id in roots or len(roots) >= MAX_ROOTS:
            raise RetentionStoreError(
                "duplicate root or root lifetime bound exceeded"
            )
        budget = _NativeBudget(
            state,
            _inspection_cost(state),
            control_bytes=2 * _CONTROL_ALLOWANCE,
            control_files=2,
        )
        snapshot = _snapshot(state, budget)
        _require_clean(snapshot)
        if object_id not in snapshot.present_object_ids:
            raise RetentionStoreError(
                "retention root requires current live bytes"
            )
        record = RootRegistrationV1(
            session.marker.store_id,
            object_id,
            reason,
            sha256(snapshot.to_json().encode("ascii")),
            _now(state),
        )
        _append(state, "root", record)
        return record


def add_retention_hold(
    root: str | Path, object_ids: tuple[str, ...], *, reason: str
) -> RetentionHoldV1:
    """Monotone additive hold, never a policy replacement or deletion waiver."""
    if type(object_ids) is not tuple or not object_ids:
        raise RetentionStoreError("hold requires a nonempty exact object tuple")
    _object_ids(object_ids)
    _text(reason)
    with StoreSession(root) as session:
        state = _load_state(session)
        budget = _NativeBudget(
            state,
            _inspection_cost(state),
            control_bytes=2 * _CONTROL_ALLOWANCE,
            control_files=2,
        )
        snapshot = _snapshot(state, budget)
        _require_clean(snapshot)
        if not set(object_ids) <= set(snapshot.present_object_ids):
            raise RetentionStoreError(
                "hold requires actual current live managed objects"
            )
        record = RetentionHoldV1(object_ids, reason, _now(state))
        _append(state, "hold", record)
        return record
