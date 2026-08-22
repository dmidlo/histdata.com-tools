"""Durable, bounded orchestration for synthetic reconstruction windows.

Temporal owns ordering, retries, and progress.  Scientific rows stay in files
owned by stage handlers; workflow-visible values are contracts, counters, and
strong :class:`~histdatacom.runtime_contracts.ArtifactRef` objects only.
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import shutil
import tempfile
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Protocol, cast

from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.reconstruction_storage import (
    ReconstructionStorageRootError,
    verify_reconstruction_storage_for_execution,
)
from histdatacom.runtime_contracts import ArtifactRef, JSONValue
from histdatacom.synthetic.contracts import canonical_contract_json
from histdatacom.synthetic.persistence import (
    verify_reconstruction_publication,
)
from histdatacom.synthetic.streaming import (
    ReconstructionCheckpointV1,
    ReconstructionCommitPhase,
    ReconstructionHeartbeatV1,
    ReconstructionResourceEstimateV1,
    ReconstructionResourceLimitError,
    ReconstructionRunV1,
    ReconstructionStoragePolicyV1,
    ReconstructionWindowV1,
)

RECONSTRUCTION_ORCHESTRATION_SCHEMA_VERSION = (
    "histdatacom.reconstruction-orchestration.v1"
)
RECONSTRUCTION_STAGE_COMMAND_SCHEMA_VERSION = (
    "histdatacom.reconstruction-stage-command.v1"
)
RECONSTRUCTION_STAGE_OUTCOME_SCHEMA_VERSION = (
    "histdatacom.reconstruction-stage-outcome.v1"
)
RECONSTRUCTION_WINDOW_TASK_SCHEMA_VERSION = (
    "histdatacom.reconstruction-window-task.v1"
)
RECONSTRUCTION_WORKFLOW_REQUEST_SCHEMA_VERSION = (
    "histdatacom.reconstruction-workflow-request.v1"
)
RECONSTRUCTION_WINDOW_STATE_SCHEMA_VERSION = (
    "histdatacom.reconstruction-window-state.v1"
)
RECONSTRUCTION_REPORT_SCHEMA_VERSION = (
    "histdatacom.reconstruction-run-report.v1"
)

MAX_RECONSTRUCTION_WORKFLOW_BYTES = 1_048_576
MAX_RECONSTRUCTION_WINDOW_STATE_BYTES = 1_048_576
MAX_RECONSTRUCTION_REPORT_BYTES = 1_048_576
MAX_RECONSTRUCTION_WINDOWS = 512
MAX_STAGE_ARTIFACT_REFS = 32
MAX_STAGE_RECEIPT_BYTES = 262_144
MAX_STAGE_MESSAGE_LENGTH = 2_048
DEFAULT_MAX_PARALLEL_RECONSTRUCTION_WINDOWS = 2
RECONSTRUCTION_REPORT_DIRECTORY = "reconstruction-reports"
RECONSTRUCTION_RECEIPT_KIND = "reconstruction_stage_receipt"

_FORBIDDEN_WORKFLOW_KEYS = frozenset(
    {"dataframe", "event_batches", "events", "records", "rows", "table"}
)


class ReconstructionStage(str, Enum):
    """Ordered data-plane boundaries controlled by Temporal."""

    SOURCE_ENRICHMENT = "source_enrichment"
    PROPOSAL = "proposal"
    CARVING = "carving"
    CROSS_SERIES_RECONCILIATION = "cross_series_reconciliation"
    BROKER_TRANSFER = "broker_transfer"
    VALIDATION = "validation"
    ATOMIC_PARTITION_COMMIT = "atomic_partition_commit"

    @classmethod
    def from_value(
        cls, value: str | "ReconstructionStage"
    ) -> "ReconstructionStage":
        """Normalize one stage name."""
        if isinstance(value, cls):
            return value
        normalized = str(value).strip().lower().replace("-", "_")
        try:
            return cls(normalized)
        except ValueError as err:
            raise ValueError(
                f"unsupported reconstruction stage: {value!r}"
            ) from err


RECONSTRUCTION_STAGE_ORDER = tuple(ReconstructionStage)


class ReconstructionStageStatus(str, Enum):
    """Bounded outcome status for one stage attempt."""

    COMPLETED = "completed"
    REFUSED = "refused"

    @classmethod
    def from_value(
        cls, value: str | "ReconstructionStageStatus"
    ) -> "ReconstructionStageStatus":
        """Normalize one stage status."""
        if isinstance(value, cls):
            return value
        try:
            return cls(str(value).strip().lower())
        except ValueError as err:
            raise ValueError("unsupported reconstruction stage status") from err


class ReconstructionCheckpointConflict(RuntimeError):
    """A stale worker tried to replace a newer checkpoint chain."""


class ReconstructionArtifactError(ValueError):
    """A stage receipt or referenced artifact failed integrity checks."""


class ReconstructionReportMismatch(ValueError):
    """Workflow state and committed storage evidence do not reconcile."""


@dataclass(frozen=True, slots=True)
class ReconstructionStageCommandV1:
    """Bounded command metadata for one artifact-producing stage."""

    stage: ReconstructionStage
    handler_name: str
    receipt_path: str
    input_manifest_refs: tuple[ArtifactRef, ...] = ()
    configuration_refs: tuple[ArtifactRef, ...] = ()
    command_id: str = ""
    schema_version: str = RECONSTRUCTION_STAGE_COMMAND_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_STAGE_COMMAND_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction stage command schema")
        object.__setattr__(
            self, "stage", ReconstructionStage.from_value(self.stage)
        )
        object.__setattr__(
            self, "handler_name", _required_text(self.handler_name)
        )
        object.__setattr__(
            self,
            "receipt_path",
            str(Path(_required_text(self.receipt_path)).expanduser().resolve()),
        )
        for name in ("input_manifest_refs", "configuration_refs"):
            refs = _unique_strong_refs(getattr(self, name), name)
            if len(refs) > MAX_STAGE_ARTIFACT_REFS:
                raise ValueError(f"{name} exceeds artifact-reference limit")
            object.__setattr__(self, name, refs)
        expected = _stable_id("reconstruction-command", self.identity_payload())
        if self.command_id and self.command_id != expected:
            raise ValueError("command_id does not match deterministic identity")
        object.__setattr__(self, "command_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return retry-independent command identity."""
        return {
            "schema_version": self.schema_version,
            "stage": self.stage.value,
            "handler_name": self.handler_name,
            "receipt_path": self.receipt_path,
            "input_manifest_refs": [
                _artifact_identity(ref) for ref in self.input_manifest_refs
            ],
            "configuration_refs": [
                _artifact_identity(ref) for ref in self.configuration_refs
            ],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return workflow-safe command metadata."""
        return {
            **self.identity_payload(),
            "command_id": self.command_id,
            "input_manifest_refs": [
                ref.to_dict() for ref in self.input_manifest_refs
            ],
            "configuration_refs": [
                ref.to_dict() for ref in self.configuration_refs
            ],
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionStageCommandV1":
        """Restore and verify one stage command."""
        return cls(
            stage=ReconstructionStage.from_value(str(data.get("stage", ""))),
            handler_name=str(data.get("handler_name", "")),
            receipt_path=str(data.get("receipt_path", "")),
            input_manifest_refs=_artifact_refs(data.get("input_manifest_refs")),
            configuration_refs=_artifact_refs(data.get("configuration_refs")),
            command_id=str(data.get("command_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionStageOutcomeV1:
    """Durable stage receipt containing references and bounded counters only."""

    run_id: str
    window_id: str
    synchronization_unit_id: str
    stage: ReconstructionStage
    command_id: str
    input_fingerprint: str
    status: ReconstructionStageStatus
    output_refs: tuple[ArtifactRef, ...] = ()
    observed_event_count: int = 0
    candidate_event_count: int = 0
    accepted_event_count: int = 0
    scratch_bytes: int = 0
    output_bytes: int = 0
    refusal_reasons: tuple[str, ...] = ()
    message: str = ""
    reused: bool = False
    outcome_id: str = ""
    schema_version: str = RECONSTRUCTION_STAGE_OUTCOME_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_STAGE_OUTCOME_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction stage outcome schema")
        for name in (
            "run_id",
            "window_id",
            "synchronization_unit_id",
            "command_id",
        ):
            object.__setattr__(self, name, _required_text(getattr(self, name)))
        object.__setattr__(
            self, "stage", ReconstructionStage.from_value(self.stage)
        )
        object.__setattr__(
            self, "status", ReconstructionStageStatus.from_value(self.status)
        )
        object.__setattr__(
            self,
            "input_fingerprint",
            _required_sha256(self.input_fingerprint, "input_fingerprint"),
        )
        refs = _unique_strong_refs(self.output_refs, "output_refs")
        if len(refs) > MAX_STAGE_ARTIFACT_REFS:
            raise ValueError("output_refs exceeds artifact-reference limit")
        object.__setattr__(self, "output_refs", refs)
        for name in (
            "observed_event_count",
            "candidate_event_count",
            "accepted_event_count",
            "scratch_bytes",
            "output_bytes",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        reasons = tuple(
            sorted({_bounded_text(item) for item in self.refusal_reasons})
        )
        object.__setattr__(self, "refusal_reasons", reasons)
        object.__setattr__(self, "message", _bounded_text(self.message))
        if not isinstance(self.reused, bool):
            raise ValueError("reused must be a boolean")
        if self.status is ReconstructionStageStatus.REFUSED and not reasons:
            raise ValueError("refused stage outcome requires refusal_reasons")
        if self.status is ReconstructionStageStatus.COMPLETED and reasons:
            raise ValueError("completed stage outcome rejects refusal_reasons")
        expected = _stable_id("reconstruction-outcome", self.identity_payload())
        if self.outcome_id and self.outcome_id != expected:
            raise ValueError("outcome_id does not match deterministic identity")
        object.__setattr__(self, "outcome_id", expected)
        _reject_inline_data(self.to_dict(), path="stage_outcome")
        _ensure_payload_size(
            self.to_dict(), MAX_STAGE_RECEIPT_BYTES, "stage outcome"
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return retry-independent receipt identity."""
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "window_id": self.window_id,
            "synchronization_unit_id": self.synchronization_unit_id,
            "stage": self.stage.value,
            "command_id": self.command_id,
            "input_fingerprint": self.input_fingerprint,
            "status": self.status.value,
            "output_refs": [
                _artifact_identity(ref) for ref in self.output_refs
            ],
            "observed_event_count": self.observed_event_count,
            "candidate_event_count": self.candidate_event_count,
            "accepted_event_count": self.accepted_event_count,
            "scratch_bytes": self.scratch_bytes,
            "output_bytes": self.output_bytes,
            "refusal_reasons": list(self.refusal_reasons),
            "message": self.message,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a bounded Temporal-safe receipt."""
        return {
            **self.identity_payload(),
            "outcome_id": self.outcome_id,
            "output_refs": [ref.to_dict() for ref in self.output_refs],
            "reused": self.reused,
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionStageOutcomeV1":
        """Restore and verify one stage receipt."""
        return cls(
            run_id=str(data.get("run_id", "")),
            window_id=str(data.get("window_id", "")),
            synchronization_unit_id=str(
                data.get("synchronization_unit_id", "")
            ),
            stage=ReconstructionStage.from_value(str(data.get("stage", ""))),
            command_id=str(data.get("command_id", "")),
            input_fingerprint=str(data.get("input_fingerprint", "")),
            status=ReconstructionStageStatus.from_value(
                str(data.get("status", ""))
            ),
            output_refs=_artifact_refs(data.get("output_refs")),
            observed_event_count=cast(int, data.get("observed_event_count", 0)),
            candidate_event_count=cast(
                int, data.get("candidate_event_count", 0)
            ),
            accepted_event_count=cast(int, data.get("accepted_event_count", 0)),
            scratch_bytes=cast(int, data.get("scratch_bytes", 0)),
            output_bytes=cast(int, data.get("output_bytes", 0)),
            refusal_reasons=_string_tuple(data.get("refusal_reasons")),
            message=str(data.get("message", "")),
            reused=cast(bool, data.get("reused", False)),
            outcome_id=str(data.get("outcome_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )

    @classmethod
    def from_json(cls, text: str) -> "ReconstructionStageOutcomeV1":
        """Restore one stage receipt from JSON."""
        return cls.from_dict(_json_mapping(text))


@dataclass(frozen=True, slots=True)
class ReconstructionWindowTaskV1:
    """One synchronized window plus its bounded stage command plan."""

    window: ReconstructionWindowV1
    resource_estimate: ReconstructionResourceEstimateV1
    commands: tuple[ReconstructionStageCommandV1, ...]
    scratch_directory: str
    task_id: str = ""
    schema_version: str = RECONSTRUCTION_WINDOW_TASK_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_WINDOW_TASK_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction window task schema")
        if not isinstance(self.window, ReconstructionWindowV1):
            raise ValueError("window task requires ReconstructionWindowV1")
        if not isinstance(
            self.resource_estimate, ReconstructionResourceEstimateV1
        ):
            raise ValueError("window task requires a resource estimate")
        commands = tuple(self.commands)
        if tuple(item.stage for item in commands) != RECONSTRUCTION_STAGE_ORDER:
            raise ValueError(
                "window task must contain the complete ordered stage plan"
            )
        if len({item.command_id for item in commands}) != len(commands):
            raise ValueError("window task command IDs must be unique")
        object.__setattr__(self, "commands", commands)
        scratch = (
            Path(_required_text(self.scratch_directory)).expanduser().resolve()
        )
        for command in commands:
            receipt = Path(command.receipt_path).expanduser().resolve()
            if not receipt.is_relative_to(scratch):
                raise ValueError(
                    "stage receipt path must remain inside window scratch"
                )
        object.__setattr__(self, "scratch_directory", str(scratch))
        expected = _stable_id(
            "reconstruction-window-task", self.identity_payload()
        )
        if self.task_id and self.task_id != expected:
            raise ValueError("task_id does not match deterministic identity")
        object.__setattr__(self, "task_id", expected)

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic task identity."""
        return {
            "schema_version": self.schema_version,
            "window": self.window.to_dict(),
            "resource_estimate": self.resource_estimate.to_dict(),
            "commands": [item.to_dict() for item in self.commands],
            "scratch_directory": self.scratch_directory,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return workflow-safe task metadata."""
        return {**self.identity_payload(), "task_id": self.task_id}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReconstructionWindowTaskV1":
        """Restore and verify one window task."""
        return cls(
            window=ReconstructionWindowV1.from_dict(
                _mapping(data.get("window"))
            ),
            resource_estimate=ReconstructionResourceEstimateV1.from_dict(
                _mapping(data.get("resource_estimate"))
            ),
            commands=tuple(
                ReconstructionStageCommandV1.from_dict(_mapping(item))
                for item in _sequence(data.get("commands"))
            ),
            scratch_directory=str(data.get("scratch_directory", "")),
            task_id=str(data.get("task_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionWorkflowRequestV1:
    """A bounded period-scale plan suitable for Temporal workflow history."""

    request_id: str
    run: ReconstructionRunV1
    tasks: tuple[ReconstructionWindowTaskV1, ...]
    manifest_store_root: str
    report_root: str
    task_queues: dict[str, str] = field(default_factory=dict)
    max_parallel_windows: int = DEFAULT_MAX_PARALLEL_RECONSTRUCTION_WINDOWS
    max_inflight_memory_bytes: int | None = None
    request_fingerprint: str = ""
    schema_version: str = RECONSTRUCTION_WORKFLOW_REQUEST_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if (
            self.schema_version
            != RECONSTRUCTION_WORKFLOW_REQUEST_SCHEMA_VERSION
        ):
            raise ValueError(
                "unsupported reconstruction workflow request schema"
            )
        object.__setattr__(self, "request_id", _required_text(self.request_id))
        if not isinstance(self.run, ReconstructionRunV1):
            raise ValueError("workflow request requires ReconstructionRunV1")
        tasks = tuple(
            sorted(self.tasks, key=lambda item: item.window.core_start_ns)
        )
        if not tasks:
            raise ValueError(
                "workflow request requires at least one window task"
            )
        if len(tasks) > MAX_RECONSTRUCTION_WINDOWS:
            raise ValueError("workflow request exceeds window limit")
        if len({item.window.window_id for item in tasks}) != len(tasks):
            raise ValueError("workflow request contains duplicate windows")
        for task in tasks:
            if task.window.run_id != self.run.run_id:
                raise ValueError("window task run_id differs from request run")
            if tuple(sorted(task.window.symbols)) != tuple(
                sorted(self.run.symbols)
            ):
                raise ValueError(
                    "window task must contain the complete synchronized run symbol set"
                )
        for previous, current in zip(tasks, tasks[1:]):
            if current.window.core_start_ns < previous.window.core_end_ns:
                raise ValueError(
                    "reconstruction window core intervals must not overlap"
                )
        object.__setattr__(self, "tasks", tasks)
        manifest_store_root = str(
            Path(_required_text(self.manifest_store_root))
            .expanduser()
            .resolve()
        )
        report_root = str(
            Path(_required_text(self.report_root)).expanduser().resolve()
        )
        object.__setattr__(self, "manifest_store_root", manifest_store_root)
        object.__setattr__(self, "report_root", report_root)
        _validate_scratch_boundaries(
            tasks,
            durable_roots=(manifest_store_root, report_root),
        )
        queues = {
            _required_text(key): _required_text(value)
            for key, value in sorted(self.task_queues.items())
        }
        object.__setattr__(self, "task_queues", queues)
        parallel = _positive_int(
            self.max_parallel_windows, "max_parallel_windows"
        )
        if parallel > self.run.storage_policy.max_inflight_batches:
            raise ValueError(
                "max_parallel_windows exceeds storage-policy inflight limit"
            )
        object.__setattr__(self, "max_parallel_windows", parallel)
        memory_limit = self.max_inflight_memory_bytes
        if memory_limit is None:
            memory_limit = self.run.storage_policy.max_memory_bytes
        object.__setattr__(
            self,
            "max_inflight_memory_bytes",
            _positive_int(memory_limit, "max_inflight_memory_bytes"),
        )
        expected = _stable_id("reconstruction-request", self.identity_payload())
        if self.request_fingerprint and self.request_fingerprint != expected:
            raise ValueError(
                "request_fingerprint does not match deterministic identity"
            )
        object.__setattr__(self, "request_fingerprint", expected)
        payload = self.to_dict()
        _reject_inline_data(payload)
        _ensure_payload_size(
            payload,
            MAX_RECONSTRUCTION_WORKFLOW_BYTES,
            "reconstruction workflow request",
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return stable semantic and execution policy inputs."""
        return {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "run": self.run.to_dict(),
            "tasks": [item.to_dict() for item in self.tasks],
            "manifest_store_root": self.manifest_store_root,
            "report_root": self.report_root,
            "task_queues": dict(self.task_queues),
            "max_parallel_windows": self.max_parallel_windows,
            "max_inflight_memory_bytes": self.max_inflight_memory_bytes,
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return the bounded workflow payload."""
        return {
            **self.identity_payload(),
            "request_fingerprint": self.request_fingerprint,
            "history_policy": "bounded_metadata_and_artifact_refs_only",
        }

    def for_task(
        self, task: ReconstructionWindowTaskV1
    ) -> "ReconstructionWorkflowRequestV1":
        """Return a bounded child-workflow request for exactly one window."""
        if task.task_id not in {item.task_id for item in self.tasks}:
            raise ValueError("child task is not part of the parent request")
        return replace(
            self,
            tasks=(task,),
            max_parallel_windows=1,
            request_fingerprint="",
        )

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionWorkflowRequestV1":
        """Restore and verify a workflow request."""
        return cls(
            request_id=str(data.get("request_id", "")),
            run=ReconstructionRunV1.from_dict(_mapping(data.get("run"))),
            tasks=tuple(
                ReconstructionWindowTaskV1.from_dict(_mapping(item))
                for item in _sequence(data.get("tasks"))
            ),
            manifest_store_root=str(data.get("manifest_store_root", "")),
            report_root=str(data.get("report_root", "")),
            task_queues={
                str(key): str(value)
                for key, value in _mapping(data.get("task_queues")).items()
            },
            max_parallel_windows=cast(int, data.get("max_parallel_windows", 0)),
            max_inflight_memory_bytes=cast(
                int | None, data.get("max_inflight_memory_bytes")
            ),
            request_fingerprint=str(data.get("request_fingerprint", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionWindowStateV1:
    """Checkpoint plus completed stage receipts for one synchronized window."""

    request_id: str
    task: ReconstructionWindowTaskV1
    checkpoint: ReconstructionCheckpointV1
    outcomes: tuple[ReconstructionStageOutcomeV1, ...] = ()
    state_id: str = ""
    schema_version: str = RECONSTRUCTION_WINDOW_STATE_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_WINDOW_STATE_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction window state schema")
        object.__setattr__(self, "request_id", _required_text(self.request_id))
        if not isinstance(self.task, ReconstructionWindowTaskV1):
            raise ValueError("window state requires ReconstructionWindowTaskV1")
        if not isinstance(self.checkpoint, ReconstructionCheckpointV1):
            raise ValueError("window state requires ReconstructionCheckpointV1")
        window = self.task.window
        if (
            self.checkpoint.run_id != window.run_id
            or self.checkpoint.window_id != window.window_id
            or self.checkpoint.synchronization_unit_id
            != window.synchronization_unit_id
        ):
            raise ValueError("checkpoint scope differs from window task")
        outcomes = tuple(self.outcomes)
        expected_stages = RECONSTRUCTION_STAGE_ORDER[: len(outcomes)]
        if tuple(item.stage for item in outcomes) != expected_stages:
            raise ValueError(
                "window outcomes must be a strict stage-order prefix"
            )
        for index, (outcome, command) in enumerate(
            zip(outcomes, self.task.commands)
        ):
            _validate_stage_outcome(
                outcome,
                window,
                command,
                outcomes[:index],
            )
        object.__setattr__(self, "outcomes", outcomes)
        self._validate_phase()
        expected = _stable_id(
            "reconstruction-window-state", self.identity_payload()
        )
        if self.state_id and self.state_id != expected:
            raise ValueError("state_id does not match deterministic identity")
        object.__setattr__(self, "state_id", expected)
        _reject_inline_data(self.to_dict(), path="window_state")
        _ensure_payload_size(
            self.to_dict(),
            MAX_RECONSTRUCTION_WINDOW_STATE_BYTES,
            "reconstruction window state",
        )

    @classmethod
    def planned(
        cls, request_id: str, task: ReconstructionWindowTaskV1
    ) -> "ReconstructionWindowStateV1":
        """Return the initial durable state."""
        return cls(
            request_id=request_id,
            task=task,
            checkpoint=ReconstructionCheckpointV1.planned(task.window),
        )

    @property
    def terminal(self) -> bool:
        """Return whether the window will not run more stages this attempt."""
        return self.checkpoint.phase in {
            ReconstructionCommitPhase.COMMITTED,
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }

    @property
    def committed_manifest_ref(self) -> ArtifactRef | None:
        """Return the discoverable committed manifest, if any."""
        return self.checkpoint.committed_manifest_ref

    def outcome_for(
        self, stage: ReconstructionStage
    ) -> ReconstructionStageOutcomeV1 | None:
        """Return the stored receipt for a stage."""
        return next(
            (item for item in self.outcomes if item.stage is stage), None
        )

    def running(self) -> "ReconstructionWindowStateV1":
        """Start or resume work from the latest durable checkpoint."""
        if self.checkpoint.phase is ReconstructionCommitPhase.COMMITTED:
            return self
        if self.checkpoint.phase is ReconstructionCommitPhase.RUNNING:
            return self
        outcomes = self.outcomes
        if self.checkpoint.phase is ReconstructionCommitPhase.CANCELLED:
            # Cancellation cleanup removes the entire window scratch tree,
            # including every uncommitted receipt and stage artifact.  A
            # resumed attempt must therefore rebuild the whole disposable
            # prefix instead of retaining references to deleted files.
            outcomes = ()
        elif (
            self.checkpoint.phase is ReconstructionCommitPhase.FAILED
            and ReconstructionStage.VALIDATION
            in tuple(item.stage for item in outcomes)
        ):
            validation_index = RECONSTRUCTION_STAGE_ORDER.index(
                ReconstructionStage.VALIDATION
            )
            outcomes = outcomes[:validation_index]
        checkpoint = self.checkpoint.transition(
            ReconstructionCommitPhase.RUNNING,
            expected_checkpoint_id=self.checkpoint.checkpoint_id,
        )
        return replace(
            self,
            checkpoint=checkpoint,
            outcomes=outcomes,
            state_id="",
        )

    def complete(
        self, outcome: ReconstructionStageOutcomeV1
    ) -> "ReconstructionWindowStateV1":
        """Append one successful stage and advance its checkpoint phase."""
        if outcome.status is not ReconstructionStageStatus.COMPLETED:
            raise ValueError("only completed outcomes can advance a window")
        expected_stage = RECONSTRUCTION_STAGE_ORDER[len(self.outcomes)]
        if outcome.stage is not expected_stage:
            raise ValueError("stage completion is out of order")
        command = self.task.commands[len(self.outcomes)]
        _validate_stage_outcome(
            outcome, self.task.window, command, self.outcomes
        )
        checkpoint = self.checkpoint
        if outcome.stage is ReconstructionStage.VALIDATION:
            staged = _phase_artifact(outcome, "staged")
            checkpoint = checkpoint.transition(
                ReconstructionCommitPhase.STAGED,
                expected_checkpoint_id=checkpoint.checkpoint_id,
                staged_manifest_ref=staged,
            )
        elif outcome.stage is ReconstructionStage.ATOMIC_PARTITION_COMMIT:
            committed = _phase_artifact(outcome, "committed")
            if (
                Path(committed.path)
                .expanduser()
                .resolve()
                .is_relative_to(
                    Path(self.task.scratch_directory).expanduser().resolve()
                )
            ):
                raise ValueError(
                    "committed manifest must remain outside disposable window scratch"
                )
            checkpoint = checkpoint.transition(
                ReconstructionCommitPhase.COMMITTED,
                expected_checkpoint_id=checkpoint.checkpoint_id,
                committed_manifest_ref=committed,
                output_watermark_ns=self.task.window.core_end_ns,
            )
        else:
            checkpoint = checkpoint.transition(
                ReconstructionCommitPhase.RUNNING,
                expected_checkpoint_id=checkpoint.checkpoint_id,
            )
        return replace(
            self,
            checkpoint=checkpoint,
            outcomes=(*self.outcomes, replace(outcome, reused=False)),
            state_id="",
        )

    def validated(self) -> "ReconstructionWindowStateV1":
        """Persist the second phase of the validation/commit protocol."""
        if self.checkpoint.phase is ReconstructionCommitPhase.VALIDATED:
            return self
        if self.checkpoint.phase is not ReconstructionCommitPhase.STAGED:
            raise ValueError("only a staged window can become validated")
        checkpoint = self.checkpoint.transition(
            ReconstructionCommitPhase.VALIDATED,
            expected_checkpoint_id=self.checkpoint.checkpoint_id,
        )
        return replace(self, checkpoint=checkpoint, state_id="")

    def interrupted(
        self,
        phase: ReconstructionCommitPhase,
        reason: str,
    ) -> "ReconstructionWindowStateV1":
        """Record a resumable cancellation or explicit refusal."""
        if phase not in {
            ReconstructionCommitPhase.CANCELLED,
            ReconstructionCommitPhase.FAILED,
        }:
            raise ValueError("interrupted state must be cancelled or failed")
        if self.checkpoint.phase is ReconstructionCommitPhase.COMMITTED:
            return self
        checkpoint = self.checkpoint.transition(
            phase,
            expected_checkpoint_id=self.checkpoint.checkpoint_id,
            interruption_reason=_bounded_text(reason),
        )
        return replace(self, checkpoint=checkpoint, state_id="")

    def _validate_phase(self) -> None:
        stages = tuple(item.stage for item in self.outcomes)
        phase = self.checkpoint.phase
        committed = ReconstructionStage.ATOMIC_PARTITION_COMMIT in stages
        validation = ReconstructionStage.VALIDATION in stages
        if committed != (phase is ReconstructionCommitPhase.COMMITTED):
            raise ValueError("committed outcome/checkpoint phase differs")
        if (
            phase
            in {
                ReconstructionCommitPhase.STAGED,
                ReconstructionCommitPhase.VALIDATED,
            }
            and not validation
        ):
            raise ValueError(
                "staged/validated checkpoint lacks validation outcome"
            )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return durable state identity fields."""
        return {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "task": self.task.to_dict(),
            "checkpoint": self.checkpoint.to_dict(),
            "outcomes": [item.to_dict() for item in self.outcomes],
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return bounded recovery metadata."""
        return {**self.identity_payload(), "state_id": self.state_id}

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "ReconstructionWindowStateV1":
        """Restore and verify durable window state."""
        return cls(
            request_id=str(data.get("request_id", "")),
            task=ReconstructionWindowTaskV1.from_dict(
                _mapping(data.get("task"))
            ),
            checkpoint=ReconstructionCheckpointV1.from_dict(
                _mapping(data.get("checkpoint"))
            ),
            outcomes=tuple(
                ReconstructionStageOutcomeV1.from_dict(_mapping(item))
                for item in _sequence(data.get("outcomes"))
            ),
            state_id=str(data.get("state_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


@dataclass(frozen=True, slots=True)
class ReconstructionRunReportV1:
    """Compact reconciliation of workflow state and committed manifests."""

    request_id: str
    run_id: str
    status: str
    window_count: int
    committed_window_count: int
    cancelled_window_count: int
    failed_window_count: int
    observed_event_count: int
    synthetic_event_count: int
    committed_manifest_refs: tuple[ArtifactRef, ...]
    window_states: tuple[dict[str, JSONValue], ...]
    report_id: str = ""
    schema_version: str = RECONSTRUCTION_REPORT_SCHEMA_VERSION

    def __post_init__(self) -> None:
        if self.schema_version != RECONSTRUCTION_REPORT_SCHEMA_VERSION:
            raise ValueError("unsupported reconstruction report schema")
        object.__setattr__(self, "request_id", _required_text(self.request_id))
        object.__setattr__(self, "run_id", _required_text(self.run_id))
        if self.status not in {"committed", "partial", "cancelled", "failed"}:
            raise ValueError("unsupported reconstruction report status")
        for name in (
            "window_count",
            "committed_window_count",
            "cancelled_window_count",
            "failed_window_count",
            "observed_event_count",
            "synthetic_event_count",
        ):
            object.__setattr__(
                self, name, _nonnegative_int(getattr(self, name), name)
            )
        refs = _unique_strong_refs(
            self.committed_manifest_refs, "committed_manifest_refs"
        )
        object.__setattr__(self, "committed_manifest_refs", refs)
        summaries = tuple(dict(item) for item in self.window_states)
        object.__setattr__(self, "window_states", summaries)
        if len(summaries) != self.window_count:
            raise ValueError("window_states count differs from window_count")
        if len(refs) != self.committed_window_count:
            raise ValueError(
                "committed manifest count differs from committed windows"
            )
        terminal_count = (
            self.committed_window_count
            + self.cancelled_window_count
            + self.failed_window_count
        )
        if terminal_count > self.window_count:
            raise ValueError("terminal window counts exceed window_count")
        expected = _stable_id("reconstruction-report", self.identity_payload())
        if self.report_id and self.report_id != expected:
            raise ValueError("report_id does not match deterministic identity")
        object.__setattr__(self, "report_id", expected)
        _reject_inline_data(self.to_dict(), path="reconstruction_report")
        _ensure_payload_size(
            self.to_dict(),
            MAX_RECONSTRUCTION_REPORT_BYTES,
            "reconstruction report",
        )

    def identity_payload(self) -> dict[str, JSONValue]:
        """Return deterministic report evidence."""
        return {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "run_id": self.run_id,
            "status": self.status,
            "window_count": self.window_count,
            "committed_window_count": self.committed_window_count,
            "cancelled_window_count": self.cancelled_window_count,
            "failed_window_count": self.failed_window_count,
            "observed_event_count": self.observed_event_count,
            "synthetic_event_count": self.synthetic_event_count,
            "committed_manifest_refs": [
                _artifact_identity(ref) for ref in self.committed_manifest_refs
            ],
            "window_states": list(self.window_states),
        }

    def to_dict(self) -> dict[str, JSONValue]:
        """Return report JSON with strong artifact references."""
        return {
            **self.identity_payload(),
            "report_id": self.report_id,
            "committed_manifest_refs": [
                ref.to_dict() for ref in self.committed_manifest_refs
            ],
        }

    def to_json(self) -> str:
        """Return deterministic compact JSON."""
        return str(canonical_contract_json(self.to_dict()))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ReconstructionRunReportV1":
        """Restore and verify a reconstruction report."""
        return cls(
            request_id=str(data.get("request_id", "")),
            run_id=str(data.get("run_id", "")),
            status=str(data.get("status", "")),
            window_count=cast(int, data.get("window_count", 0)),
            committed_window_count=cast(
                int, data.get("committed_window_count", 0)
            ),
            cancelled_window_count=cast(
                int, data.get("cancelled_window_count", 0)
            ),
            failed_window_count=cast(int, data.get("failed_window_count", 0)),
            observed_event_count=cast(int, data.get("observed_event_count", 0)),
            synthetic_event_count=cast(
                int, data.get("synthetic_event_count", 0)
            ),
            committed_manifest_refs=_artifact_refs(
                data.get("committed_manifest_refs")
            ),
            window_states=tuple(
                dict(_mapping(item))
                for item in _sequence(data.get("window_states"))
            ),
            report_id=str(data.get("report_id", "")),
            schema_version=str(data.get("schema_version", "")),
        )


class ReconstructionCheckpointStore:
    """Optimistic window-state repository backed by the manifest SQLite DB."""

    def __init__(self, root: str | Path):
        self.store = ManifestStatusStore(root)

    @staticmethod
    def job_id_for(window: ReconstructionWindowV1) -> str:
        """Return a compact stable manifest-store key."""
        digest = hashlib.sha256(
            f"{window.run_id}|{window.window_id}".encode("utf-8")
        ).hexdigest()[:24]
        return f"reconstruction-window-{digest}"

    def initialize(
        self,
        request_id: str,
        task: ReconstructionWindowTaskV1,
    ) -> ReconstructionWindowStateV1:
        """Create the initial state or return the already durable state."""
        state = ReconstructionWindowStateV1.planned(request_id, task)
        if self.store.compare_and_swap_job_snapshot(
            self._snapshot(state), expected_snapshot_id=None
        ):
            return state
        existing = self.load(task.window)
        if existing is None:
            raise ReconstructionCheckpointConflict(
                "checkpoint initialization lost without a durable winner"
            )
        self._validate_task(existing, request_id, task)
        return existing

    def load(
        self, window: ReconstructionWindowV1
    ) -> ReconstructionWindowStateV1 | None:
        """Load and verify the latest durable state after any process restart."""
        payload = self.store.get_job_snapshot(self.job_id_for(window))
        if payload is None:
            return None
        state = ReconstructionWindowStateV1.from_dict(
            _mapping(payload.get("reconstruction_window_state"))
        )
        if state.state_id != str(payload.get("snapshot_id", "")):
            raise ReconstructionArtifactError(
                "stored reconstruction snapshot identity differs from state"
            )
        return state

    def save(
        self,
        state: ReconstructionWindowStateV1,
        *,
        expected_state_id: str,
    ) -> ReconstructionWindowStateV1:
        """Advance the chain, resolving identical duplicate completion."""
        if self.store.compare_and_swap_job_snapshot(
            self._snapshot(state), expected_snapshot_id=expected_state_id
        ):
            return state
        current = self.load(state.task.window)
        if current is None:
            raise ReconstructionCheckpointConflict(
                "checkpoint disappeared during save"
            )
        if _state_contains(current, state):
            return current
        raise ReconstructionCheckpointConflict(
            "stale reconstruction checkpoint transition rejected"
        )

    @staticmethod
    def _validate_task(
        state: ReconstructionWindowStateV1,
        request_id: str,
        task: ReconstructionWindowTaskV1,
    ) -> None:
        if state.request_id != request_id or state.task.task_id != task.task_id:
            raise ReconstructionCheckpointConflict(
                "durable checkpoint belongs to a different request or task"
            )

    def _snapshot(
        self, state: ReconstructionWindowStateV1
    ) -> dict[str, JSONValue]:
        phase = state.checkpoint.phase
        status = {
            ReconstructionCommitPhase.COMMITTED: "COMPLETED",
            ReconstructionCommitPhase.CANCELLED: "CANCELLED",
            ReconstructionCommitPhase.FAILED: "FAILED",
        }.get(
            phase,
            (
                "PLANNED"
                if phase is ReconstructionCommitPhase.PLANNED
                else "RUNNING"
            ),
        )
        job_id = self.job_id_for(state.task.window)
        artifacts: list[JSONValue] = [
            ref.to_dict()
            for outcome in state.outcomes
            for ref in outcome.output_refs
        ]
        return {
            "schema_version": 1,
            "job_id": job_id,
            "request_id": state.request_id,
            "workflow_id": job_id,
            "run_id": state.task.window.run_id,
            "lifecycle": phase.value,
            "status": status,
            "snapshot_id": state.state_id,
            "reconstruction_window_state": state.to_dict(),
            "artifacts": artifacts,
        }


@dataclass(frozen=True, slots=True)
class ReconstructionStageInvocationV1:
    """Bounded activity input passed to a data-plane stage handler."""

    run: ReconstructionRunV1
    task: ReconstructionWindowTaskV1
    command: ReconstructionStageCommandV1
    prior_outcomes: tuple[ReconstructionStageOutcomeV1, ...]
    heartbeat_callback: Callable[[ReconstructionHeartbeatV1], Any] | None = (
        field(
            default=None,
            compare=False,
            repr=False,
        )
    )
    cancellation_check: Callable[[], bool] | None = field(
        default=None,
        compare=False,
        repr=False,
    )

    @property
    def input_fingerprint(self) -> str:
        """Hash declared inputs and prior outputs for retry validation."""
        payload: dict[str, JSONValue] = {
            "run_id": self.run.run_id,
            "window_id": self.task.window.window_id,
            "command_id": self.command.command_id,
            "input_refs": [
                _artifact_identity(ref)
                for ref in (
                    *self.command.input_manifest_refs,
                    *self.command.configuration_refs,
                    *tuple(
                        ref
                        for outcome in self.prior_outcomes
                        for ref in outcome.output_refs
                    ),
                )
            ],
            "prior_outcome_ids": [
                item.outcome_id for item in self.prior_outcomes
            ],
        }
        return hashlib.sha256(
            canonical_contract_json(payload).encode("utf-8")
        ).hexdigest()

    @property
    def cancellation_requested(self) -> bool:
        """Return whether the activity should stop producing new work."""
        return bool(self.cancellation_check and self.cancellation_check())

    def heartbeat(
        self,
        *,
        sequence: int,
        completed_units: int,
        total_units: int,
        observed_event_count: int = 0,
        candidate_event_count: int = 0,
        accepted_event_count: int = 0,
        scratch_bytes: int = 0,
        output_bytes: int = 0,
        message: str = "",
    ) -> None:
        """Emit bounded intra-stage progress from a long-running handler."""
        if self.heartbeat_callback is None:
            return
        heartbeat = ReconstructionHeartbeatV1(
            run_id=self.task.window.run_id,
            window_id=self.task.window.window_id,
            synchronization_unit_id=self.task.window.synchronization_unit_id,
            phase=ReconstructionCommitPhase.RUNNING,
            sequence=sequence,
            completed_units=completed_units,
            total_units=total_units,
            observed_event_count=observed_event_count,
            candidate_event_count=candidate_event_count,
            accepted_event_count=accepted_event_count,
            scratch_bytes=scratch_bytes,
            output_bytes=output_bytes,
            cancellation_requested=self.cancellation_requested,
            message=message or self.command.stage.value,
        )
        result = self.heartbeat_callback(heartbeat)
        if inspect.isawaitable(result):
            raise TypeError(
                "stage-handler heartbeat callback must be synchronous"
            )

    def completed(
        self,
        *,
        output_refs: Sequence[ArtifactRef],
        observed_event_count: int = 0,
        candidate_event_count: int = 0,
        accepted_event_count: int = 0,
        scratch_bytes: int = 0,
        output_bytes: int = 0,
        message: str = "",
    ) -> ReconstructionStageOutcomeV1:
        """Build a correctly scoped successful stage outcome."""
        window = self.task.window
        return ReconstructionStageOutcomeV1(
            run_id=window.run_id,
            window_id=window.window_id,
            synchronization_unit_id=window.synchronization_unit_id,
            stage=self.command.stage,
            command_id=self.command.command_id,
            input_fingerprint=self.input_fingerprint,
            status=ReconstructionStageStatus.COMPLETED,
            output_refs=tuple(output_refs),
            observed_event_count=observed_event_count,
            candidate_event_count=candidate_event_count,
            accepted_event_count=accepted_event_count,
            scratch_bytes=scratch_bytes,
            output_bytes=output_bytes,
            message=message,
        )

    def refused(
        self,
        *reasons: str,
        message: str = "",
    ) -> ReconstructionStageOutcomeV1:
        """Build a fail-closed scientific/resource refusal receipt."""
        window = self.task.window
        return ReconstructionStageOutcomeV1(
            run_id=window.run_id,
            window_id=window.window_id,
            synchronization_unit_id=window.synchronization_unit_id,
            stage=self.command.stage,
            command_id=self.command.command_id,
            input_fingerprint=self.input_fingerprint,
            status=ReconstructionStageStatus.REFUSED,
            refusal_reasons=tuple(reasons),
            message=message,
        )


ReconstructionStageHandler = Callable[
    [ReconstructionStageInvocationV1],
    ReconstructionStageOutcomeV1 | Awaitable[ReconstructionStageOutcomeV1],
]
_STAGE_HANDLERS: dict[str, ReconstructionStageHandler] = {}


def register_reconstruction_stage_handler(
    name: str,
    handler: ReconstructionStageHandler,
    *,
    replace_existing: bool = False,
) -> None:
    """Register an activity-side adapter outside workflow state."""
    normalized = _required_text(name)
    if not callable(handler):
        raise TypeError("reconstruction stage handler must be callable")
    if normalized in _STAGE_HANDLERS and not replace_existing:
        raise ValueError(
            f"reconstruction stage handler already registered: {normalized}"
        )
    _STAGE_HANDLERS[normalized] = handler


def unregister_reconstruction_stage_handler(name: str) -> None:
    """Remove a process-local stage adapter, primarily for test isolation."""
    _STAGE_HANDLERS.pop(str(name).strip(), None)


def registered_reconstruction_stage_handlers() -> (
    Mapping[str, ReconstructionStageHandler]
):
    """Return an isolated snapshot of installed activity-side adapters."""
    return dict(_STAGE_HANDLERS)


async def execute_reconstruction_stage(
    invocation: ReconstructionStageInvocationV1,
    *,
    verify_outputs: bool = True,
) -> ReconstructionStageOutcomeV1:
    """Execute or reuse one idempotent artifact-producing stage receipt."""
    for ref in (
        *invocation.command.input_manifest_refs,
        *invocation.command.configuration_refs,
        *tuple(
            ref
            for outcome in invocation.prior_outcomes
            for ref in outcome.output_refs
        ),
    ):
        verify_artifact_ref(ref)
    try:
        _verify_command_storage(invocation.command)
    except ReconstructionStorageRootError as err:
        raise ReconstructionArtifactError(str(err)) from err
    receipt_path = Path(invocation.command.receipt_path).expanduser()
    if receipt_path.exists():
        outcome = _read_stage_receipt(receipt_path)
        _validate_stage_outcome(
            outcome,
            invocation.task.window,
            invocation.command,
            invocation.prior_outcomes,
        )
        if verify_outputs:
            for ref in outcome.output_refs:
                verify_artifact_ref(ref)
        return replace(outcome, reused=True)

    handler = _STAGE_HANDLERS.get(invocation.command.handler_name)
    if handler is None:
        raise ReconstructionArtifactError(
            "no reconstruction stage handler is registered for "
            f"{invocation.command.handler_name!r}, and no durable receipt exists"
        )
    result = handler(invocation)
    if inspect.isawaitable(result):
        result = await result
    outcome = result
    _validate_stage_outcome(
        outcome,
        invocation.task.window,
        invocation.command,
        invocation.prior_outcomes,
    )
    if verify_outputs:
        for ref in outcome.output_refs:
            verify_artifact_ref(ref)
    try:
        _verify_command_storage(invocation.command)
    except ReconstructionStorageRootError as err:
        raise ReconstructionArtifactError(str(err)) from err
    _write_stage_receipt(receipt_path, outcome)
    return outcome


def _verify_command_storage(command: ReconstructionStageCommandV1) -> None:
    for ref in command.configuration_refs:
        verify_reconstruction_storage_for_execution(ref)


class ReconstructionStageExecutor(Protocol):
    """Activity boundary used by local tests and Temporal workflows."""

    async def execute(
        self, invocation: ReconstructionStageInvocationV1
    ) -> ReconstructionStageOutcomeV1:
        """Run one stage and return bounded receipt metadata."""


class RegisteredReconstructionStageExecutor:
    """Execute stages through the process-local activity handler registry."""

    async def execute(
        self, invocation: ReconstructionStageInvocationV1
    ) -> ReconstructionStageOutcomeV1:
        """Execute or reuse the stage receipt."""
        return await execute_reconstruction_stage(invocation)


HeartbeatCallback = Callable[
    [ReconstructionHeartbeatV1], None | Awaitable[None]
]
CancellationCheck = Callable[[], bool]


async def run_reconstruction_window(
    request: ReconstructionWorkflowRequestV1,
    task: ReconstructionWindowTaskV1,
    *,
    checkpoint_store: ReconstructionCheckpointStore,
    stage_executor: ReconstructionStageExecutor,
    heartbeat: HeartbeatCallback | None = None,
    cancellation_requested: CancellationCheck | None = None,
) -> ReconstructionWindowStateV1:
    """Run or resume one synchronized window from its last valid checkpoint."""
    state = checkpoint_store.initialize(request.request_id, task)
    if state.checkpoint.phase is ReconstructionCommitPhase.COMMITTED:
        return state
    try:
        request.run.storage_policy.preflight(task.resource_estimate)
    except ReconstructionResourceLimitError as err:
        if state.checkpoint.phase is not ReconstructionCommitPhase.FAILED:
            failed = state.interrupted(
                ReconstructionCommitPhase.FAILED,
                "; ".join(err.violations),
            )
            state = checkpoint_store.save(
                failed, expected_state_id=state.state_id
            )
        return state

    lane_limit = cast(int, request.max_inflight_memory_bytes)
    if task.resource_estimate.estimated_memory_bytes > lane_limit:
        if state.checkpoint.phase is not ReconstructionCommitPhase.FAILED:
            failed = state.interrupted(
                ReconstructionCommitPhase.FAILED,
                "estimated_memory_bytes "
                f"{task.resource_estimate.estimated_memory_bytes} exceeds "
                f"lane limit {lane_limit}",
            )
            state = checkpoint_store.save(
                failed, expected_state_id=state.state_id
            )
        return state

    if state.checkpoint.phase in {
        ReconstructionCommitPhase.CANCELLED,
        ReconstructionCommitPhase.FAILED,
    }:
        state = checkpoint_store.save(
            state.running(), expected_state_id=state.state_id
        )
    elif state.checkpoint.phase is ReconstructionCommitPhase.PLANNED:
        state = checkpoint_store.save(
            state.running(), expected_state_id=state.state_id
        )

    if state.checkpoint.phase is ReconstructionCommitPhase.STAGED:
        state = checkpoint_store.save(
            state.validated(), expected_state_id=state.state_id
        )

    while len(state.outcomes) < len(task.commands):
        if cancellation_requested is not None and cancellation_requested():
            cancelled = state.interrupted(
                ReconstructionCommitPhase.CANCELLED,
                "cancellation requested before next stage",
            )
            state = checkpoint_store.save(
                cancelled, expected_state_id=state.state_id
            )
            cleanup_reconstruction_window_scratch(task.scratch_directory)
            return state
        command = task.commands[len(state.outcomes)]
        invocation = ReconstructionStageInvocationV1(
            run=request.run,
            task=task,
            command=command,
            prior_outcomes=state.outcomes,
            heartbeat_callback=heartbeat,
            cancellation_check=cancellation_requested,
        )
        await _emit_heartbeat(
            heartbeat, _heartbeat_for(state, command.stage, False)
        )
        outcome = await stage_executor.execute(invocation)
        resource_violations = _outcome_resource_violations(
            request.run.storage_policy,
            task.resource_estimate,
            outcome,
        )
        if resource_violations:
            refused = state.interrupted(
                ReconstructionCommitPhase.FAILED,
                "; ".join(resource_violations),
            )
            return checkpoint_store.save(
                refused, expected_state_id=state.state_id
            )
        if outcome.status is ReconstructionStageStatus.REFUSED:
            refused = state.interrupted(
                ReconstructionCommitPhase.FAILED,
                _bounded_reason_summary(outcome.refusal_reasons),
            )
            state = checkpoint_store.save(
                refused, expected_state_id=state.state_id
            )
            return state
        next_state = state.complete(outcome)
        state = checkpoint_store.save(
            next_state, expected_state_id=state.state_id
        )
        await _emit_heartbeat(
            heartbeat, _heartbeat_for(state, command.stage, True)
        )
        if (
            command.stage is ReconstructionStage.VALIDATION
            and state.checkpoint.phase is ReconstructionCommitPhase.STAGED
        ):
            validated = state.validated()
            state = checkpoint_store.save(
                validated, expected_state_id=state.state_id
            )
    return state


def plan_reconstruction_waves(
    tasks: Sequence[ReconstructionWindowTaskV1],
    *,
    max_parallel_windows: int,
    max_inflight_memory_bytes: int,
) -> tuple[tuple[ReconstructionWindowTaskV1, ...], ...]:
    """Plan deterministic bounded waves that enforce producer backpressure."""
    maximum = _positive_int(max_parallel_windows, "max_parallel_windows")
    byte_limit = _positive_int(
        max_inflight_memory_bytes, "max_inflight_memory_bytes"
    )
    waves: list[tuple[ReconstructionWindowTaskV1, ...]] = []
    current: list[ReconstructionWindowTaskV1] = []
    current_bytes = 0
    for task in sorted(tasks, key=lambda item: item.window.core_start_ns):
        weight = max(1, task.resource_estimate.estimated_memory_bytes)
        if weight > byte_limit:
            if current:
                waves.append(tuple(current))
                current = []
                current_bytes = 0
            waves.append((task,))
            continue
        if current and (
            len(current) >= maximum or current_bytes + weight > byte_limit
        ):
            waves.append(tuple(current))
            current = []
            current_bytes = 0
        current.append(task)
        current_bytes += weight
    if current:
        waves.append(tuple(current))
    return tuple(waves)


async def run_reconstruction_request(
    request: ReconstructionWorkflowRequestV1,
    *,
    stage_executor: ReconstructionStageExecutor | None = None,
    heartbeat: HeartbeatCallback | None = None,
    cancellation_requested: CancellationCheck | None = None,
) -> tuple[ReconstructionWindowStateV1, ...]:
    """Execute a request locally with the same wave backpressure as Temporal."""
    executor = stage_executor or RegisteredReconstructionStageExecutor()
    store = ReconstructionCheckpointStore(request.manifest_store_root)
    states: list[ReconstructionWindowStateV1] = []
    waves = plan_reconstruction_waves(
        request.tasks,
        max_parallel_windows=request.max_parallel_windows,
        max_inflight_memory_bytes=cast(int, request.max_inflight_memory_bytes),
    )
    for wave in waves:
        wave_states = await asyncio.gather(
            *(
                run_reconstruction_window(
                    request,
                    task,
                    checkpoint_store=store,
                    stage_executor=executor,
                    heartbeat=heartbeat,
                    cancellation_requested=cancellation_requested,
                )
                for task in wave
            )
        )
        states.extend(wave_states)
        if cancellation_requested is not None and cancellation_requested():
            break
    return tuple(
        sorted(states, key=lambda item: item.task.window.core_start_ns)
    )


def reconcile_reconstruction_report(
    request: ReconstructionWorkflowRequestV1,
    states: Sequence[ReconstructionWindowStateV1],
    *,
    progress: Callable[[Mapping[str, JSONValue]], None] | None = None,
) -> ReconstructionRunReportV1:
    """Reconcile durable workflow checkpoints with final storage manifests."""
    ordered = tuple(
        sorted(states, key=lambda item: item.task.window.core_start_ns)
    )
    if len(ordered) != len(request.tasks):
        raise ReconstructionReportMismatch(
            "report state count differs from requested window count"
        )
    expected = {item.window.window_id for item in request.tasks}
    if {item.task.window.window_id for item in ordered} != expected:
        raise ReconstructionReportMismatch(
            "report window set differs from request"
        )
    refs: list[ArtifactRef] = []
    window_summaries: list[dict[str, JSONValue]] = []
    observed = 0
    synthetic = 0
    for index, state in enumerate(ordered, start=1):
        window = state.task.window
        if progress is not None:
            progress(
                {
                    "phase": "report_reconciliation",
                    "window_id": window.window_id,
                    "completed_windows": index - 1,
                    "total_windows": len(ordered),
                }
            )
        summary: dict[str, JSONValue] = {
            "window_id": window.window_id,
            "synchronization_unit_id": window.synchronization_unit_id,
            "phase": state.checkpoint.phase.value,
            "checkpoint_id": state.checkpoint.checkpoint_id,
            "completed_stages": [item.stage.value for item in state.outcomes],
            "resource_usage": _aggregate_outcome_telemetry(state.outcomes),
        }
        manifest_ref = state.committed_manifest_ref
        if manifest_ref is not None:
            verify_artifact_ref(manifest_ref)
            manifest = verify_reconstruction_publication(manifest_ref.path)
            if (
                manifest.run_id != request.run.run_id
                or manifest.window_id != window.window_id
                or manifest.synchronization_unit_id
                != window.synchronization_unit_id
                or tuple(sorted(manifest.symbols))
                != tuple(sorted(window.symbols))
            ):
                message = (
                    "committed manifest scope differs for window "
                    f"{window.window_id}"
                )
                raise ReconstructionReportMismatch(message)
            refs.append(manifest_ref)
            observed += manifest.observed_event_count
            synthetic += manifest.synthetic_event_count
            summary.update(
                {
                    "manifest_id": manifest.manifest_id,
                    "publication_id": manifest.publication_id,
                    "event_count": manifest.event_count,
                    "observed_event_count": manifest.observed_event_count,
                    "synthetic_event_count": manifest.synthetic_event_count,
                }
            )
        elif state.checkpoint.phase is ReconstructionCommitPhase.COMMITTED:
            raise ReconstructionReportMismatch(
                f"committed window {window.window_id} lacks manifest reference"
            )
        window_summaries.append(summary)
        if progress is not None:
            progress(
                {
                    "phase": "report_reconciliation",
                    "window_id": window.window_id,
                    "completed_windows": index,
                    "total_windows": len(ordered),
                }
            )
    committed = sum(
        item.checkpoint.phase is ReconstructionCommitPhase.COMMITTED
        for item in ordered
    )
    cancelled = sum(
        item.checkpoint.phase is ReconstructionCommitPhase.CANCELLED
        for item in ordered
    )
    failed = sum(
        item.checkpoint.phase is ReconstructionCommitPhase.FAILED
        for item in ordered
    )
    status = "committed"
    if committed != len(ordered):
        if failed:
            status = "failed" if committed == 0 else "partial"
        elif cancelled:
            status = "cancelled" if committed == 0 else "partial"
        else:
            status = "partial"
    return ReconstructionRunReportV1(
        request_id=request.request_id,
        run_id=request.run.run_id,
        status=status,
        window_count=len(ordered),
        committed_window_count=committed,
        cancelled_window_count=cancelled,
        failed_window_count=failed,
        observed_event_count=observed,
        synthetic_event_count=synthetic,
        committed_manifest_refs=tuple(refs),
        window_states=tuple(window_summaries),
    )


def _aggregate_outcome_telemetry(
    outcomes: Sequence[ReconstructionStageOutcomeV1],
) -> dict[str, JSONValue]:
    """Reconcile stage telemetry without counting duplicate output refs."""
    runtimes: list[float] = []
    peak_rss: list[int] = []
    scratch: list[int] = []
    output: list[int] = []
    amplification: list[float] = []
    for outcome in outcomes:
        metadata = [ref.metadata for ref in outcome.output_refs]
        runtimes.append(
            max(
                (
                    _metadata_number(item, "runtime_seconds")
                    for item in metadata
                ),
                default=0.0,
            )
        )
        peak_rss.append(
            max(
                (_metadata_int(item, "peak_rss_bytes") for item in metadata),
                default=0,
            )
        )
        scratch.append(
            max(
                (_metadata_int(item, "scratch_bytes") for item in metadata),
                default=0,
            )
        )
        output.append(
            max(
                (_metadata_int(item, "output_bytes") for item in metadata),
                default=0,
            )
        )
        amplification.append(
            max(
                (
                    _metadata_number(item, "candidate_amplification")
                    for item in metadata
                ),
                default=0.0,
            )
        )
    return {
        "runtime_seconds": round(sum(runtimes), 6),
        "peak_rss_bytes": max(peak_rss, default=0),
        "peak_scratch_bytes": max(scratch, default=0),
        "stage_output_bytes_total": sum(output),
        "peak_candidate_amplification": round(
            max(amplification, default=0.0), 9
        ),
        "basis": "sum-stage-runtime-max-stage-resources-v1",
    }


def _metadata_number(metadata: Mapping[str, JSONValue], key: str) -> float:
    value = metadata.get(key, 0.0)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return 0.0
    return float(value)


def _metadata_int(metadata: Mapping[str, JSONValue], key: str) -> int:
    value = metadata.get(key, 0)
    if isinstance(value, bool) or not isinstance(value, int):
        return 0
    return max(0, value)


def write_reconstruction_report(
    report: ReconstructionRunReportV1,
    root: str | Path,
) -> ArtifactRef:
    """Atomically write the compact reconciled run report."""
    directory = (
        Path(root).expanduser()
        / ".histdatacom"
        / RECONSTRUCTION_REPORT_DIRECTORY
    )
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{_safe_filename(report.request_id)}.json"
    payload = report.to_json().encode("utf-8")
    _atomic_write(path, payload)
    return ArtifactRef(
        kind="reconstruction_run_report",
        path=str(path.resolve()),
        size_bytes=len(payload),
        sha256=hashlib.sha256(payload).hexdigest(),
        metadata={"report_id": report.report_id, "run_id": report.run_id},
    )


def verify_artifact_ref(ref: ArtifactRef) -> Path:
    """Fail closed unless a local strong artifact reference matches bytes."""
    strong = _strong_ref(ref)
    path = Path(strong.path).expanduser()
    if not path.is_file():
        raise ReconstructionArtifactError(f"artifact is missing: {path}")
    size = path.stat().st_size
    if size != strong.size_bytes:
        raise ReconstructionArtifactError(
            f"artifact size differs for {path}: {size} != {strong.size_bytes}"
        )
    digest = _file_sha256(path)
    if digest != strong.sha256:
        raise ReconstructionArtifactError(f"artifact sha256 differs for {path}")
    return path


def artifact_ref_for_file(
    path: str | Path,
    *,
    kind: str,
    metadata: Mapping[str, JSONValue] | None = None,
) -> ArtifactRef:
    """Build a strong reference for one existing local artifact."""
    target = Path(path).expanduser().resolve()
    if not target.is_file():
        raise ReconstructionArtifactError(f"artifact is missing: {target}")
    return ArtifactRef(
        kind=_required_text(kind),
        path=str(target),
        size_bytes=target.stat().st_size,
        sha256=_file_sha256(target),
        metadata=dict(metadata or {}),
    )


def cleanup_reconstruction_window_scratch(path: str | Path) -> bool:
    """Remove only the explicitly scoped uncommitted window scratch tree."""
    target = Path(path).expanduser()
    if not target.exists():
        return False
    if target.is_symlink() or not target.is_dir():
        raise ReconstructionArtifactError(
            "reconstruction scratch path must be a real directory"
        )
    if target.resolve() in {Path.home().resolve(), Path("/").resolve()}:
        raise ReconstructionArtifactError(
            "refusing unsafe scratch cleanup root"
        )
    shutil.rmtree(target)
    return True


def _heartbeat_for(
    state: ReconstructionWindowStateV1,
    stage: ReconstructionStage,
    completed: bool,
) -> ReconstructionHeartbeatV1:
    outcomes = state.outcomes
    completed_units = len(outcomes)
    current = outcomes[-1] if outcomes else None
    stage_status = "completed" if completed else "starting"
    return ReconstructionHeartbeatV1(
        run_id=state.task.window.run_id,
        window_id=state.task.window.window_id,
        synchronization_unit_id=state.task.window.synchronization_unit_id,
        phase=state.checkpoint.phase,
        sequence=state.checkpoint.revision,
        completed_units=completed_units,
        total_units=len(RECONSTRUCTION_STAGE_ORDER),
        observed_event_count=current.observed_event_count if current else 0,
        candidate_event_count=current.candidate_event_count if current else 0,
        accepted_event_count=current.accepted_event_count if current else 0,
        scratch_bytes=current.scratch_bytes if current else 0,
        output_bytes=current.output_bytes if current else 0,
        checkpoint_id=state.checkpoint.checkpoint_id,
        message=f"{stage.value}:{stage_status}",
    )


async def _emit_heartbeat(
    callback: HeartbeatCallback | None,
    heartbeat: ReconstructionHeartbeatV1,
) -> None:
    if callback is None:
        return
    result = callback(heartbeat)
    if inspect.isawaitable(result):
        await result


def _outcome_resource_violations(
    policy: ReconstructionStoragePolicyV1,
    estimate: ReconstructionResourceEstimateV1,
    outcome: ReconstructionStageOutcomeV1,
) -> tuple[str, ...]:
    """Return fail-closed evidence when actual stage use exceeds admission."""
    violations: list[str] = []
    limits = (
        (
            "candidate_event_count",
            outcome.candidate_event_count,
            estimate.candidate_event_count,
        ),
        ("scratch_bytes", outcome.scratch_bytes, policy.max_scratch_bytes),
        ("output_bytes", outcome.output_bytes, policy.max_output_bytes),
    )
    for name, actual, limit in limits:
        if actual > limit:
            violations.append(f"{name} {actual} exceeds admitted limit {limit}")
    peak_rss_bytes = max(
        (
            value
            for ref in outcome.output_refs
            if type(value := ref.metadata.get("peak_rss_bytes")) is int
        ),
        default=0,
    )
    if peak_rss_bytes > policy.max_memory_bytes:
        violations.append(
            f"peak_rss_bytes {peak_rss_bytes} exceeds admitted limit "
            f"{policy.max_memory_bytes}"
        )
    if outcome.accepted_event_count > outcome.candidate_event_count:
        violations.append("accepted_event_count exceeds candidate_event_count")
    return tuple(violations)


def _validate_stage_outcome(
    outcome: ReconstructionStageOutcomeV1,
    window: ReconstructionWindowV1,
    command: ReconstructionStageCommandV1,
    prior_outcomes: Sequence[ReconstructionStageOutcomeV1],
) -> None:
    if (
        outcome.run_id != window.run_id
        or outcome.window_id != window.window_id
        or outcome.synchronization_unit_id != window.synchronization_unit_id
        or outcome.stage is not command.stage
        or outcome.command_id != command.command_id
    ):
        raise ReconstructionArtifactError(
            "stage outcome scope differs from invocation"
        )
    payload: dict[str, JSONValue] = {
        "run_id": window.run_id,
        "window_id": window.window_id,
        "command_id": command.command_id,
        "input_refs": [
            _artifact_identity(ref)
            for ref in (
                *command.input_manifest_refs,
                *command.configuration_refs,
                *tuple(
                    ref for prior in prior_outcomes for ref in prior.output_refs
                ),
            )
        ],
        "prior_outcome_ids": [item.outcome_id for item in prior_outcomes],
    }
    expected = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    if outcome.input_fingerprint != expected:
        raise ReconstructionArtifactError(
            "stage outcome input fingerprint differs from invocation"
        )


def _phase_artifact(
    outcome: ReconstructionStageOutcomeV1, phase: str
) -> ArtifactRef:
    matching = tuple(
        ref
        for ref in outcome.output_refs
        if str(ref.metadata.get("commit_phase", "")).strip().lower() == phase
    )
    if len(matching) != 1:
        raise ValueError(
            f"{outcome.stage.value} requires exactly one {phase} manifest ref"
        )
    return matching[0]


def _state_contains(
    current: ReconstructionWindowStateV1,
    proposed: ReconstructionWindowStateV1,
) -> bool:
    if (
        current.request_id != proposed.request_id
        or current.task.task_id != proposed.task.task_id
    ):
        return False
    proposed_ids = tuple(item.outcome_id for item in proposed.outcomes)
    current_ids = tuple(item.outcome_id for item in current.outcomes)
    if current_ids[: len(proposed_ids)] != proposed_ids:
        return False
    if current.state_id == proposed.state_id:
        return True
    if (
        current.checkpoint.parent_checkpoint_id
        == proposed.checkpoint.checkpoint_id
    ):
        return True
    return (
        len(current_ids) > len(proposed_ids)
        and current.checkpoint.revision > proposed.checkpoint.revision
    )


def _read_stage_receipt(path: Path) -> ReconstructionStageOutcomeV1:
    if path.stat().st_size > MAX_STAGE_RECEIPT_BYTES:
        raise ReconstructionArtifactError("stage receipt exceeds size limit")
    try:
        return ReconstructionStageOutcomeV1.from_json(
            path.read_text(encoding="utf-8")
        )
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as err:
        raise ReconstructionArtifactError(
            f"invalid stage receipt: {path}"
        ) from err


def _write_stage_receipt(
    path: Path, outcome: ReconstructionStageOutcomeV1
) -> None:
    payload = outcome.to_json().encode("utf-8")
    if path.exists():
        existing = _read_stage_receipt(path)
        if existing.outcome_id != outcome.outcome_id:
            raise ReconstructionArtifactError(
                "stage receipt path already contains different evidence"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write(path, payload)


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent
    )
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _reject_inline_data(value: Any, path: str = "request") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).strip().lower()
            if normalized in _FORBIDDEN_WORKFLOW_KEYS:
                raise ValueError(
                    f"workflow payload cannot contain {path}.{key}"
                )
            _reject_inline_data(item, f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(
        value, (str, bytes, bytearray)
    ):
        for index, item in enumerate(value):
            _reject_inline_data(item, f"{path}[{index}]")


def _unique_strong_refs(
    values: Sequence[ArtifactRef], name: str
) -> tuple[ArtifactRef, ...]:
    refs: dict[tuple[str, str, str, str], ArtifactRef] = {}
    for value in values:
        try:
            ref = _strong_ref(value)
        except ValueError as err:
            raise ValueError(f"invalid {name}: {err}") from err
        key = (
            ref.kind,
            ref.path,
            ref.sha256,
            canonical_contract_json(ref.metadata),
        )
        refs[key] = ref
    return tuple(refs[key] for key in sorted(refs))


def _strong_ref(value: ArtifactRef) -> ArtifactRef:
    if not isinstance(value, ArtifactRef):
        raise ValueError("artifact reference has the wrong type")
    kind = _required_text(value.kind)
    path = str(Path(_required_text(value.path)).expanduser().resolve())
    size = _nonnegative_int(value.size_bytes, "artifact size_bytes")
    digest = _required_sha256(value.sha256, "artifact sha256")
    return ArtifactRef(
        kind=kind,
        path=path,
        size_bytes=size,
        sha256=digest,
        metadata=dict(value.metadata),
    )


def _artifact_identity(ref: ArtifactRef) -> dict[str, JSONValue]:
    strong = _strong_ref(ref)
    return {
        "kind": strong.kind,
        "path": strong.path,
        "size_bytes": strong.size_bytes,
        "sha256": strong.sha256,
        "metadata": dict(strong.metadata),
    }


def _artifact_refs(value: Any) -> tuple[ArtifactRef, ...]:
    return tuple(
        ArtifactRef.from_dict(_mapping(item)) for item in _sequence(value)
    )


def _stable_id(prefix: str, payload: Mapping[str, JSONValue]) -> str:
    digest = hashlib.sha256(
        canonical_contract_json(payload).encode("utf-8")
    ).hexdigest()
    return f"{prefix}:sha256:{digest}"


def _required_text(value: Any) -> str:
    normalized = str(value).strip() if value is not None else ""
    if not normalized:
        raise ValueError("required text value is empty")
    return normalized


def _bounded_text(value: Any) -> str:
    normalized = str(value or "").strip()
    if len(normalized.encode("utf-8")) > MAX_STAGE_MESSAGE_LENGTH:
        raise ValueError("stage text exceeds bounded message limit")
    return normalized


def _bounded_reason_summary(values: Sequence[str]) -> str:
    reasons = tuple(
        str(value).strip() for value in values if str(value).strip()
    )
    full = "; ".join(reasons)
    if len(full.encode("utf-8")) <= MAX_STAGE_MESSAGE_LENGTH:
        return full
    digest = hashlib.sha256(full.encode("utf-8")).hexdigest()[:16]
    selected: list[str] = []
    for index, reason in enumerate(reasons):
        suffix = f"; +{len(reasons) - index - 1} more [sha256:{digest}]"
        candidate = "; ".join((*selected, reason)) + suffix
        if len(candidate.encode("utf-8")) > MAX_STAGE_MESSAGE_LENGTH:
            break
        selected.append(reason)
    suffix = f"; +{len(reasons) - len(selected)} more [sha256:{digest}]"
    return "; ".join(selected) + suffix


def _required_sha256(value: Any, name: str) -> str:
    normalized = str(value).strip().lower()
    if len(normalized) != 64 or any(
        char not in "0123456789abcdef" for char in normalized
    ):
        raise ValueError(f"{name} must be a lowercase sha256 digest")
    return normalized


def _nonnegative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer")
    return value


def _positive_int(value: Any, name: str) -> int:
    result = _nonnegative_int(value, name)
    if result == 0:
        raise ValueError(f"{name} must be positive")
    return result


def _ensure_payload_size(
    value: Mapping[str, JSONValue], maximum: int, name: str
) -> None:
    size = len(canonical_contract_json(value).encode("utf-8"))
    if size > maximum:
        raise ValueError(f"{name} exceeds {maximum} bytes")


def _file_sha256(path: Path) -> str:
    target = path.resolve()
    stat = target.stat()
    return _file_sha256_for_identity(
        str(target),
        stat.st_dev,
        stat.st_ino,
        stat.st_size,
        stat.st_mtime_ns,
        stat.st_ctime_ns,
    )


@lru_cache(maxsize=8192)
def _file_sha256_for_identity(
    path: str,
    device: int,
    inode: int,
    size: int,
    modified_ns: int,
    changed_ns: int,
) -> str:
    del device, inode, size, modified_ns, changed_ns
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _safe_filename(value: str) -> str:
    allowed = (
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_."
    )
    normalized = "".join(char if char in allowed else "_" for char in value)
    return normalized[:160] or "reconstruction-report"


def _validate_scratch_boundaries(
    tasks: Sequence[ReconstructionWindowTaskV1],
    *,
    durable_roots: Sequence[str],
) -> None:
    scratch_paths = tuple(
        Path(task.scratch_directory).expanduser().resolve() for task in tasks
    )
    for index, left in enumerate(scratch_paths):
        for right in scratch_paths[index + 1 :]:
            if _paths_overlap(left, right):
                raise ValueError(
                    "reconstruction window scratch directories must be disjoint"
                )
    durable_paths = tuple(
        Path(root).expanduser().resolve() for root in durable_roots
    )
    for scratch, task in zip(scratch_paths, tasks):
        if any(_paths_overlap(scratch, root) for root in durable_paths):
            raise ValueError(
                "window scratch must not overlap manifest or report storage"
            )
        for command in task.commands:
            for ref in (
                *command.input_manifest_refs,
                *command.configuration_refs,
            ):
                if (
                    Path(ref.path)
                    .expanduser()
                    .resolve()
                    .is_relative_to(scratch)
                ):
                    raise ValueError(
                        "durable stage inputs must remain outside window scratch"
                    )


def _paths_overlap(left: Path, right: Path) -> bool:
    return (
        left == right
        or left.is_relative_to(right)
        or right.is_relative_to(left)
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("expected a mapping")
    return value


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise ValueError("expected a sequence")
    return value


def _string_tuple(value: Any) -> tuple[str, ...]:
    return tuple(str(item) for item in _sequence(value))


def _json_mapping(text: str) -> Mapping[str, Any]:
    value = json.loads(text)
    return _mapping(value)
