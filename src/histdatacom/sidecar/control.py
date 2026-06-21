"""Transport-neutral sidecar job control and status contracts."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Any, Mapping

from histdatacom.runtime_contracts import (
    ArtifactRef,
    JSONValue,
    StatusEvent,
    WorkStatus,
)

CONTROL_SCHEMA_VERSION = 1


class JobControlAction(str, Enum):
    """Public actions supported by the local sidecar control API."""

    SUBMIT = "submit"
    LIST = "list"
    INSPECT = "inspect"
    PROGRESS = "progress"
    LOGS = "logs"
    CANCEL = "cancel"
    RETRY = "retry"
    RESUME = "resume"
    ARTIFACTS = "artifacts"
    RESULT = "result"


class JobLifecycle(str, Enum):
    """GUI-readable lifecycle state for a sidecar job."""

    SUBMITTED = "submitted"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCEL_REQUESTED = "cancel_requested"
    CANCELLED = "cancelled"
    RETRY_REQUESTED = "retry_requested"
    RETRYING = "retrying"
    RESUME_REQUESTED = "resume_requested"
    RESUMING = "resuming"
    UNKNOWN = "unknown"


class ControlOperationName(str, Enum):
    """Explicit control operations surfaced to CLI and GUI clients."""

    CANCEL = "cancel"
    RETRY = "retry"
    RESUME = "resume"


class ControlOperationPhase(str, Enum):
    """State of a control operation for a job."""

    AVAILABLE = "available"
    UNAVAILABLE = "unavailable"
    REQUESTED = "requested"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class ControlOperationState:
    """Serializable state for one job control operation."""

    name: ControlOperationName
    phase: ControlOperationPhase = ControlOperationPhase.AVAILABLE
    available: bool = True
    requested_at_utc: str = ""
    reason: str = ""
    message: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    @classmethod
    def unavailable(
        cls,
        name: ControlOperationName,
        *,
        message: str = "",
    ) -> "ControlOperationState":
        """Return an unavailable operation state."""
        return cls(
            name=name,
            phase=ControlOperationPhase.UNAVAILABLE,
            available=False,
            message=message,
        )

    def transition(
        self,
        phase: ControlOperationPhase,
        *,
        requested_at_utc: str = "",
        reason: str = "",
        message: str = "",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> "ControlOperationState":
        """Return this operation with an updated phase."""
        available = phase != ControlOperationPhase.UNAVAILABLE
        return replace(
            self,
            phase=phase,
            available=available,
            requested_at_utc=requested_at_utc or self.requested_at_utc,
            reason=reason or self.reason,
            message=message or self.message,
            metadata=dict(metadata or self.metadata),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible operation state."""
        return {
            "name": self.name.value,
            "phase": self.phase.value,
            "available": self.available,
            "requested_at_utc": self.requested_at_utc,
            "reason": self.reason,
            "message": self.message,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ControlOperationState":
        """Create an operation state from JSON-compatible data."""
        name = ControlOperationName(str(data.get("name", "cancel")))
        phase = ControlOperationPhase(str(data.get("phase", "available")))
        return cls(
            name=name,
            phase=phase,
            available=bool(data.get("available", True)),
            requested_at_utc=str(data.get("requested_at_utc", "") or ""),
            reason=str(data.get("reason", "") or ""),
            message=str(data.get("message", "") or ""),
            metadata=dict(data.get("metadata") or {}),
        )


def _cancel_available() -> ControlOperationState:
    return ControlOperationState(ControlOperationName.CANCEL)


def _retry_available() -> ControlOperationState:
    return ControlOperationState(ControlOperationName.RETRY)


def _resume_available() -> ControlOperationState:
    return ControlOperationState(ControlOperationName.RESUME)


@dataclass(frozen=True, slots=True)
class JobControlStates:
    """Control operation states for one job."""

    cancel: ControlOperationState = field(default_factory=_cancel_available)
    retry: ControlOperationState = field(default_factory=_retry_available)
    resume: ControlOperationState = field(default_factory=_resume_available)

    @classmethod
    def for_status(
        cls,
        status: WorkStatus,
        lifecycle: JobLifecycle,
    ) -> "JobControlStates":
        """Return sensible controls for the current job state."""
        terminal = status.terminal or lifecycle in {
            JobLifecycle.SUCCEEDED,
            JobLifecycle.CANCELLED,
        }
        retryable = status == WorkStatus.FAILED or lifecycle in {
            JobLifecycle.FAILED,
            JobLifecycle.CANCELLED,
        }
        resumable = lifecycle in {
            JobLifecycle.FAILED,
            JobLifecycle.CANCELLED,
            JobLifecycle.RETRY_REQUESTED,
        }
        return cls(
            cancel=(
                ControlOperationState.unavailable(
                    ControlOperationName.CANCEL,
                    message="Job is already terminal.",
                )
                if terminal
                else _cancel_available()
            ),
            retry=(
                _retry_available()
                if retryable
                else ControlOperationState.unavailable(
                    ControlOperationName.RETRY,
                    message="Retry is available after failure/cancellation.",
                )
            ),
            resume=(
                _resume_available()
                if resumable
                else ControlOperationState.unavailable(
                    ControlOperationName.RESUME,
                    message="Resume is available after interrupted work.",
                )
            ),
        )

    def with_operation(
        self,
        operation: ControlOperationName,
        state: ControlOperationState,
    ) -> "JobControlStates":
        """Return controls with one operation replaced."""
        if operation == ControlOperationName.CANCEL:
            return replace(self, cancel=state)
        if operation == ControlOperationName.RETRY:
            return replace(self, retry=state)
        return replace(self, resume=state)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible control states."""
        return {
            "cancel": self.cancel.to_dict(),
            "retry": self.retry.to_dict(),
            "resume": self.resume.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "JobControlStates":
        """Create control states from JSON-compatible data."""
        return cls(
            cancel=ControlOperationState.from_dict(
                _coerce_mapping(data.get("cancel", {}))
            ),
            retry=ControlOperationState.from_dict(
                _coerce_mapping(data.get("retry", {}))
            ),
            resume=ControlOperationState.from_dict(
                _coerce_mapping(data.get("resume", {}))
            ),
        )


@dataclass(frozen=True, slots=True)
class JobProgressSnapshot:
    """Progress state independent of any terminal UI renderer."""

    workflow_name: str = ""
    request_id: str = ""
    status: WorkStatus = WorkStatus.UNKNOWN
    current_stage: str = ""
    total_children: int = 0
    completed_children: int = 0
    unit: str = "children"
    started_at_utc: str = ""
    updated_at_utc: str = ""
    rate_per_second: float = 0.0
    last_error: str = ""
    planned_children: tuple[str, ...] = ()
    completed_stages: tuple[str, ...] = ()
    events: tuple[StatusEvent, ...] = ()
    artifacts: tuple[ArtifactRef, ...] = ()

    @property
    def percent_complete(self) -> float:
        """Return bounded percentage complete for status displays."""
        if self.total_children <= 0:
            return 0.0
        return min(
            100.0,
            max(0.0, (self.completed_children / self.total_children) * 100),
        )

    def to_dict(self) -> dict[str, JSONValue]:
        """Return JSON-compatible progress state."""
        return {
            "workflow_name": self.workflow_name,
            "request_id": self.request_id,
            "status": self.status.value,
            "current_stage": self.current_stage,
            "total_children": self.total_children,
            "completed_children": self.completed_children,
            "unit": self.unit,
            "percent_complete": self.percent_complete,
            "rate_per_second": self.rate_per_second,
            "started_at_utc": self.started_at_utc,
            "updated_at_utc": self.updated_at_utc,
            "last_error": self.last_error,
            "planned_children": list(self.planned_children),
            "completed_stages": list(self.completed_stages),
            "events": [event.to_dict() for event in self.events],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "JobProgressSnapshot":
        """Create progress state from a workflow status query payload."""
        events = tuple(
            StatusEvent.from_dict(_coerce_mapping(item))
            for item in data.get("events", [])
        )
        return cls(
            workflow_name=str(data.get("workflow_name", "") or ""),
            request_id=str(data.get("request_id", "") or ""),
            status=WorkStatus.from_value(data.get("status")),
            current_stage=str(data.get("current_stage", "") or ""),
            total_children=int(data.get("total_children", 0) or 0),
            completed_children=int(data.get("completed_children", 0) or 0),
            unit=str(data.get("unit", "children") or "children"),
            started_at_utc=str(data.get("started_at_utc", "") or ""),
            updated_at_utc=str(data.get("updated_at_utc", "") or ""),
            rate_per_second=_float_value(data.get("rate_per_second")),
            last_error=str(
                data.get("last_error", "") or _last_event_error(events)
            ),
            planned_children=tuple(
                str(item) for item in data.get("planned_children", [])
            ),
            completed_stages=tuple(
                str(item) for item in data.get("completed_stages", [])
            ),
            events=events,
            artifacts=tuple(
                ArtifactRef.from_dict(_coerce_mapping(item))
                for item in data.get("artifacts", [])
            ),
        )


@dataclass(frozen=True, slots=True)
class JobLogEntry:
    """Log entry surfaced by the local control API."""

    source: str
    message: str
    level: str = "info"
    timestamp_utc: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, JSONValue]:
        """Return a JSON-compatible log entry."""
        return {
            "source": self.source,
            "level": self.level,
            "message": self.message,
            "timestamp_utc": self.timestamp_utc,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "JobLogEntry":
        """Create a log entry from JSON-compatible data."""
        return cls(
            source=str(data.get("source", "") or ""),
            level=str(data.get("level", "info") or "info"),
            message=str(data.get("message", "") or ""),
            timestamp_utc=str(data.get("timestamp_utc", "") or ""),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True, slots=True)
class SidecarJobSnapshot:
    """GUI-ready status/control snapshot for one sidecar job."""

    job_id: str
    request_id: str
    workflow_id: str
    run_id: str = ""
    namespace: str = ""
    task_queue: str = ""
    lifecycle: JobLifecycle = JobLifecycle.UNKNOWN
    status: WorkStatus = WorkStatus.UNKNOWN
    progress: JobProgressSnapshot | None = None
    controls: JobControlStates = field(default_factory=JobControlStates)
    logs: tuple[JobLogEntry, ...] = ()
    artifacts: tuple[ArtifactRef, ...] = ()
    result: Any = None
    sidecar_state: str = ""
    sidecar_message: str = ""
    updated_at_utc: str = ""
    metadata: dict[str, JSONValue] = field(default_factory=dict)
    schema_version: int = CONTROL_SCHEMA_VERSION

    @classmethod
    def from_handle(
        cls,
        handle: Any,
        *,
        lifecycle: JobLifecycle = JobLifecycle.SUBMITTED,
        status: WorkStatus = WorkStatus.PLANNED,
        sidecar_status: Any = None,
    ) -> "SidecarJobSnapshot":
        """Create a snapshot from a submitted workflow handle."""
        request_id = str(getattr(handle, "request_id", "") or "")
        workflow_id = str(
            getattr(handle, "workflow_id", "")
            or getattr(handle, "id", "")
            or request_id
        )
        snapshot = cls(
            job_id=workflow_id or request_id,
            request_id=request_id,
            workflow_id=workflow_id,
            run_id=str(getattr(handle, "run_id", "") or ""),
            namespace=str(getattr(handle, "namespace", "") or ""),
            task_queue=str(getattr(handle, "task_queue", "") or ""),
            lifecycle=lifecycle,
            status=status,
            controls=JobControlStates.for_status(status, lifecycle),
        )
        return snapshot.with_sidecar_status(sidecar_status)

    @classmethod
    def from_workflow_status(
        cls,
        handle: Any,
        status_payload: Mapping[str, Any],
        *,
        sidecar_status: Any = None,
    ) -> "SidecarJobSnapshot":
        """Create a snapshot from a workflow status query payload."""
        progress = JobProgressSnapshot.from_dict(status_payload)
        lifecycle = lifecycle_from_work_status(progress.status)
        return (
            cls.from_handle(
                handle,
                lifecycle=lifecycle,
                status=progress.status,
                sidecar_status=sidecar_status,
            )
            .with_progress(progress)
            .with_sidecar_status(sidecar_status)
        )

    def with_sidecar_status(self, sidecar_status: Any) -> "SidecarJobSnapshot":
        """Return a copy enriched with sidecar process status, if present."""
        if sidecar_status is None:
            return self
        metadata = dict(self.metadata)
        logs = getattr(sidecar_status, "logs", None)
        if isinstance(logs, Mapping):
            metadata["sidecar_logs"] = {
                str(key): str(value) for key, value in logs.items()
            }
        return replace(
            self,
            sidecar_state=str(getattr(sidecar_status, "state", "") or ""),
            sidecar_message=str(getattr(sidecar_status, "message", "") or ""),
            metadata=metadata,
        )

    def with_progress(
        self,
        progress: JobProgressSnapshot,
    ) -> "SidecarJobSnapshot":
        """Return a copy with updated workflow progress."""
        lifecycle = lifecycle_from_work_status(progress.status)
        return replace(
            self,
            lifecycle=lifecycle,
            status=progress.status,
            progress=progress,
            artifacts=progress.artifacts,
            controls=JobControlStates.for_status(progress.status, lifecycle),
        )

    def with_result(self, result: Any) -> "SidecarJobSnapshot":
        """Return a copy with a completed workflow result payload."""
        progress = self.progress
        artifacts = self.artifacts
        status = self.status
        if isinstance(result, Mapping):
            status = WorkStatus.from_value(result.get("status"))
            if isinstance(result.get("progress"), Mapping):
                progress = JobProgressSnapshot.from_dict(
                    _coerce_mapping(result.get("progress"))
                )
                artifacts = progress.artifacts
            if isinstance(result.get("artifacts"), list):
                artifacts = tuple(
                    ArtifactRef.from_dict(_coerce_mapping(item))
                    for item in result.get("artifacts", [])
                )
        lifecycle = lifecycle_from_work_status(status)
        return replace(
            self,
            lifecycle=lifecycle,
            status=status,
            progress=progress,
            artifacts=artifacts,
            result=result,
            controls=JobControlStates.for_status(status, lifecycle),
        )

    def request_cancel(
        self,
        *,
        requested_at_utc: str = "",
        reason: str = "",
        message: str = "Cancellation requested.",
    ) -> "SidecarJobSnapshot":
        """Return a snapshot with cancellation explicitly requested."""
        state = self.controls.cancel.transition(
            ControlOperationPhase.REQUESTED,
            requested_at_utc=requested_at_utc,
            reason=reason,
            message=message,
        )
        return replace(
            self,
            lifecycle=JobLifecycle.CANCEL_REQUESTED,
            controls=self.controls.with_operation(
                ControlOperationName.CANCEL,
                state,
            ),
        )

    def mark_cancelled(
        self,
        *,
        message: str = "Job cancelled.",
    ) -> "SidecarJobSnapshot":
        """Return a terminal cancelled snapshot."""
        state = self.controls.cancel.transition(
            ControlOperationPhase.COMPLETED,
            message=message,
        )
        lifecycle = JobLifecycle.CANCELLED
        status = WorkStatus.CANCELLED
        return replace(
            self,
            lifecycle=lifecycle,
            status=status,
            controls=JobControlStates.for_status(
                status,
                lifecycle,
            ).with_operation(ControlOperationName.CANCEL, state),
        )

    def request_retry(
        self,
        *,
        requested_at_utc: str = "",
        reason: str = "",
        message: str = "Retry requested.",
    ) -> "SidecarJobSnapshot":
        """Return a snapshot with retry explicitly requested."""
        state = self.controls.retry.transition(
            ControlOperationPhase.REQUESTED,
            requested_at_utc=requested_at_utc,
            reason=reason,
            message=message,
        )
        return replace(
            self,
            lifecycle=JobLifecycle.RETRY_REQUESTED,
            controls=self.controls.with_operation(
                ControlOperationName.RETRY,
                state,
            ),
        )

    def mark_retrying(
        self,
        *,
        message: str = "Retry in progress.",
    ) -> "SidecarJobSnapshot":
        """Return a snapshot with retry in progress."""
        state = self.controls.retry.transition(
            ControlOperationPhase.IN_PROGRESS,
            message=message,
        )
        return replace(
            self,
            lifecycle=JobLifecycle.RETRYING,
            controls=self.controls.with_operation(
                ControlOperationName.RETRY,
                state,
            ),
        )

    def request_resume(
        self,
        *,
        requested_at_utc: str = "",
        reason: str = "",
        message: str = "Resume requested.",
    ) -> "SidecarJobSnapshot":
        """Return a snapshot with resume explicitly requested."""
        state = self.controls.resume.transition(
            ControlOperationPhase.REQUESTED,
            requested_at_utc=requested_at_utc,
            reason=reason,
            message=message,
        )
        return replace(
            self,
            lifecycle=JobLifecycle.RESUME_REQUESTED,
            controls=self.controls.with_operation(
                ControlOperationName.RESUME,
                state,
            ),
        )

    def mark_resuming(
        self,
        *,
        message: str = "Resume in progress.",
    ) -> "SidecarJobSnapshot":
        """Return a snapshot with resume in progress."""
        state = self.controls.resume.transition(
            ControlOperationPhase.IN_PROGRESS,
            message=message,
        )
        return replace(
            self,
            lifecycle=JobLifecycle.RESUMING,
            controls=self.controls.with_operation(
                ControlOperationName.RESUME,
                state,
            ),
        )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible job snapshot."""
        return {
            "schema_version": self.schema_version,
            "job_id": self.job_id,
            "request_id": self.request_id,
            "workflow_id": self.workflow_id,
            "run_id": self.run_id,
            "namespace": self.namespace,
            "task_queue": self.task_queue,
            "lifecycle": self.lifecycle.value,
            "status": self.status.value,
            "progress": (
                self.progress.to_dict() if self.progress is not None else None
            ),
            "controls": self.controls.to_dict(),
            "logs": [entry.to_dict() for entry in self.logs],
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "result": self.result,
            "sidecar_state": self.sidecar_state,
            "sidecar_message": self.sidecar_message,
            "updated_at_utc": self.updated_at_utc,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SidecarJobSnapshot":
        """Create a job snapshot from JSON-compatible data."""
        progress = data.get("progress")
        return cls(
            schema_version=int(data.get("schema_version", 1) or 1),
            job_id=str(data.get("job_id", "") or ""),
            request_id=str(data.get("request_id", "") or ""),
            workflow_id=str(data.get("workflow_id", "") or ""),
            run_id=str(data.get("run_id", "") or ""),
            namespace=str(data.get("namespace", "") or ""),
            task_queue=str(data.get("task_queue", "") or ""),
            lifecycle=JobLifecycle(str(data.get("lifecycle", "unknown"))),
            status=WorkStatus.from_value(data.get("status")),
            progress=(
                JobProgressSnapshot.from_dict(_coerce_mapping(progress))
                if isinstance(progress, Mapping)
                else None
            ),
            controls=JobControlStates.from_dict(
                _coerce_mapping(data.get("controls", {}))
            ),
            logs=tuple(
                JobLogEntry.from_dict(_coerce_mapping(item))
                for item in data.get("logs", [])
            ),
            artifacts=tuple(
                ArtifactRef.from_dict(_coerce_mapping(item))
                for item in data.get("artifacts", [])
            ),
            result=data.get("result"),
            sidecar_state=str(data.get("sidecar_state", "") or ""),
            sidecar_message=str(data.get("sidecar_message", "") or ""),
            updated_at_utc=str(data.get("updated_at_utc", "") or ""),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(frozen=True, slots=True)
class SidecarJobList:
    """Serializable collection returned by job list calls."""

    jobs: tuple[SidecarJobSnapshot, ...]
    schema_version: int = CONTROL_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-compatible list payload."""
        return {
            "schema_version": self.schema_version,
            "jobs": [job.to_dict() for job in self.jobs],
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SidecarJobList":
        """Create a job list payload from JSON-compatible data."""
        return cls(
            jobs=tuple(
                SidecarJobSnapshot.from_dict(_coerce_mapping(item))
                for item in data.get("jobs", [])
            ),
            schema_version=int(data.get("schema_version", 1) or 1),
        )


def lifecycle_from_work_status(status: WorkStatus) -> JobLifecycle:
    """Map sidecar work status to a public job lifecycle."""
    if status == WorkStatus.COMPLETED:
        return JobLifecycle.SUCCEEDED
    if status == WorkStatus.FAILED:
        return JobLifecycle.FAILED
    if status == WorkStatus.CANCELLED:
        return JobLifecycle.CANCELLED
    if status == WorkStatus.PLANNED:
        return JobLifecycle.SUBMITTED
    if status == WorkStatus.UNKNOWN:
        return JobLifecycle.RUNNING
    if status.terminal:
        return JobLifecycle.SUCCEEDED
    return JobLifecycle.RUNNING


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _float_value(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _last_event_error(events: tuple[StatusEvent, ...]) -> str:
    for event in reversed(events):
        value = event.metadata.get("last_error")
        if value:
            return str(value)
        if event.status == WorkStatus.FAILED and event.message:
            return str(event.message)
    return ""
