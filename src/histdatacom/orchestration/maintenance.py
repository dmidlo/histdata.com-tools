"""Maintenance and retention helpers for local orchestration runtime state."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from histdatacom.manifest_store import (
    MANIFEST_SCHEMA_VERSION,
    ManifestStatusStore,
)
from histdatacom.orchestration.runtime import OrchestrationRuntimePolicy

MAINTENANCE_SCHEMA_VERSION = 1
DEFAULT_MAX_LOG_BYTES = 10 * 1024 * 1024
DEFAULT_MAX_ROTATED_LOGS = 5
DEFAULT_MAX_TEMPORAL_SQLITE_BYTES = 512 * 1024 * 1024
DEFAULT_MAX_JOB_SNAPSHOTS = 100
DEFAULT_MAX_STATUS_EVENTS_PER_OWNER = 200
DEFAULT_MAX_STAGE_RESULTS_PER_WORK_ITEM = 25
DEFAULT_MAX_ARTIFACTS_PER_OWNER = 200
DEFAULT_MAX_DATASET_PLANS_PER_REQUEST = 50


@dataclass(frozen=True, slots=True)
class OrchestrationRetentionPolicy:
    """Bounded retention policy for workspace-scoped orchestration state."""

    max_log_bytes: int = DEFAULT_MAX_LOG_BYTES
    max_rotated_logs: int = DEFAULT_MAX_ROTATED_LOGS
    max_temporal_sqlite_bytes: int = DEFAULT_MAX_TEMPORAL_SQLITE_BYTES
    max_job_snapshots: int = DEFAULT_MAX_JOB_SNAPSHOTS
    max_status_events_per_owner: int = DEFAULT_MAX_STATUS_EVENTS_PER_OWNER
    max_stage_results_per_work_item: int = (
        DEFAULT_MAX_STAGE_RESULTS_PER_WORK_ITEM
    )
    max_artifacts_per_owner: int = DEFAULT_MAX_ARTIFACTS_PER_OWNER
    max_dataset_plans_per_request: int = DEFAULT_MAX_DATASET_PLANS_PER_REQUEST

    def __post_init__(self) -> None:
        """Validate retention limits early for CLI/API callers."""
        for name, value in self.to_dict().items():
            if int(value) < 0:
                raise ValueError(
                    f"{name} must be greater than or equal to zero."
                )

    def to_dict(self) -> dict[str, int]:
        """Return a JSON-compatible policy document."""
        return {
            "max_log_bytes": self.max_log_bytes,
            "max_rotated_logs": self.max_rotated_logs,
            "max_temporal_sqlite_bytes": self.max_temporal_sqlite_bytes,
            "max_job_snapshots": self.max_job_snapshots,
            "max_status_events_per_owner": self.max_status_events_per_owner,
            "max_stage_results_per_work_item": (
                self.max_stage_results_per_work_item
            ),
            "max_artifacts_per_owner": self.max_artifacts_per_owner,
            "max_dataset_plans_per_request": (
                self.max_dataset_plans_per_request
            ),
        }


@dataclass(frozen=True, slots=True)
class LogMaintenanceResult:
    """Result for one orchestration log maintenance operation."""

    path: str
    exists: bool
    action: str
    size_before_bytes: int = 0
    size_after_bytes: int = 0
    rotations_removed: int = 0
    reason: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible result."""
        return {
            "path": self.path,
            "exists": self.exists,
            "action": self.action,
            "size_before_bytes": self.size_before_bytes,
            "size_after_bytes": self.size_after_bytes,
            "rotations_removed": self.rotations_removed,
            "reason": self.reason,
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class StatusStoreMaintenanceResult:
    """Result for manifest/status store retention pruning."""

    path: str
    exists: bool
    action: str
    rows_deleted: dict[str, int] = field(default_factory=dict)
    reason: str = ""
    error: str = ""
    schema_version: int = 0
    expected_schema_version: int = MANIFEST_SCHEMA_VERSION
    schema_state: str = "missing"
    size_before_bytes: int = 0
    size_after_bytes: int = 0
    bytes_recovered: int = 0
    compacted: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible result."""
        return {
            "path": self.path,
            "exists": self.exists,
            "action": self.action,
            "rows_deleted": dict(self.rows_deleted),
            "reason": self.reason,
            "error": self.error,
            "schema_version": self.schema_version,
            "expected_schema_version": self.expected_schema_version,
            "schema_state": self.schema_state,
            "size_before_bytes": self.size_before_bytes,
            "size_after_bytes": self.size_after_bytes,
            "bytes_recovered": self.bytes_recovered,
            "compacted": self.compacted,
        }


@dataclass(frozen=True, slots=True)
class TemporalSqliteMaintenanceResult:
    """Result for Temporal SQLite history inspection."""

    path: str
    files: tuple[str, ...]
    exists: bool
    action: str
    size_bytes: int = 0
    max_bytes: int = DEFAULT_MAX_TEMPORAL_SQLITE_BYTES
    within_limit: bool = True
    reason: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible result."""
        return {
            "path": self.path,
            "files": list(self.files),
            "exists": self.exists,
            "action": self.action,
            "size_bytes": self.size_bytes,
            "max_bytes": self.max_bytes,
            "within_limit": self.within_limit,
            "reason": self.reason,
            "error": self.error,
        }


@dataclass(frozen=True, slots=True)
class OrchestrationMaintenanceResult:
    """JSON-ready result for an orchestration maintenance pass."""

    state: str
    message: str
    runtime_policy: OrchestrationRuntimePolicy
    retention_policy: OrchestrationRetentionPolicy
    orchestration_state: str
    logs: tuple[LogMaintenanceResult, ...]
    status_store: StatusStoreMaintenanceResult
    temporal_sqlite: TemporalSqliteMaintenanceResult
    warnings: tuple[str, ...] = ()
    downloaded_artifacts_removed: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible result."""
        return {
            "schema_version": MAINTENANCE_SCHEMA_VERSION,
            "state": self.state,
            "message": self.message,
            "workspace": str(self.runtime_policy.workspace),
            "runtime_home": str(self.runtime_policy.runtime_home),
            "orchestration_state": self.orchestration_state,
            "paths": self.runtime_policy.paths.to_dict(),
            "retention_policy": self.retention_policy.to_dict(),
            "logs": [result.to_dict() for result in self.logs],
            "status_store": self.status_store.to_dict(),
            "temporal_sqlite": self.temporal_sqlite.to_dict(),
            "warnings": list(self.warnings),
            "downloaded_artifacts_removed": self.downloaded_artifacts_removed,
            "data_directory_policy": (
                "Orchestration maintenance is workspace-scoped and does not remove "
                "downloaded HistData ZIP, CSV, XLSX, or cache artifacts by "
                "default."
            ),
        }


def run_orchestration_maintenance(
    runtime_policy: OrchestrationRuntimePolicy,
    retention_policy: OrchestrationRetentionPolicy | None = None,
    *,
    orchestration_state: str = "stopped",
    allow_running: bool = False,
) -> OrchestrationMaintenanceResult:
    """Run workspace-scoped orchestration maintenance."""
    policy = retention_policy or OrchestrationRetentionPolicy()
    temporal_sqlite = _temporal_sqlite_result(runtime_policy, policy)
    warnings = _temporal_sqlite_warnings(temporal_sqlite)
    skip_reason = ""
    if orchestration_state == "running" and not allow_running:
        skip_reason = (
            "Orchestration is running; stop it or pass --allow-running before "
            "maintenance mutates logs or status rows."
        )
        warnings = (*warnings, skip_reason)

    logs = _maintain_logs(
        runtime_policy.paths.logs_dir,
        policy,
        skip_reason=skip_reason,
    )
    status_store = _status_store_result(
        runtime_policy,
        policy,
        skip_reason=skip_reason,
    )
    if skip_reason:
        return OrchestrationMaintenanceResult(
            state="skipped",
            message=skip_reason,
            runtime_policy=runtime_policy,
            retention_policy=policy,
            orchestration_state=orchestration_state,
            logs=logs,
            status_store=status_store,
            temporal_sqlite=temporal_sqlite,
            warnings=warnings,
        )
    errors = tuple(
        error
        for error in (
            *(result.error for result in logs if result.error),
            status_store.error,
            temporal_sqlite.error,
        )
        if error
    )
    if errors:
        return OrchestrationMaintenanceResult(
            state="error",
            message="Orchestration maintenance completed with errors.",
            runtime_policy=runtime_policy,
            retention_policy=policy,
            orchestration_state=orchestration_state,
            logs=logs,
            status_store=status_store,
            temporal_sqlite=temporal_sqlite,
            warnings=(*warnings, *errors),
        )
    return OrchestrationMaintenanceResult(
        state="completed",
        message="Orchestration maintenance completed.",
        runtime_policy=runtime_policy,
        retention_policy=policy,
        orchestration_state=orchestration_state,
        logs=logs,
        status_store=status_store,
        temporal_sqlite=temporal_sqlite,
        warnings=warnings,
    )


def _maintain_logs(
    logs_dir: Path,
    policy: OrchestrationRetentionPolicy,
    *,
    skip_reason: str = "",
) -> tuple[LogMaintenanceResult, ...]:
    paths = _orchestration_log_paths(logs_dir)
    return tuple(
        _log_result(path, policy, skip_reason=skip_reason) for path in paths
    )


def _orchestration_log_paths(logs_dir: Path) -> tuple[Path, ...]:
    if not logs_dir.exists():
        return ()
    return tuple(
        sorted(path for path in logs_dir.glob("*.log") if path.exists())
    )


def _log_result(
    path: Path,
    policy: OrchestrationRetentionPolicy,
    *,
    skip_reason: str = "",
) -> LogMaintenanceResult:
    try:
        if not path.exists():
            return LogMaintenanceResult(
                path=str(path),
                exists=False,
                action="missing",
                reason="Log file does not exist.",
            )
        size_before = path.stat().st_size
        if skip_reason:
            return LogMaintenanceResult(
                path=str(path),
                exists=True,
                action="skipped",
                size_before_bytes=size_before,
                size_after_bytes=size_before,
                reason=skip_reason,
            )
        if size_before <= policy.max_log_bytes:
            return LogMaintenanceResult(
                path=str(path),
                exists=True,
                action="kept",
                size_before_bytes=size_before,
                size_after_bytes=size_before,
                reason="Log is within retention limit.",
            )
        removed = _rotate_log_path(path, policy.max_rotated_logs)
        size_after = path.stat().st_size if path.exists() else 0
        action = "rotated" if policy.max_rotated_logs else "truncated"
        return LogMaintenanceResult(
            path=str(path),
            exists=True,
            action=action,
            size_before_bytes=size_before,
            size_after_bytes=size_after,
            rotations_removed=removed,
            reason="Log exceeded retention limit.",
        )
    except OSError as err:
        return LogMaintenanceResult(
            path=str(path),
            exists=path.exists(),
            action="error",
            error=str(err),
        )


def _rotate_log_path(path: Path, max_rotated_logs: int) -> int:
    removed = 0
    if max_rotated_logs == 0:
        path.unlink(missing_ok=True)
        path.touch()
        return 1
    oldest = _rotated_log_path(path, max_rotated_logs)
    if oldest.exists():
        oldest.unlink()
        removed += 1
    for index in range(max_rotated_logs - 1, 0, -1):
        source = _rotated_log_path(path, index)
        if source.exists():
            source.replace(_rotated_log_path(path, index + 1))
    path.replace(_rotated_log_path(path, 1))
    path.touch()
    return removed


def _rotated_log_path(path: Path, index: int) -> Path:
    return path.with_name(f"{path.name}.{index}")


def _status_store_result(
    runtime_policy: OrchestrationRuntimePolicy,
    policy: OrchestrationRetentionPolicy,
    *,
    skip_reason: str = "",
) -> StatusStoreMaintenanceResult:
    store_root = runtime_policy.paths.manifests_dir
    store_path = ManifestStatusStore.path_for_root(store_root)
    size_before = _sqlite_file_size(store_path)
    schema = ManifestStatusStore.inspect_schema(store_root)
    schema_fields = _status_store_schema_fields(schema)
    if not store_path.exists():
        return StatusStoreMaintenanceResult(
            path=str(store_path),
            exists=False,
            action="missing",
            rows_deleted=_zero_row_counts(),
            reason="Manifest/status store does not exist.",
            size_before_bytes=size_before,
            size_after_bytes=size_before,
            **schema_fields,
        )
    if skip_reason:
        return StatusStoreMaintenanceResult(
            path=str(store_path),
            exists=True,
            action="skipped",
            rows_deleted=_zero_row_counts(),
            reason=skip_reason,
            size_before_bytes=size_before,
            size_after_bytes=size_before,
            **schema_fields,
        )
    if schema["state"] in {"unsupported", "error"}:
        return StatusStoreMaintenanceResult(
            path=str(store_path),
            exists=True,
            action="error",
            rows_deleted=_zero_row_counts(),
            error=str(schema.get("error", "")),
            size_before_bytes=size_before,
            size_after_bytes=size_before,
            **schema_fields,
        )
    try:
        store = ManifestStatusStore(store_root)
        rows_deleted = store.prune_retention(
            max_job_snapshots=policy.max_job_snapshots,
            max_status_events_per_owner=policy.max_status_events_per_owner,
            max_stage_results_per_work_item=(
                policy.max_stage_results_per_work_item
            ),
            max_artifacts_per_owner=policy.max_artifacts_per_owner,
            max_dataset_plans_per_request=(
                policy.max_dataset_plans_per_request
            ),
        )
        compacted = False
        if sum(rows_deleted.values()) > 0:
            _compact_sqlite_store(store.db_path)
            compacted = True
        schema_fields = _status_store_schema_fields(store.schema_status())
        size_after = _sqlite_file_size(store_path)
    except (OSError, sqlite3.DatabaseError, ValueError) as err:
        return StatusStoreMaintenanceResult(
            path=str(store_path),
            exists=True,
            action="error",
            rows_deleted=_zero_row_counts(),
            error=str(err),
            size_before_bytes=size_before,
            size_after_bytes=_sqlite_file_size(store_path),
            **schema_fields,
        )
    return StatusStoreMaintenanceResult(
        path=str(store_path),
        exists=True,
        action="pruned",
        rows_deleted=rows_deleted,
        reason="Manifest/status rows were pruned to retention limits.",
        size_before_bytes=size_before,
        size_after_bytes=size_after,
        bytes_recovered=max(0, size_before - size_after),
        compacted=compacted,
        **schema_fields,
    )


def _zero_row_counts() -> dict[str, int]:
    return {
        "jobs": 0,
        "status_events": 0,
        "stage_results": 0,
        "artifacts": 0,
        "dataset_plans": 0,
    }


def _status_store_schema_fields(schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": int(schema.get("schema_version", 0) or 0),
        "expected_schema_version": int(
            schema.get("expected_schema_version", MANIFEST_SCHEMA_VERSION)
            or MANIFEST_SCHEMA_VERSION
        ),
        "schema_state": str(schema.get("state", "") or "missing"),
    }


def _sqlite_file_size(path: Path) -> int:
    """Return bytes held by a SQLite database and its sidecar files."""
    paths = (
        path,
        path.with_name(f"{path.name}-wal"),
        path.with_name(f"{path.name}-shm"),
    )
    total = 0
    for candidate in paths:
        try:
            total += candidate.stat().st_size
        except FileNotFoundError:
            continue
    return total


def _compact_sqlite_store(path: Path) -> None:
    """Checkpoint and vacuum a stopped local SQLite store after row pruning."""
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        conn.execute("VACUUM")
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")


def _temporal_sqlite_result(
    runtime_policy: OrchestrationRuntimePolicy,
    policy: OrchestrationRetentionPolicy,
) -> TemporalSqliteMaintenanceResult:
    paths = _temporal_sqlite_paths(runtime_policy.paths.sqlite_db)
    try:
        files = tuple(str(path) for path in paths if path.exists())
        size_bytes = sum(path.stat().st_size for path in paths if path.exists())
    except OSError as err:
        return TemporalSqliteMaintenanceResult(
            path=str(runtime_policy.paths.sqlite_db),
            files=(),
            exists=False,
            action="error",
            max_bytes=policy.max_temporal_sqlite_bytes,
            within_limit=False,
            error=str(err),
        )
    exists = bool(files)
    within_limit = size_bytes <= policy.max_temporal_sqlite_bytes
    return TemporalSqliteMaintenanceResult(
        path=str(runtime_policy.paths.sqlite_db),
        files=files,
        exists=exists,
        action="preserved",
        size_bytes=size_bytes,
        max_bytes=policy.max_temporal_sqlite_bytes,
        within_limit=within_limit,
        reason=(
            "Temporal SQLite history is measured and preserved by default; "
            "reset it only through an explicit operator recovery step."
        ),
    )


def _temporal_sqlite_paths(sqlite_db: Path) -> tuple[Path, ...]:
    return (
        sqlite_db,
        sqlite_db.with_name(f"{sqlite_db.name}-wal"),
        sqlite_db.with_name(f"{sqlite_db.name}-shm"),
    )


def _temporal_sqlite_warnings(
    result: TemporalSqliteMaintenanceResult,
) -> tuple[str, ...]:
    if not result.exists or result.within_limit:
        return ()
    return (
        (
            "Temporal SQLite history exceeds the configured size limit and was "
            "preserved by default."
        ),
    )
