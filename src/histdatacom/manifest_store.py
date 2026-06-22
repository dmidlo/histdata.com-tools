"""SQLite-backed manifest and status storage."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from histdatacom.runtime_contracts import (
    ArtifactRef,
    JSONValue,
    StageResult,
    StatusEvent,
    WorkItem,
    WorkStatus,
    derive_work_id,
)

MANIFEST_DIRECTORY = ".histdatacom"
MANIFEST_DB_FILENAME = "manifest-status.sqlite3"
MANIFEST_SCHEMA_VERSION = 1
DATASET_PLAN_REF_KEY = "dataset_plan_ref"
DATASET_PLAN_BATCHES_KEY = "dataset_plan_batches"
STATUS_STORE_REF_KEY = "sidecar_status_store"
STATUS_STORE_REF_KIND = "manifest_status_store"
DEFAULT_DATASET_PLAN_INLINE_WORK_ITEM_LIMIT = 64
PLAN_SPILL_METADATA_KEY = "temporal_plan_spill"
INLINE_WORK_ITEM_LIMIT_METADATA_KEY = "inline_work_item_limit"
LIVE_SNAPSHOT_EVENT_LIMIT = 200
LIVE_SNAPSHOT_ARTIFACT_LIMIT = 200


@dataclass(frozen=True, slots=True)
class ManifestMigrationResult:
    """Result from importing a legacy metadata artifact."""

    path: str
    migrated: bool
    reason: str = ""
    work_id: str = ""
    error: str = ""


class ManifestStatusStore:
    """Manifest/status store for records, artifacts, and sidecar jobs."""

    def __init__(self, root_dir: str | Path):
        """Create a store rooted at a data directory."""
        self.root_dir = Path(root_dir).expanduser()
        self.db_path = self.path_for_root(self.root_dir)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @staticmethod
    def path_for_root(root_dir: str | Path) -> Path:
        """Return the SQLite manifest path for a data root."""
        return (
            Path(root_dir).expanduser()
            / MANIFEST_DIRECTORY
            / MANIFEST_DB_FILENAME
        )

    @classmethod
    def inspect_schema(cls, root_dir: str | Path) -> dict[str, Any]:
        """Return manifest/status SQLite schema diagnostics without mutation."""
        db_path = cls.path_for_root(root_dir)
        if not db_path.exists():
            return _manifest_schema_status(db_path, exists=False)
        try:
            with sqlite3.connect(
                f"file:{db_path}?mode=ro",
                uri=True,
            ) as conn:
                user_version = _read_user_version(conn)
        except sqlite3.DatabaseError as err:
            return _manifest_schema_status(
                db_path,
                exists=True,
                state="error",
                error=str(err),
            )
        return _manifest_schema_status(
            db_path,
            exists=True,
            user_version=user_version,
        )

    @classmethod
    def existing_for_record(
        cls,
        record: Any,
        *,
        base_dir: str | Path = "",
    ) -> "ManifestStatusStore | None":
        """Return an existing store that can contain the record."""
        roots = list(_candidate_roots(record, base_dir=base_dir))
        for root in roots:
            db_path = cls.path_for_root(root)
            if db_path.exists():
                return cls(root)
        return None

    def write_record(
        self,
        record: Any,
        *,
        source: str = "record_manifest_status",
        message: str = "Record manifest/status metadata stored.",
    ) -> WorkItem:
        """Upsert a legacy Record into manifest storage."""
        item = WorkItem.from_record(record)
        self.write_work_item(item, source=source, message=message)
        return item

    def write_work_item(
        self,
        work_item: WorkItem,
        *,
        source: str = "work_item",
        message: str = "Work item status stored.",
    ) -> None:
        """Upsert a work item and append a status event."""
        payload = work_item.to_dict()
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO work_items (
                    work_id,
                    status,
                    status_text,
                    url,
                    data_dir,
                    data_dir_key,
                    data_format,
                    data_timeframe,
                    data_fxpair,
                    data_datemonth,
                    cache_start,
                    cache_end,
                    cache_line_count,
                    payload_json,
                    updated_at_utc,
                    schema_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(work_id) DO UPDATE SET
                    status=excluded.status,
                    status_text=excluded.status_text,
                    url=excluded.url,
                    data_dir=excluded.data_dir,
                    data_dir_key=excluded.data_dir_key,
                    data_format=excluded.data_format,
                    data_timeframe=excluded.data_timeframe,
                    data_fxpair=excluded.data_fxpair,
                    data_datemonth=excluded.data_datemonth,
                    cache_start=excluded.cache_start,
                    cache_end=excluded.cache_end,
                    cache_line_count=excluded.cache_line_count,
                    payload_json=excluded.payload_json,
                    updated_at_utc=excluded.updated_at_utc,
                    schema_version=excluded.schema_version
                """,
                (
                    work_item.work_id,
                    work_item.status.value,
                    work_item.status_text,
                    work_item.url,
                    work_item.data_dir,
                    _path_key(work_item.data_dir),
                    work_item.data_format,
                    work_item.data_timeframe,
                    work_item.data_fxpair,
                    work_item.data_datemonth,
                    work_item.cache_start,
                    work_item.cache_end,
                    work_item.cache_line_count,
                    _json_dumps(payload),
                    now,
                    MANIFEST_SCHEMA_VERSION,
                ),
            )
            self._insert_status_event(
                conn,
                owner_kind="work_item",
                owner_id=work_item.work_id,
                event=StatusEvent(
                    status=work_item.status,
                    stage=source,
                    message=message,
                    work_id=work_item.work_id,
                    timestamp_utc=now,
                ),
            )

    def write_dataset_plan(
        self,
        *,
        plan_id: str,
        request_id: str,
        work_items: tuple[WorkItem, ...],
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> dict[str, JSONValue]:
        """Persist a dataset plan and return a compact workflow reference."""
        work_ids = tuple(item.work_id for item in work_items)
        payload: dict[str, JSONValue] = {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "plan_id": plan_id,
            "request_id": request_id,
            "work_item_count": len(work_items),
            "work_ids": list(work_ids),
            "metadata": dict(metadata or {}),
        }
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO dataset_plans (
                    plan_id,
                    request_id,
                    work_item_count,
                    work_ids_json,
                    payload_json,
                    updated_at_utc,
                    schema_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(plan_id) DO UPDATE SET
                    request_id=excluded.request_id,
                    work_item_count=excluded.work_item_count,
                    work_ids_json=excluded.work_ids_json,
                    payload_json=excluded.payload_json,
                    updated_at_utc=excluded.updated_at_utc,
                    schema_version=excluded.schema_version
                """,
                (
                    plan_id,
                    request_id,
                    len(work_items),
                    _json_dumps(list(work_ids)),
                    _json_dumps(payload),
                    now,
                    MANIFEST_SCHEMA_VERSION,
                ),
            )
        for item in work_items:
            self.write_work_item(
                item,
                source="dataset_plan",
                message="Dataset plan work item stored.",
            )
        return self.dataset_plan_ref(plan_id, work_item_count=len(work_items))

    def dataset_plan_ref(
        self,
        plan_id: str,
        *,
        work_item_count: int = 0,
    ) -> dict[str, JSONValue]:
        """Return a compact JSON-safe reference to a stored dataset plan."""
        ref: dict[str, JSONValue] = {
            "kind": "dataset_plan",
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "plan_id": plan_id,
            "store_root": str(self.root_dir),
            "store_path": str(self.db_path),
        }
        if work_item_count:
            ref["work_item_count"] = work_item_count
        return ref

    def status_store_ref(self) -> dict[str, JSONValue]:
        """Return a compact JSON-safe reference to this status store."""
        return {
            "kind": STATUS_STORE_REF_KIND,
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "store_root": str(self.root_dir),
            "store_path": str(self.db_path),
        }

    def get_dataset_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Return one stored dataset plan payload."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM dataset_plans WHERE plan_id = ?",
                (plan_id,),
            ).fetchone()
        if row is None:
            return None
        return dict(json.loads(str(row["payload_json"])))

    def get_dataset_plan_work_items(
        self,
        plan_id: str,
        *,
        work_ids: tuple[str, ...] = (),
    ) -> tuple[WorkItem, ...]:
        """Load ordered work items from a stored dataset plan."""
        plan_work_ids = self._dataset_plan_work_ids(plan_id)
        if not plan_work_ids:
            return ()
        if work_ids:
            plan_work_id_set = set(plan_work_ids)
            selected_work_ids = tuple(
                work_id for work_id in work_ids if work_id in plan_work_id_set
            )
        else:
            selected_work_ids = plan_work_ids
        items: list[WorkItem] = []
        for work_id in selected_work_ids:
            item = self.get_work_item(work_id)
            if item is not None:
                items.append(item)
        return tuple(items)

    def get_work_item(self, work_id: str) -> WorkItem | None:
        """Return a work item by stable id."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM work_items WHERE work_id = ?",
                (work_id,),
            ).fetchone()
        if row is None:
            return None
        return WorkItem.from_dict(json.loads(str(row["payload_json"])))

    def get_work_item_for_record(self, record: Any) -> WorkItem | None:
        """Return a stored work item matching a legacy Record."""
        work_id = WorkItem.from_record(record).work_id
        item = self.get_work_item(work_id)
        if item is not None:
            return item

        data_dir_key = _path_key(getattr(record, "data_dir", ""))
        if not data_dir_key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT payload_json
                FROM work_items
                WHERE data_dir_key = ?
                ORDER BY updated_at_utc DESC
                LIMIT 1
                """,
                (data_dir_key,),
            ).fetchone()
        if row is None:
            return None
        return WorkItem.from_dict(json.loads(str(row["payload_json"])))

    def list_work_items(
        self,
        *,
        status: WorkStatus | str | None = None,
        limit: int | None = None,
    ) -> tuple[WorkItem, ...]:
        """List work items without walking artifact directories."""
        where = ""
        params: list[Any] = []
        if status is not None:
            where = "WHERE status = ?"
            params.append(WorkStatus.from_value(status).value)
        sql = f"""
            SELECT payload_json
            FROM work_items
            {where}
            ORDER BY updated_at_utc DESC, work_id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return tuple(
            WorkItem.from_dict(json.loads(str(row["payload_json"])))
            for row in rows
        )

    def _dataset_plan_work_ids(self, plan_id: str) -> tuple[str, ...]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT work_ids_json FROM dataset_plans WHERE plan_id = ?",
                (plan_id,),
            ).fetchone()
        if row is None:
            return ()
        raw_work_ids = json.loads(str(row["work_ids_json"]))
        if not isinstance(raw_work_ids, list):
            return ()
        return tuple(str(work_id) for work_id in raw_work_ids)

    def delete_record(self, record: Any) -> None:
        """Remove current work-item state for a legacy Record."""
        work_id = WorkItem.from_record(record).work_id
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM work_items WHERE work_id = ?",
                (work_id,),
            )
            data_dir_key = _path_key(getattr(record, "data_dir", ""))
            if data_dir_key:
                conn.execute(
                    "DELETE FROM work_items WHERE data_dir_key = ?",
                    (data_dir_key,),
                )

    def import_meta_file(
        self,
        meta_path: str | Path,
        *,
        record: Any | None = None,
    ) -> ManifestMigrationResult:
        """Import one legacy `.meta` file into manifest storage."""
        path = Path(meta_path)
        if not path.exists():
            return ManifestMigrationResult(
                path=str(path),
                migrated=False,
                reason="missing",
            )

        try:
            payload = json.loads(path.read_text(encoding="UTF-8"))
        except (OSError, json.JSONDecodeError) as err:
            return ManifestMigrationResult(
                path=str(path),
                migrated=False,
                reason="corrupt",
                error=str(err),
            )
        if not isinstance(payload, Mapping):
            return ManifestMigrationResult(
                path=str(path),
                migrated=False,
                reason="invalid",
                error="legacy metadata root is not an object",
            )

        values = dict(payload)
        if record is not None:
            record_dir = str(getattr(record, "data_dir", "") or "")
            if record_dir:
                values["data_dir"] = record_dir
            record_url = str(getattr(record, "url", "") or "")
            if record_url and not values.get("url"):
                values["url"] = record_url
        values["work_id"] = values.get("work_id") or _work_id_from_mapping(
            values,
        )
        item = WorkItem.from_dict(values)
        self.write_work_item(
            item,
            source="legacy_meta_import",
            message="Legacy .meta metadata imported.",
        )
        cleanup_error = ""
        try:
            path.unlink()
        except OSError as err:
            cleanup_error = str(err)
        return ManifestMigrationResult(
            path=str(path),
            migrated=True,
            reason=(
                "imported" if not cleanup_error else "imported_cleanup_failed"
            ),
            work_id=item.work_id,
            error=cleanup_error,
        )

    def import_legacy_meta_files(
        self,
        root_dir: str | Path | None = None,
    ) -> tuple[ManifestMigrationResult, ...]:
        """Import all legacy `.meta` files below a data root."""
        root = (
            Path(root_dir).expanduser()
            if root_dir is not None
            else self.root_dir
        )
        return tuple(
            self.import_meta_file(path) for path in sorted(root.rglob(".meta"))
        )

    def schema_status(self) -> dict[str, Any]:
        """Return manifest/status SQLite schema diagnostics."""
        return self.inspect_schema(self.root_dir)

    def write_stage_result(self, result: StageResult) -> None:
        """Persist activity stage output metadata outside workflow history."""
        payload = result.to_dict()
        now = _utc_now()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO stage_results (
                    work_id,
                    stage,
                    status,
                    failure_json,
                    metrics_json,
                    payload_json,
                    created_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.work_id,
                    result.stage,
                    result.status.value,
                    _json_dumps(payload.get("failure")),
                    _json_dumps(payload.get("metrics", {})),
                    _json_dumps(payload),
                    now,
                ),
            )
            for event in result.events:
                self._insert_status_event(
                    conn,
                    owner_kind="work_item",
                    owner_id=result.work_id,
                    event=event,
                )
            for artifact in result.artifacts:
                self._insert_artifact(
                    conn,
                    owner_kind="work_item",
                    owner_id=result.work_id,
                    work_id=result.work_id,
                    artifact=artifact,
                    created_at_utc=now,
                )

    def write_live_stage_update(
        self,
        *,
        request_id: str,
        job_id: str,
        result: StageResult,
        work_item: WorkItem | None = None,
        workflow_id: str = "",
        task_queue: str = "",
        namespace: str = "",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> dict[str, Any]:
        """Persist one activity result and merge it into a live job snapshot."""
        if work_item is not None:
            self.write_work_item(
                work_item,
                source=result.stage,
                message=f"{result.stage} work item status stored.",
            )
        self.write_stage_result(result)

        stored = self.get_job_snapshot(job_id) or {}
        snapshot = _live_snapshot_payload(
            stored,
            request_id=request_id,
            job_id=job_id,
            workflow_id=workflow_id or job_id,
            task_queue=task_queue,
            namespace=namespace,
            result=result,
            work_item=work_item,
            metadata=metadata,
        )
        self.write_job_snapshot(snapshot)
        return snapshot

    def write_job_snapshot(self, snapshot: Any) -> None:
        """Persist a GUI-ready sidecar job snapshot."""
        payload = _mapping_payload(snapshot)
        job_id = str(
            payload.get("job_id", "") or payload.get("workflow_id", "")
        )
        if not job_id:
            raise ValueError("Job snapshot requires job_id or workflow_id.")
        progress = payload.get("progress")
        progress_payload = progress if isinstance(progress, Mapping) else {}
        now = _utc_now()
        payload["updated_at_utc"] = str(
            payload.get("updated_at_utc", "") or now
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO jobs (
                    job_id,
                    request_id,
                    workflow_id,
                    run_id,
                    lifecycle,
                    status,
                    task_queue,
                    payload_json,
                    updated_at_utc,
                    schema_version
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(job_id) DO UPDATE SET
                    request_id=excluded.request_id,
                    workflow_id=excluded.workflow_id,
                    run_id=excluded.run_id,
                    lifecycle=excluded.lifecycle,
                    status=excluded.status,
                    task_queue=excluded.task_queue,
                    payload_json=excluded.payload_json,
                    updated_at_utc=excluded.updated_at_utc,
                    schema_version=excluded.schema_version
                """,
                (
                    job_id,
                    str(payload.get("request_id", "") or ""),
                    str(payload.get("workflow_id", "") or job_id),
                    str(payload.get("run_id", "") or ""),
                    str(payload.get("lifecycle", "") or ""),
                    str(payload.get("status", "") or ""),
                    str(payload.get("task_queue", "") or ""),
                    _json_dumps(payload),
                    now,
                    int(payload.get("schema_version", 1) or 1),
                ),
            )
            self._insert_status_event(
                conn,
                owner_kind="job",
                owner_id=job_id,
                event=StatusEvent(
                    status=WorkStatus.from_value(payload.get("status")),
                    stage=str(
                        progress_payload.get("current_stage", "")
                        or "job_snapshot"
                    ),
                    message="Sidecar job snapshot stored.",
                    work_id=job_id,
                    timestamp_utc=now,
                ),
            )
            for event_payload in progress_payload.get("events", []):
                self._insert_status_event(
                    conn,
                    owner_kind="job",
                    owner_id=job_id,
                    event=StatusEvent.from_dict(_coerce_mapping(event_payload)),
                )
            artifact_payloads = [
                *_coerce_list(payload.get("artifacts")),
                *_coerce_list(progress_payload.get("artifacts")),
            ]
            seen_artifacts: set[tuple[str, str]] = set()
            for artifact_payload in artifact_payloads:
                artifact = ArtifactRef.from_dict(
                    _coerce_mapping(artifact_payload)
                )
                key = (artifact.kind, artifact.path)
                if key in seen_artifacts:
                    continue
                seen_artifacts.add(key)
                self._insert_artifact(
                    conn,
                    owner_kind="job",
                    owner_id=job_id,
                    work_id="",
                    artifact=artifact,
                    created_at_utc=now,
                )

    def get_job_snapshot(self, job_id: str) -> dict[str, Any] | None:
        """Return one stored sidecar job snapshot payload."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM jobs WHERE job_id = ?",
                (job_id,),
            ).fetchone()
        if row is None:
            return None
        return dict(json.loads(str(row["payload_json"])))

    def list_job_snapshots(
        self,
        *,
        status: WorkStatus | str | None = None,
        limit: int | None = None,
    ) -> tuple[dict[str, Any], ...]:
        """List stored sidecar job snapshots without querying Temporal."""
        where = ""
        params: list[Any] = []
        if status is not None:
            where = "WHERE status = ?"
            params.append(WorkStatus.from_value(status).value)
        sql = f"""
            SELECT payload_json
            FROM jobs
            {where}
            ORDER BY updated_at_utc DESC, job_id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return tuple(dict(json.loads(str(row["payload_json"]))) for row in rows)

    def status_history(
        self,
        owner_id: str,
        *,
        owner_kind: str = "",
    ) -> tuple[dict[str, Any], ...]:
        """Return status history for a job or work item."""
        where = "WHERE owner_id = ?"
        params: list[Any] = [owner_id]
        if owner_kind:
            where += " AND owner_kind = ?"
            params.append(owner_kind)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT payload_json
                FROM status_events
                {where}
                ORDER BY id ASC
                """,
                params,
            ).fetchall()
        return tuple(dict(json.loads(str(row["payload_json"]))) for row in rows)

    def list_stage_results(
        self,
        work_id: str,
    ) -> tuple[dict[str, Any], ...]:
        """Return stored stage results for one work item."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT payload_json
                FROM stage_results
                WHERE work_id = ?
                ORDER BY id ASC
                """,
                (work_id,),
            ).fetchall()
        return tuple(dict(json.loads(str(row["payload_json"]))) for row in rows)

    def list_artifacts(
        self,
        owner_id: str,
        *,
        owner_kind: str = "work_item",
    ) -> tuple[dict[str, JSONValue], ...]:
        """Return stored artifact references for a job or work item."""
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT kind, path, size_bytes, sha256, metadata_json
                FROM artifacts
                WHERE owner_kind = ? AND owner_id = ?
                ORDER BY id ASC
                """,
                (owner_kind, owner_id),
            ).fetchall()
        return tuple(
            {
                "kind": str(row["kind"]),
                "path": str(row["path"]),
                "size_bytes": row["size_bytes"],
                "sha256": str(row["sha256"]),
                "metadata": dict(json.loads(str(row["metadata_json"]))),
            }
            for row in rows
        )

    def prune_retention(
        self,
        *,
        max_job_snapshots: int,
        max_status_events_per_owner: int,
        max_stage_results_per_work_item: int,
        max_artifacts_per_owner: int,
        max_dataset_plans_per_request: int,
    ) -> dict[str, int]:
        """Prune append-only sidecar rows while preserving current work items."""
        _validate_retention_limit("max_job_snapshots", max_job_snapshots)
        _validate_retention_limit(
            "max_status_events_per_owner",
            max_status_events_per_owner,
        )
        _validate_retention_limit(
            "max_stage_results_per_work_item",
            max_stage_results_per_work_item,
        )
        _validate_retention_limit(
            "max_artifacts_per_owner",
            max_artifacts_per_owner,
        )
        _validate_retention_limit(
            "max_dataset_plans_per_request",
            max_dataset_plans_per_request,
        )
        deleted = {
            "jobs": 0,
            "status_events": 0,
            "stage_results": 0,
            "artifacts": 0,
            "dataset_plans": 0,
        }
        with self._connect() as conn:
            deleted_job_ids = _older_key_values(
                conn,
                table="jobs",
                key_column="job_id",
                keep=max_job_snapshots,
                order_by="updated_at_utc DESC, job_id ASC",
            )
            deleted["jobs"] += _delete_text_values(
                conn,
                table="jobs",
                column="job_id",
                values=deleted_job_ids,
            )
            if deleted_job_ids:
                deleted["status_events"] += _delete_owner_rows(
                    conn,
                    table="status_events",
                    owner_kind="job",
                    owner_ids=deleted_job_ids,
                )
                deleted["artifacts"] += _delete_owner_rows(
                    conn,
                    table="artifacts",
                    owner_kind="job",
                    owner_ids=deleted_job_ids,
                )

            deleted["status_events"] += _delete_group_overflow_rows(
                conn,
                table="status_events",
                id_column="id",
                group_columns=("owner_kind", "owner_id"),
                keep=max_status_events_per_owner,
            )
            deleted["stage_results"] += _delete_group_overflow_rows(
                conn,
                table="stage_results",
                id_column="id",
                group_columns=("work_id",),
                keep=max_stage_results_per_work_item,
            )
            deleted["artifacts"] += _delete_group_overflow_rows(
                conn,
                table="artifacts",
                id_column="id",
                group_columns=("owner_kind", "owner_id"),
                keep=max_artifacts_per_owner,
            )
            deleted["dataset_plans"] += _delete_group_overflow_rows(
                conn,
                table="dataset_plans",
                id_column="plan_id",
                group_columns=("request_id",),
                keep=max_dataset_plans_per_request,
                order_by="updated_at_utc DESC, plan_id ASC",
            )
        return deleted

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            user_version = _read_user_version(conn)
            if user_version > MANIFEST_SCHEMA_VERSION:
                raise ValueError(
                    "Unsupported manifest/status schema version "
                    f"{user_version}; expected <= {MANIFEST_SCHEMA_VERSION}. "
                    "Upgrade histdatacom before opening this store."
                )
            conn.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS work_items (
                    work_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    status_text TEXT NOT NULL DEFAULT '',
                    url TEXT NOT NULL DEFAULT '',
                    data_dir TEXT NOT NULL DEFAULT '',
                    data_dir_key TEXT NOT NULL DEFAULT '',
                    data_format TEXT NOT NULL DEFAULT '',
                    data_timeframe TEXT NOT NULL DEFAULT '',
                    data_fxpair TEXT NOT NULL DEFAULT '',
                    data_datemonth TEXT NOT NULL DEFAULT '',
                    cache_start TEXT NOT NULL DEFAULT '',
                    cache_end TEXT NOT NULL DEFAULT '',
                    cache_line_count TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_work_items_status
                    ON work_items(status);
                CREATE INDEX IF NOT EXISTS idx_work_items_data_dir_key
                    ON work_items(data_dir_key);
                CREATE TABLE IF NOT EXISTS status_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_kind TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    work_id TEXT NOT NULL DEFAULT '',
                    stage TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL DEFAULT '',
                    timestamp_utc TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_status_events_owner
                    ON status_events(owner_kind, owner_id, id);
                CREATE TABLE IF NOT EXISTS artifacts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    owner_kind TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    work_id TEXT NOT NULL DEFAULT '',
                    kind TEXT NOT NULL DEFAULT '',
                    path TEXT NOT NULL DEFAULT '',
                    size_bytes INTEGER,
                    sha256 TEXT NOT NULL DEFAULT '',
                    metadata_json TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_artifacts_owner
                    ON artifacts(owner_kind, owner_id);
                CREATE TABLE IF NOT EXISTS stage_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    work_id TEXT NOT NULL,
                    stage TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    failure_json TEXT NOT NULL,
                    metrics_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at_utc TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_stage_results_work_id
                    ON stage_results(work_id, id);
                CREATE TABLE IF NOT EXISTS dataset_plans (
                    plan_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL DEFAULT '',
                    work_item_count INTEGER NOT NULL DEFAULT 0,
                    work_ids_json TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_dataset_plans_request_id
                    ON dataset_plans(request_id);
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL DEFAULT '',
                    workflow_id TEXT NOT NULL DEFAULT '',
                    run_id TEXT NOT NULL DEFAULT '',
                    lifecycle TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT '',
                    task_queue TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL,
                    updated_at_utc TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_status
                    ON jobs(status);
                CREATE INDEX IF NOT EXISTS idx_jobs_request_id
                    ON jobs(request_id);
                """)
            _migrate_manifest_schema(conn, user_version)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _insert_status_event(
        self,
        conn: sqlite3.Connection,
        *,
        owner_kind: str,
        owner_id: str,
        event: StatusEvent,
    ) -> None:
        payload = event.to_dict()
        timestamp = event.timestamp_utc or _utc_now()
        payload["timestamp_utc"] = timestamp
        conn.execute(
            """
            INSERT INTO status_events (
                owner_kind,
                owner_id,
                work_id,
                stage,
                status,
                message,
                timestamp_utc,
                payload_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_kind,
                owner_id,
                event.work_id,
                event.stage,
                event.status.value,
                event.message,
                timestamp,
                _json_dumps(payload),
            ),
        )

    def _insert_artifact(
        self,
        conn: sqlite3.Connection,
        *,
        owner_kind: str,
        owner_id: str,
        work_id: str,
        artifact: ArtifactRef,
        created_at_utc: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO artifacts (
                owner_kind,
                owner_id,
                work_id,
                kind,
                path,
                size_bytes,
                sha256,
                metadata_json,
                created_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                owner_kind,
                owner_id,
                work_id,
                artifact.kind,
                artifact.path,
                artifact.size_bytes,
                artifact.sha256,
                _json_dumps(artifact.metadata),
                created_at_utc,
            ),
        )


def restore_record_from_manifest(
    record: Any,
    *,
    base_dir: str | Path,
) -> bool:
    """Restore a legacy Record from manifest storage or migrate `.meta`."""
    store = ManifestStatusStore(base_dir)
    item = store.get_work_item_for_record(record)
    if item is not None:
        _apply_work_item_to_record(record, item)
        return True

    meta_path = Path(record.data_dir, ".meta")
    result = store.import_meta_file(meta_path, record=record)
    if not result.migrated:
        return False

    migrated = store.get_work_item(result.work_id)
    if migrated is None:
        return False
    _apply_work_item_to_record(record, migrated)
    return True


def delete_record_from_manifest(
    record: Any,
    *,
    base_dir: str | Path = "",
) -> None:
    """Delete current manifest work-item state for a legacy Record."""
    store = ManifestStatusStore.existing_for_record(record, base_dir=base_dir)
    if store is not None:
        store.delete_record(record)


def _apply_work_item_to_record(record: Any, item: WorkItem) -> None:
    values = item.to_record_kwargs()
    values.pop("data_dir", None)
    record(**values)


def _candidate_roots(
    record: Any, *, base_dir: str | Path = ""
) -> tuple[Path, ...]:
    roots: list[Path] = []
    if base_dir:
        roots.append(Path(base_dir).expanduser())
    data_dir = str(getattr(record, "data_dir", "") or "")
    if data_dir:
        path = Path(data_dir).expanduser()
        roots.append(path)
        roots.extend(path.parents)

    seen: set[str] = set()
    unique: list[Path] = []
    for root in roots:
        key = _path_key(root)
        if key and key not in seen:
            seen.add(key)
            unique.append(root)
    return tuple(unique)


def _work_id_from_mapping(values: Mapping[str, Any]) -> str:
    return str(
        derive_work_id(
            str(values.get("url", "") or ""),
            str(values.get("data_format", "") or ""),
            str(values.get("data_timeframe", "") or ""),
            str(values.get("data_fxpair", "") or ""),
            str(values.get("data_datemonth", "") or ""),
        )
    )


def _mapping_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    to_dict = getattr(value, "to_dict", None)
    if callable(to_dict):
        return dict(to_dict())
    raise TypeError("Manifest snapshot payload must be a mapping or to_dict().")


def _coerce_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _coerce_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _validate_retention_limit(name: str, value: int) -> None:
    if value < 0:
        raise ValueError(f"{name} must be greater than or equal to zero.")


def _older_key_values(
    conn: sqlite3.Connection,
    *,
    table: str,
    key_column: str,
    keep: int,
    order_by: str,
) -> tuple[str, ...]:
    rows = conn.execute(
        f"SELECT {key_column} FROM {table} ORDER BY {order_by}"
    ).fetchall()
    return tuple(str(row[key_column]) for row in rows[keep:])


def _delete_text_values(
    conn: sqlite3.Connection,
    *,
    table: str,
    column: str,
    values: tuple[str, ...],
) -> int:
    if not values:
        return 0
    placeholders = ",".join("?" for _ in values)
    cursor = conn.execute(
        f"DELETE FROM {table} WHERE {column} IN ({placeholders})",
        values,
    )
    return _rowcount(cursor)


def _delete_owner_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    owner_kind: str,
    owner_ids: tuple[str, ...],
) -> int:
    if not owner_ids:
        return 0
    placeholders = ",".join("?" for _ in owner_ids)
    cursor = conn.execute(
        f"""
        DELETE FROM {table}
        WHERE owner_kind = ? AND owner_id IN ({placeholders})
        """,
        (owner_kind, *owner_ids),
    )
    return _rowcount(cursor)


def _delete_group_overflow_rows(
    conn: sqlite3.Connection,
    *,
    table: str,
    id_column: str,
    group_columns: tuple[str, ...],
    keep: int,
    order_by: str | None = None,
) -> int:
    overflow_ids = _group_overflow_ids(
        conn,
        table=table,
        id_column=id_column,
        group_columns=group_columns,
        keep=keep,
        order_by=order_by or f"{id_column} DESC",
    )
    return _delete_text_values(
        conn,
        table=table,
        column=id_column,
        values=overflow_ids,
    )


def _group_overflow_ids(
    conn: sqlite3.Connection,
    *,
    table: str,
    id_column: str,
    group_columns: tuple[str, ...],
    keep: int,
    order_by: str,
) -> tuple[str, ...]:
    select_columns = ", ".join(group_columns)
    groups = conn.execute(
        f"SELECT {select_columns} FROM {table} GROUP BY {select_columns}"
    ).fetchall()
    ids: list[str] = []
    where = " AND ".join(f"{column} = ?" for column in group_columns)
    for group in groups:
        params = tuple(group[column] for column in group_columns)
        rows = conn.execute(
            f"""
            SELECT {id_column}
            FROM {table}
            WHERE {where}
            ORDER BY {order_by}
            """,
            params,
        ).fetchall()
        ids.extend(str(row[id_column]) for row in rows[keep:])
    return tuple(ids)


def _rowcount(cursor: sqlite3.Cursor) -> int:
    return max(0, int(cursor.rowcount))


def _live_snapshot_payload(
    stored: Mapping[str, Any],
    *,
    request_id: str,
    job_id: str,
    workflow_id: str,
    task_queue: str,
    namespace: str,
    result: StageResult,
    work_item: WorkItem | None,
    metadata: Mapping[str, JSONValue] | None,
) -> dict[str, Any]:
    stored_payload = dict(stored)
    now = _utc_now()
    progress_metadata = _coerce_mapping(result.metrics.get("progress"))
    stored_progress = _coerce_mapping(stored_payload.get("progress"))
    updated_at = str(
        progress_metadata.get("updated_at_utc", "")
        or stored_progress.get("updated_at_utc", "")
        or now
    )
    started_at = str(
        stored_progress.get("started_at_utc", "")
        or progress_metadata.get("started_at_utc", "")
        or updated_at
    )
    events = _bounded_events(
        (
            *_coerce_list(stored_progress.get("events")),
            *(event.to_dict() for event in result.events),
        )
    )
    logs = _bounded_logs(
        (
            *_coerce_list(stored_payload.get("logs")),
            *(_log_from_event(event) for event in events),
        )
    )
    artifacts = _bounded_artifacts(
        (
            *_coerce_list(stored_payload.get("artifacts")),
            *_coerce_list(stored_progress.get("artifacts")),
            *(artifact.to_dict() for artifact in result.artifacts),
        )
    )
    status = result.status.value
    progress = {
        "workflow_name": str(
            stored_progress.get("workflow_name", "") or "HistDataRunWorkflow"
        ),
        "request_id": request_id,
        "status": status,
        "current_stage": result.stage,
        "total_children": _progress_int(
            progress_metadata,
            "total",
            stored_progress.get("total_children", 0),
        ),
        "completed_children": _progress_int(
            progress_metadata,
            "completed",
            stored_progress.get("completed_children", 0),
        ),
        "unit": str(
            progress_metadata.get("unit", "")
            or stored_progress.get("unit", "")
            or "work_items"
        ),
        "started_at_utc": started_at,
        "updated_at_utc": updated_at,
        "rate_per_second": _progress_float(
            progress_metadata.get("rate_per_second"),
            stored_progress.get("rate_per_second", 0.0),
        ),
        "last_error": str(
            result.failure.message
            if result.failure is not None
            else progress_metadata.get(
                "last_error",
                stored_progress.get("last_error", ""),
            )
            or ""
        ),
        "planned_children": _string_tuple(
            _coerce_list(stored_progress.get("planned_children"))
        ),
        "completed_stages": _completed_stages(
            stored_progress,
            result.stage,
        ),
        "events": events,
        "artifacts": artifacts,
    }
    stored_metadata = _coerce_mapping(stored_payload.get("metadata"))
    live_metadata = dict(metadata or {})
    live_metadata.setdefault("live_status_store", True)
    live_metadata.setdefault("live_status_write_stage", result.stage)
    if work_item is not None:
        live_metadata.setdefault("live_status_work_id", work_item.work_id)

    stored_result = _coerce_mapping(stored_payload.get("result"))
    return {
        "schema_version": int(
            stored_payload.get("schema_version", MANIFEST_SCHEMA_VERSION) or 1
        ),
        "job_id": job_id,
        "request_id": request_id,
        "workflow_id": workflow_id or job_id,
        "run_id": str(stored_payload.get("run_id", "") or ""),
        "namespace": str(stored_payload.get("namespace", "") or namespace),
        "task_queue": str(stored_payload.get("task_queue", "") or task_queue),
        "lifecycle": _live_lifecycle(
            result.status,
            str(stored_payload.get("lifecycle", "") or ""),
        ),
        "status": status,
        "progress": progress,
        "controls": _coerce_mapping(stored_payload.get("controls")),
        "logs": logs,
        "artifacts": artifacts,
        "result": {
            "request_id": request_id,
            "workflow_name": "HistDataRunWorkflow",
            "status": status,
            "stage_result_count": _stored_count(
                stored_result,
                "stage_result_count",
            )
            + 1,
            "work_item_count": _stored_count(
                stored_result,
                "work_item_count",
            )
            + (1 if work_item is not None else 0),
            "artifact_count": len(artifacts),
            "progress": {
                "current_stage": result.stage,
                "status": status,
                "event_count": len(events),
                "artifact_count": len(artifacts),
                "updated_at_utc": updated_at,
            },
        },
        "sidecar_state": str(stored_payload.get("sidecar_state", "") or ""),
        "sidecar_message": str(stored_payload.get("sidecar_message", "") or ""),
        "updated_at_utc": updated_at,
        "metadata": {
            **stored_metadata,
            **live_metadata,
        },
    }


def _bounded_events(values: tuple[Any, ...]) -> list[dict[str, JSONValue]]:
    events: list[dict[str, JSONValue]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for value in values:
        payload = _coerce_mapping(value)
        if not payload:
            continue
        key = (
            str(payload.get("timestamp_utc", "") or ""),
            str(payload.get("work_id", "") or ""),
            str(payload.get("stage", "") or ""),
            str(payload.get("status", "") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        events.append(payload)
    return events[-LIVE_SNAPSHOT_EVENT_LIMIT:]


def _bounded_artifacts(values: tuple[Any, ...]) -> list[dict[str, JSONValue]]:
    artifacts: list[dict[str, JSONValue]] = []
    seen: set[tuple[str, str]] = set()
    for value in values:
        payload = _coerce_mapping(value)
        kind = str(payload.get("kind", "") or "")
        path = str(payload.get("path", "") or "")
        if not kind and not path:
            continue
        key = (kind, path)
        if key in seen:
            continue
        seen.add(key)
        artifacts.append(payload)
    return artifacts[-LIVE_SNAPSHOT_ARTIFACT_LIMIT:]


def _bounded_logs(values: tuple[Any, ...]) -> list[dict[str, JSONValue]]:
    logs: list[dict[str, JSONValue]] = []
    seen: set[tuple[str, str, str]] = set()
    for value in values:
        payload = _coerce_mapping(value)
        if not payload:
            continue
        key = (
            str(payload.get("timestamp_utc", "") or ""),
            str(payload.get("source", "") or ""),
            str(payload.get("message", "") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        logs.append(payload)
    return logs[-LIVE_SNAPSHOT_EVENT_LIMIT:]


def _log_from_event(event: Mapping[str, Any]) -> dict[str, JSONValue]:
    metadata = _coerce_mapping(event.get("metadata"))
    status = str(event.get("status", "") or "")
    return {
        "source": str(metadata.get("source") or event.get("stage", "") or ""),
        "level": str(
            metadata.get("level")
            or ("error" if status == WorkStatus.FAILED.value else "info")
        ),
        "message": str(event.get("message", "") or ""),
        "timestamp_utc": str(event.get("timestamp_utc", "") or ""),
        "metadata": metadata,
    }


def _completed_stages(
    stored_progress: Mapping[str, Any],
    stage: str,
) -> list[str]:
    stages = _string_tuple(
        _coerce_list(stored_progress.get("completed_stages"))
    )
    if stage and stage not in stages:
        stages = (*stages, stage)
    return list(stages)


def _string_tuple(values: list[Any]) -> tuple[str, ...]:
    return tuple(str(value) for value in values if str(value or ""))


def _progress_int(
    progress_metadata: Mapping[str, Any],
    key: str,
    fallback: Any,
) -> int:
    value = progress_metadata.get(key, fallback)
    try:
        return max(0, int(float(value or 0)))
    except (TypeError, ValueError):
        return 0


def _progress_float(value: Any, fallback: Any) -> float:
    try:
        return float(value if value not in (None, "") else fallback or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _stored_count(payload: Mapping[str, Any], key: str) -> int:
    try:
        return max(0, int(payload.get(key, 0) or 0))
    except (TypeError, ValueError):
        return 0


def _live_lifecycle(status: WorkStatus, stored_lifecycle: str) -> str:
    if status == WorkStatus.FAILED:
        return "failed"
    if status == WorkStatus.CANCELLED:
        return "cancelled"
    if stored_lifecycle in {
        "cancel_requested",
        "retry_requested",
        "resume_requested",
    }:
        return stored_lifecycle
    return "running"


def _read_user_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("PRAGMA user_version").fetchone()
    return int(row[0] if row is not None else 0)


def _set_user_version(conn: sqlite3.Connection, version: int) -> None:
    conn.execute(f"PRAGMA user_version = {int(version)}")


def _migrate_manifest_schema(
    conn: sqlite3.Connection,
    user_version: int,
) -> None:
    if user_version == MANIFEST_SCHEMA_VERSION:
        return
    if user_version > MANIFEST_SCHEMA_VERSION:
        raise ValueError(
            "Unsupported manifest/status schema version "
            f"{user_version}; expected <= {MANIFEST_SCHEMA_VERSION}."
        )
    _set_user_version(conn, MANIFEST_SCHEMA_VERSION)


def _manifest_schema_status(
    db_path: Path,
    *,
    exists: bool,
    user_version: int = 0,
    state: str = "",
    error: str = "",
) -> dict[str, Any]:
    if not exists:
        resolved_state = "missing"
    elif state:
        resolved_state = state
    elif user_version > MANIFEST_SCHEMA_VERSION:
        resolved_state = "unsupported"
        error = (
            "Unsupported manifest/status schema version "
            f"{user_version}; expected <= {MANIFEST_SCHEMA_VERSION}."
        )
    elif user_version == MANIFEST_SCHEMA_VERSION:
        resolved_state = "current"
    elif user_version == 0:
        resolved_state = "legacy_unversioned"
    else:
        resolved_state = "migration_required"
    return {
        "path": str(db_path),
        "exists": exists,
        "schema_version": user_version,
        "expected_schema_version": MANIFEST_SCHEMA_VERSION,
        "state": resolved_state,
        "error": error,
    }


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def _path_key(path: str | Path) -> str:
    text = str(path or "")
    if not text:
        return ""
    return str(Path(text).expanduser().resolve(strict=False))


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
