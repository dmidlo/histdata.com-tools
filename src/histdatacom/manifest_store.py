"""SQLite-backed manifest and status storage."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
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
        source: str = "record_memento",
        message: str = "Record metadata stored.",
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
        return ManifestMigrationResult(
            path=str(path),
            migrated=True,
            reason="imported",
            work_id=item.work_id,
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

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
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


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True)


def _path_key(path: str | Path) -> str:
    text = str(path or "")
    if not text:
        return ""
    return str(Path(text).expanduser().resolve(strict=False))


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
