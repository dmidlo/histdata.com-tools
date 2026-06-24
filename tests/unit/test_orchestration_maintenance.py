"""Tests for orchestration runtime retention and maintenance."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from histdatacom.manifest_store import (
    MANIFEST_SCHEMA_VERSION,
    ManifestStatusStore,
)
from histdatacom.runtime_contracts import (
    ArtifactRef,
    StageResult,
    StatusEvent,
    WorkItem,
    WorkStatus,
)
from histdatacom.orchestration.maintenance import (
    OrchestrationRetentionPolicy,
    run_orchestration_maintenance,
)
from histdatacom.orchestration.runtime import build_orchestration_runtime_policy


def test_orchestration_maintenance_rotates_logs_and_prunes_status_store(
    tmp_path: Path,
) -> None:
    """Maintenance should bound orchestration state without deleting data artifacts."""
    runtime_policy = build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    runtime_policy.paths.logs_dir.mkdir(parents=True)
    runtime_policy.paths.sqlite_dir.mkdir(parents=True)
    runtime_policy.paths.server_log.write_text("x" * 32, encoding="utf-8")
    runtime_policy.paths.server_log.with_name(
        "temporal-server.log.1"
    ).write_text("old", encoding="utf-8")
    runtime_policy.paths.sqlite_db.write_bytes(b"sqlite-history")
    download_dir = tmp_path / "downloads"
    archive_paths = tuple(
        _write_artifact(download_dir / f"archive-{index}.zip")
        for index in range(3)
    )
    csv_path = _write_artifact(download_dir / "DAT_ASCII_EURUSD_M1_202201.csv")
    cache_path = _write_artifact(download_dir / ".data")
    store = ManifestStatusStore(runtime_policy.paths.manifests_dir)
    _write_retained_rows(store, archive_paths)
    retention_policy = OrchestrationRetentionPolicy(
        max_log_bytes=8,
        max_rotated_logs=1,
        max_temporal_sqlite_bytes=8,
        max_job_snapshots=1,
        max_status_events_per_owner=1,
        max_stage_results_per_work_item=1,
        max_artifacts_per_owner=1,
        max_dataset_plans_per_request=1,
    )

    result = run_orchestration_maintenance(
        runtime_policy,
        retention_policy,
        orchestration_state="stopped",
    )
    payload = result.to_dict()

    assert result.state == "completed"
    assert payload["downloaded_artifacts_removed"] is False
    assert payload["logs"][0]["action"] == "rotated"
    assert runtime_policy.paths.server_log.read_text(encoding="utf-8") == ""
    assert (
        runtime_policy.paths.server_log.with_name(
            "temporal-server.log.1"
        ).read_text(encoding="utf-8")
        == "x" * 32
    )
    assert payload["temporal_sqlite"]["action"] == "preserved"
    assert payload["temporal_sqlite"]["within_limit"] is False
    assert runtime_policy.paths.sqlite_db.exists()
    assert payload["status_store"]["schema_state"] == "current"
    assert payload["status_store"]["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert _table_count(store.db_path, "jobs") == 1
    assert _table_count(store.db_path, "stage_results") == 1
    assert _table_count(store.db_path, "dataset_plans") == 1
    assert sum(payload["status_store"]["rows_deleted"].values()) > 0
    [stage_result] = store.list_stage_results("work-retention")
    [event] = store.status_history("work-retention", owner_kind="work_item")
    [artifact] = store.list_artifacts("work-retention")
    assert stage_result["stage"] == "stage-2"
    assert event["stage"] == "stage-2"
    assert artifact["path"] == str(archive_paths[2])
    assert len(store.list_job_snapshots()) == 1
    assert all(path.exists() for path in (*archive_paths, csv_path, cache_path))


def test_orchestration_maintenance_skips_mutation_while_running(
    tmp_path: Path,
) -> None:
    """Default maintenance should not mutate live logs or SQLite stores."""
    runtime_policy = build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    runtime_policy.paths.logs_dir.mkdir(parents=True)
    runtime_policy.paths.server_log.write_text("x" * 32, encoding="utf-8")
    store = ManifestStatusStore(runtime_policy.paths.manifests_dir)
    archive_paths = tuple(
        _write_artifact(tmp_path / "downloads" / f"archive-{index}.zip")
        for index in range(3)
    )
    _write_retained_rows(store, archive_paths)

    result = run_orchestration_maintenance(
        runtime_policy,
        OrchestrationRetentionPolicy(
            max_log_bytes=8,
            max_rotated_logs=1,
            max_job_snapshots=1,
            max_status_events_per_owner=1,
            max_stage_results_per_work_item=1,
            max_artifacts_per_owner=1,
            max_dataset_plans_per_request=1,
        ),
        orchestration_state="running",
    )

    assert result.state == "skipped"
    assert (
        runtime_policy.paths.server_log.read_text(encoding="utf-8") == "x" * 32
    )
    assert _table_count(store.db_path, "jobs") == 3
    assert _table_count(store.db_path, "stage_results") == 3


def test_orchestration_maintenance_reports_future_status_store_schema(
    tmp_path: Path,
) -> None:
    """Unsupported manifest DB versions should surface as maintenance errors."""
    runtime_policy = build_orchestration_runtime_policy(
        workspace=tmp_path / "workspace",
        runtime_home=tmp_path / "runtime",
    )
    store_path = ManifestStatusStore.path_for_root(
        runtime_policy.paths.manifests_dir
    )
    store_path.parent.mkdir(parents=True)
    with sqlite3.connect(store_path) as conn:
        conn.execute(f"PRAGMA user_version = {MANIFEST_SCHEMA_VERSION + 1}")

    result = run_orchestration_maintenance(
        runtime_policy, orchestration_state="stopped"
    )
    payload = result.to_dict()

    assert result.state == "error"
    assert payload["status_store"]["action"] == "error"
    assert payload["status_store"]["schema_state"] == "unsupported"
    assert payload["status_store"]["schema_version"] == (
        MANIFEST_SCHEMA_VERSION + 1
    )


def _write_retained_rows(
    store: ManifestStatusStore,
    artifact_paths: tuple[Path, ...],
) -> None:
    for index, artifact_path in enumerate(artifact_paths):
        store.write_stage_result(
            StageResult(
                work_id="work-retention",
                stage=f"stage-{index}",
                status=WorkStatus.COMPLETED,
                artifacts=(ArtifactRef(kind="zip", path=str(artifact_path)),),
                events=(
                    StatusEvent(
                        status=WorkStatus.COMPLETED,
                        stage=f"stage-{index}",
                        message=f"stage {index}",
                        work_id="work-retention",
                    ),
                ),
            )
        )
        store.write_job_snapshot(
            {
                "job_id": f"histdatacom-run-{index}",
                "request_id": "run-retention",
                "workflow_id": f"histdatacom-run-{index}",
                "run_id": f"temporal-run-{index}",
                "lifecycle": "completed",
                "status": WorkStatus.COMPLETED.value,
                "task_queue": "histdatacom.test.orchestration",
                "progress": {
                    "current_stage": f"stage-{index}",
                    "events": [
                        StatusEvent(
                            status=WorkStatus.COMPLETED,
                            stage=f"job-stage-{index}",
                            message=f"job {index}",
                            work_id=f"histdatacom-run-{index}",
                        ).to_dict()
                    ],
                    "artifacts": [
                        ArtifactRef(
                            kind="zip",
                            path=str(artifact_path),
                        ).to_dict()
                    ],
                },
            }
        )
        store.write_dataset_plan(
            plan_id=f"plan-{index}",
            request_id="run-retention",
            work_items=(
                WorkItem(
                    work_id=f"work-plan-{index}",
                    status=WorkStatus.PLANNED,
                ),
            ),
        )


def _write_artifact(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("artifact", encoding="utf-8")
    return path


def _table_count(db_path: Path, table: str) -> int:
    sql = {
        "jobs": "SELECT COUNT(*) FROM jobs",
        "stage_results": "SELECT COUNT(*) FROM stage_results",
        "dataset_plans": "SELECT COUNT(*) FROM dataset_plans",
    }[table]
    with sqlite3.connect(db_path) as conn:
        return int(conn.execute(sql).fetchone()[0])
