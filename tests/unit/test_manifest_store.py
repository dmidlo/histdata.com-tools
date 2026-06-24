"""Tests for manifest/status storage."""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from histdatacom.manifest_store import (
    MANIFEST_DB_FILENAME,
    MANIFEST_DIRECTORY,
    MANIFEST_SCHEMA_VERSION,
    ManifestStatusStore,
)
from histdatacom.records import Record
from histdatacom.runtime_contracts import (
    ArtifactRef,
    FailureInfo,
    StageResult,
    StatusEvent,
    WorkItem,
    WorkStatus,
)
from histdatacom.orchestration.control import (
    JobLifecycle,
    JobProgressSnapshot,
    OrchestrationJobSnapshot,
)

ASCII_M1_URL = (
    "http://www.histdata.com/download-free-forex-data/"
    "?/ascii/1-minute-bar-quotes/eurusd/2022"
)


def _expected_ascii_m1_dir(base_dir: Path) -> str:
    data_path = Path("ASCII", "M1", "eurusd", "2022")
    return f"{base_dir / data_path}{os.sep}"


def test_record_write_creates_manifest_without_legacy_meta(
    tmp_path: Path,
) -> None:
    """Record writes should upsert the manifest without legacy orchestrations."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.CSV_FILE.value)

    record.write_manifest_status(base_dir=str(tmp_path))

    db_path = tmp_path / MANIFEST_DIRECTORY / MANIFEST_DB_FILENAME
    meta_path = Path(record.data_dir) / ".meta"
    store = ManifestStatusStore(tmp_path)
    [item] = store.list_work_items()
    history = store.status_history(item.work_id, owner_kind="work_item")

    assert db_path.exists()
    assert not meta_path.exists()
    assert item.status is WorkStatus.CSV_FILE
    assert item.data_dir == _expected_ascii_m1_dir(tmp_path)
    assert history[-1]["stage"] == "record_manifest_status"


def test_manifest_store_sets_user_version_for_unversioned_database(
    tmp_path: Path,
) -> None:
    """Opening a legacy unversioned SQLite store should mark schema v1."""
    db_path = tmp_path / MANIFEST_DIRECTORY / MANIFEST_DB_FILENAME
    db_path.parent.mkdir(parents=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("CREATE TABLE legacy_marker (id INTEGER PRIMARY KEY)")
        conn.execute("PRAGMA user_version = 0")

    before = ManifestStatusStore.inspect_schema(tmp_path)
    store = ManifestStatusStore(tmp_path)
    after = store.schema_status()

    assert before["state"] == "legacy_unversioned"
    assert after["state"] == "current"
    assert after["schema_version"] == MANIFEST_SCHEMA_VERSION
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == (
            MANIFEST_SCHEMA_VERSION
        )


def test_manifest_store_rejects_future_user_version(
    tmp_path: Path,
) -> None:
    """Opening a newer manifest schema should fail without mutation."""
    db_path = tmp_path / MANIFEST_DIRECTORY / MANIFEST_DB_FILENAME
    db_path.parent.mkdir(parents=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute(f"PRAGMA user_version = {MANIFEST_SCHEMA_VERSION + 1}")

    inspection = ManifestStatusStore.inspect_schema(tmp_path)

    with pytest.raises(ValueError, match="Unsupported manifest/status schema"):
        ManifestStatusStore(tmp_path)

    assert inspection["state"] == "unsupported"
    assert inspection["schema_version"] == MANIFEST_SCHEMA_VERSION + 1


def test_record_delete_clears_current_manifest_state(
    tmp_path: Path,
) -> None:
    """Deleting status should clear manifest and existing legacy metadata."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.CSV_FILE.value)
    record.write_manifest_status(base_dir=str(tmp_path))
    meta_path = Path(record.data_dir) / ".meta"
    meta_path.write_text("{}", encoding="UTF-8")
    store = ManifestStatusStore(tmp_path)
    assert store.list_work_items()

    record.delete_manifest_status()

    assert not meta_path.exists()
    assert not store.list_work_items()


def test_restore_imports_legacy_meta_without_manifest(
    tmp_path: Path,
) -> None:
    """Existing `.meta` files should be imported into the manifest store."""
    current_base = tmp_path / "current"
    stale_base = tmp_path / "stale"
    current_data_dir = _expected_ascii_m1_dir(current_base)
    stale_data_dir = _expected_ascii_m1_dir(stale_base)
    meta_path = Path(current_data_dir) / ".meta"
    meta_path.parent.mkdir(parents=True)
    meta_path.write_text(
        json.dumps(
            {
                "url": ASCII_M1_URL,
                "status": "CSV_FILE",
                "data_dir": stale_data_dir,
                "zip_filename": "legacy.zip",
            },
        ),
        encoding="UTF-8",
    )
    restored = Record(url=ASCII_M1_URL)

    assert restored.restore_manifest_status(str(current_base))

    store = ManifestStatusStore(current_base)
    [item] = store.list_work_items()
    history = store.status_history(item.work_id, owner_kind="work_item")

    assert restored.status is WorkStatus.CSV_FILE
    assert restored.zip_filename == "legacy.zip"
    assert restored.data_dir == current_data_dir
    assert not meta_path.exists()
    assert item.data_dir == current_data_dir
    assert history[-1]["stage"] == "legacy_meta_import"


def test_missing_or_corrupt_legacy_meta_is_graceful(
    tmp_path: Path,
) -> None:
    """Missing and corrupt legacy metadata should not crash restore/import."""
    missing = Record(url=ASCII_M1_URL)
    corrupt = Record(url=ASCII_M1_URL)
    corrupt_data_dir = _expected_ascii_m1_dir(tmp_path / "corrupt")
    corrupt_meta = Path(corrupt_data_dir) / ".meta"
    corrupt_meta.parent.mkdir(parents=True)
    corrupt_meta.write_text("{not-json", encoding="UTF-8")

    assert not missing.restore_manifest_status(str(tmp_path / "missing"))
    assert not corrupt.restore_manifest_status(str(tmp_path / "corrupt"))

    result = ManifestStatusStore(tmp_path).import_meta_file(
        tmp_path / "does-not-exist" / ".meta"
    )
    corrupt_result = ManifestStatusStore(tmp_path).import_meta_file(
        corrupt_meta
    )

    assert result.reason == "missing"
    assert not result.migrated
    assert corrupt_result.reason == "corrupt"
    assert not corrupt_result.migrated
    assert corrupt_meta.exists()


def test_manifest_store_persists_stage_result_details(
    tmp_path: Path,
) -> None:
    """Stage artifacts, status history, metrics, and failures are queryable."""
    store = ManifestStatusStore(tmp_path)
    result = StageResult(
        work_id="work-stage",
        stage="download_archive",
        status=WorkStatus.RETRIED,
        artifacts=(
            ArtifactRef(
                kind="zip",
                path=str(tmp_path / "archive.zip"),
                size_bytes=123,
                sha256="abc",
                metadata={"pair": "eurusd"},
            ),
        ),
        events=(
            StatusEvent(
                status=WorkStatus.RETRIED,
                stage="download_archive",
                message="temporary network error",
                work_id="work-stage",
                metadata={"attempt": 2},
            ),
        ),
        failure=FailureInfo(
            code="DOWNLOAD_RETRYABLE",
            message="temporary network error",
            retryable=True,
            detail={"timeout": 10},
        ),
        metrics={"attempts": 2},
    )

    store.write_stage_result(result)

    [stored_result] = store.list_stage_results("work-stage")
    [event] = store.status_history("work-stage", owner_kind="work_item")
    [artifact] = store.list_artifacts("work-stage")

    assert stored_result["failure"]["code"] == "DOWNLOAD_RETRYABLE"
    assert stored_result["metrics"]["attempts"] == 2
    assert event["metadata"]["attempt"] == 2
    assert artifact["kind"] == "zip"
    assert artifact["metadata"]["pair"] == "eurusd"


def test_manifest_store_persists_dataset_plan_work_items(
    tmp_path: Path,
) -> None:
    """Dataset plans should store ordered work-item metadata by reference."""
    store = ManifestStatusStore(tmp_path)
    first = WorkItem(
        work_id="work-eurusd-202201",
        status=WorkStatus.URL_NEW,
        data_fxpair="eurusd",
        data_timeframe="T",
        data_format="ASCII",
        data_datemonth="202201",
    )
    second = WorkItem(
        work_id="work-eurusd-202202",
        status=WorkStatus.URL_NEW,
        data_fxpair="eurusd",
        data_timeframe="T",
        data_format="ASCII",
        data_datemonth="202202",
    )
    unrelated = WorkItem(
        work_id="work-usdjpy-202202",
        status=WorkStatus.URL_NEW,
        data_fxpair="usdjpy",
        data_timeframe="T",
        data_format="ASCII",
        data_datemonth="202202",
    )

    plan_ref = store.write_dataset_plan(
        plan_id="plan-1",
        request_id="run-plan",
        work_items=(first, second),
        metadata={"pair": "eurusd"},
    )
    store.write_work_item(unrelated, source="unrelated")

    plan = store.get_dataset_plan("plan-1")
    loaded = store.get_dataset_plan_work_items(
        "plan-1",
        work_ids=(
            "work-usdjpy-202202",
            "work-eurusd-202202",
            "work-eurusd-202201",
        ),
    )

    assert plan_ref["plan_id"] == "plan-1"
    assert plan_ref["store_root"] == str(tmp_path)
    assert plan is not None
    assert plan["work_item_count"] == 2
    assert [item.work_id for item in loaded] == [
        "work-eurusd-202202",
        "work-eurusd-202201",
    ]


def test_manifest_store_persists_orchestration_job_snapshots(
    tmp_path: Path,
) -> None:
    """Orchestration job status should be queryable without Temporal history."""
    store = ManifestStatusStore(tmp_path)
    snapshot = OrchestrationJobSnapshot(
        job_id="histdatacom-run-1",
        request_id="run-1",
        workflow_id="histdatacom-run-1",
        run_id="temporal-run-1",
        task_queue="histdatacom-orchestration",
        lifecycle=JobLifecycle.RUNNING,
        status=WorkStatus.CACHE_READY,
        progress=JobProgressSnapshot(
            workflow_name="HistDataRunWorkflow",
            request_id="run-1",
            status=WorkStatus.CACHE_READY,
            current_stage="build_cache",
            total_children=4,
            completed_children=3,
            events=(
                StatusEvent(
                    status=WorkStatus.CACHE_READY,
                    stage="build_cache",
                    message="Cache ready.",
                    work_id="work-cache",
                ),
            ),
            artifacts=(
                ArtifactRef(
                    kind="cache",
                    path=str(tmp_path / ".data"),
                    metadata={"line_count": 3},
                ),
            ),
        ),
    )

    store.write_job_snapshot(snapshot)

    stored = store.get_job_snapshot("histdatacom-run-1")
    [listed] = store.list_job_snapshots(status=WorkStatus.CACHE_READY)
    history = store.status_history("histdatacom-run-1", owner_kind="job")
    [artifact] = store.list_artifacts("histdatacom-run-1", owner_kind="job")

    assert stored is not None
    assert stored["request_id"] == "run-1"
    assert listed["workflow_id"] == "histdatacom-run-1"
    assert history[-1]["stage"] == "build_cache"
    assert artifact["kind"] == "cache"
    assert artifact["metadata"]["line_count"] == 3
