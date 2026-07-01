"""Tests for Rich orchestration progress rendering."""

from __future__ import annotations

from dataclasses import replace

from rich.console import Console

from histdatacom.runtime_contracts import StatusEvent, WorkStatus
from histdatacom.orchestration.control import (
    JobProgressSnapshot,
    OrchestrationJobSnapshot,
)
from histdatacom.orchestration.rich_progress import (
    build_job_progress_renderable,
)


class _Handle:
    """Minimal handle shape for renderer tests."""

    request_id = "run-rich"
    workflow_id = "histdatacom-run-rich"
    run_id = "run-fake"
    task_queue = "histdatacom.test.orchestration"
    namespace = "default"


def test_rich_progress_uses_operator_stage_labels() -> None:
    """The live progress panel should avoid internal workflow class names."""
    progress = JobProgressSnapshot(
        workflow_name="HistDataRunWorkflow",
        request_id="run-rich",
        status=WorkStatus.UNKNOWN,
        current_stage="BuildCacheWorkflow",
        total_children=3,
        completed_children=2,
        planned_children=(
            "ValidateUrlsWorkflow",
            "DownloadArchivesWorkflow",
            "BuildCacheWorkflow",
        ),
        completed_stages=(
            "ValidateUrlsWorkflow",
            "DownloadArchivesWorkflow",
        ),
        events=(
            StatusEvent(
                status=WorkStatus.CACHE_READY,
                stage="BuildCacheWorkflow",
                message="BuildCacheWorkflow completed.",
            ),
        ),
    )
    snapshot = OrchestrationJobSnapshot.from_handle(_Handle()).with_progress(
        progress
    )
    console = Console(record=True, width=100)

    console.print(build_job_progress_renderable(snapshot))
    rendered = console.export_text()

    assert "Build Polars caches" in rendered
    assert "Validate URLs" in rendered
    assert "Download archives" in rendered
    assert "BuildCacheWorkflow" not in rendered


def test_rich_progress_renders_operational_health_without_paths() -> None:
    """The job dashboard should show bounded operator health signals."""
    progress = JobProgressSnapshot(
        workflow_name="HistDataRunWorkflow",
        request_id="run-rich",
        status=WorkStatus.CACHE_READY,
        current_stage="build_cache",
        total_children=10,
        completed_children=4,
        unit="files",
        rate_per_second=2.0,
    )
    snapshot = replace(
        OrchestrationJobSnapshot.from_handle(_Handle()).with_progress(progress),
        metadata={
            "runtime_health": {
                "state": "running",
                "message": "Orchestration server and workers are running.",
                "pid_count": 2,
                "component_count": 2,
                "components": {
                    "server": {"state": "running", "pid": 1234},
                    "worker:orchestration": {
                        "state": "running",
                        "pid": 1235,
                        "readiness_state": "ready",
                    },
                },
                "disk": {
                    "state": "ok",
                    "free_bytes": 20 * 1024**3,
                    "used_bytes": 10 * 1024**3,
                    "percent_used": 33.3,
                },
            },
            "operational_health": {
                "status": "partial-cache",
                "root": "/Users/example/private/data",
                "summary": {
                    "cache_count": 8,
                    "cache_size_bytes": 3 * 1024**3,
                    "source_artifact_count": 2,
                    "source_artifact_size_bytes": 12 * 1024**2,
                    "symbol_count": 4,
                    "symbols_with_cache": 3,
                },
                "cleanup": {"state": "pending"},
                "disk": {
                    "path": "/Users/example/private/data",
                    "state": "warning",
                    "free_bytes": 4 * 1024**3,
                    "used_bytes": 96 * 1024**3,
                    "percent_used": 96.0,
                },
                "workflows": {
                    "state": "active",
                    "active_count": 1,
                    "job_count": 2,
                },
                "groups": [
                    {
                        "group": "major-triangles",
                        "status": "partial-cache",
                        "symbols_with_cache": 3,
                        "expected_symbol_count": 4,
                        "source_artifact_count": 2,
                    }
                ],
            },
        },
    )
    console = Console(record=True, width=120)

    console.print(build_job_progress_renderable(snapshot))
    rendered = console.export_text()

    assert "Operational Health" in rendered
    assert "Runtime" in rendered
    assert "running" in rendered
    assert "server running pid 1234" in rendered
    assert "worker:orchestration running pid 1235 readiness ready" in rendered
    assert "Disk" in rendered
    assert "4.0 GB free" in rendered
    assert "96.0% used" in rendered
    assert "Cache" in rendered
    assert "8 .data cache(s)" in rendered
    assert "3/4 symbols cached" in rendered
    assert "Sources" in rendered
    assert "2 transient ZIP/CSV/XLS/XLSX artifact(s)" in rendered
    assert "major-triangles: partial-cache" in rendered
    assert "ETA:" in rendered
    assert "3s" in rendered
    assert "/Users/example" not in rendered
