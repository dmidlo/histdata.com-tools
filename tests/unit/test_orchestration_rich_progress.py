"""Tests for Rich orchestration progress rendering."""

from __future__ import annotations

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
