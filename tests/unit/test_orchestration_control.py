"""Tests for GUI-ready orchestration control contracts."""

from __future__ import annotations

from types import SimpleNamespace

from histdatacom.runtime_contracts import ArtifactRef, StatusEvent, WorkStatus
from histdatacom.orchestration.control import (
    ControlOperationPhase,
    JobLifecycle,
    JobProgressSnapshot,
    OrchestrationJobSnapshot,
)


class _Handle:
    """Minimal handle shape for control snapshots."""

    request_id = "run-control"
    workflow_id = "histdatacom-run-control"
    run_id = "run-fake"
    task_queue = "histdatacom.test.orchestration"
    namespace = "default"


def test_progress_snapshot_serializes_events_and_artifacts() -> None:
    """Progress snapshots should be renderer-independent JSON payloads."""
    progress = JobProgressSnapshot(
        workflow_name="HistDataRunWorkflow",
        request_id="run-control",
        status=WorkStatus.UNKNOWN,
        current_stage="DownloadArchivesWorkflow",
        total_children=4,
        completed_children=1,
        unit="children",
        started_at_utc="2026-06-21T00:00:00Z",
        updated_at_utc="2026-06-21T00:00:04Z",
        rate_per_second=0.25,
        last_error="",
        planned_children=(
            "ValidateUrlsWorkflow",
            "DownloadArchivesWorkflow",
        ),
        completed_stages=("ValidateUrlsWorkflow",),
        events=(
            StatusEvent(
                status=WorkStatus.URL_VALID,
                stage="validate_urls",
                message="URLs validated",
                timestamp_utc="2026-06-21T00:00:00Z",
            ),
        ),
        artifacts=(
            ArtifactRef(
                kind="manifest",
                path="/tmp/manifest.json",
                size_bytes=123,
            ),
        ),
    )

    payload = progress.to_dict()
    round_trip = JobProgressSnapshot.from_dict(payload)

    assert payload["percent_complete"] == 25.0
    assert payload["rate_per_second"] == 0.25
    assert payload["unit"] == "children"
    assert round_trip == progress


def test_job_snapshot_round_trips_with_progress_and_controls() -> None:
    """Job snapshots should carry GUI-ready status and operation state."""
    progress = JobProgressSnapshot(
        workflow_name="HistDataRunWorkflow",
        request_id="run-control",
        status=WorkStatus.UNKNOWN,
        current_stage="BuildCacheWorkflow",
        total_children=2,
        completed_children=1,
    )

    snapshot = OrchestrationJobSnapshot.from_handle(_Handle()).with_progress(
        progress
    )
    payload = snapshot.to_dict()
    round_trip = OrchestrationJobSnapshot.from_dict(payload)

    assert payload["schema_version"] == 1
    assert payload["lifecycle"] == JobLifecycle.RUNNING.value
    assert payload["controls"]["cancel"]["available"] is True
    assert payload["controls"]["retry"]["available"] is False
    assert round_trip.workflow_id == "histdatacom-run-control"
    assert round_trip.progress == progress


def test_job_snapshot_carries_orchestration_runtime_log_paths() -> None:
    """Runtime log file paths should be visible to GUI clients."""
    orchestration_status = SimpleNamespace(
        state="running",
        message="ok",
        logs={
            "server": "/tmp/histdatacom/temporal-server.log",
            "worker": "/tmp/histdatacom/worker.log",
        },
    )

    snapshot = OrchestrationJobSnapshot.from_handle(
        _Handle(),
        orchestration_status=orchestration_status,
    )

    assert snapshot.orchestration_state == "running"
    assert snapshot.metadata["orchestration_logs"] == {
        "server": "/tmp/histdatacom/temporal-server.log",
        "worker": "/tmp/histdatacom/worker.log",
    }


def test_job_snapshot_represents_cancel_retry_resume_transitions() -> None:
    """Cancellation, retry, and resume intent should be explicit states."""
    base = OrchestrationJobSnapshot.from_handle(
        _Handle(),
        lifecycle=JobLifecycle.FAILED,
        status=WorkStatus.FAILED,
    )

    cancel_requested = base.request_cancel(reason="operator")
    retry_requested = base.request_retry(
        reason="transient network",
        stage="BuildCacheWorkflow",
    )
    retrying = retry_requested.mark_retrying()
    resume_requested = base.request_resume(
        reason="continue checkpoint",
        stage="DownloadArchivesWorkflow",
    )
    resuming = resume_requested.mark_resuming()

    assert cancel_requested.lifecycle == JobLifecycle.CANCEL_REQUESTED
    assert (
        cancel_requested.controls.cancel.phase
        == ControlOperationPhase.REQUESTED
    )
    assert cancel_requested.controls.cancel.reason == "operator"
    assert (
        cancel_requested.controls.cancel.metadata["cancellation"][
            "stops_future_work"
        ]
        is True
    )
    assert retry_requested.lifecycle == JobLifecycle.RETRY_REQUESTED
    assert (
        retry_requested.controls.retry.metadata["resume_policy"]["stage"]
        == "build_cache"
    )
    assert retrying.lifecycle == JobLifecycle.RETRYING
    assert resume_requested.lifecycle == JobLifecycle.RESUME_REQUESTED
    assert (
        resume_requested.controls.resume.metadata["resume_policy"]["stage"]
        == "download_archives"
    )
    assert resuming.lifecycle == JobLifecycle.RESUMING
