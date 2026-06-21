"""Tests for cancellation, cleanup, and resume policy helpers."""

from __future__ import annotations

from histdatacom.cancellation import (
    job_cancellation_metadata,
    operation_resume_policy,
)


def test_workflow_stage_aliases_resolve_resume_policy() -> None:
    """Workflow names should map to their operation resume contracts."""
    policy = operation_resume_policy("BuildCacheWorkflow")

    assert policy.stage == "build_cache"
    assert policy.partial_artifact_disposition.value == "remove_temp"
    assert policy.resume_mode.value == "reuse_complete_artifact"
    assert policy.retry_safe is True


def test_job_cancellation_metadata_documents_all_operation_policies() -> None:
    """Whole-job cancellation metadata should be GUI-ready and exhaustive."""
    metadata = job_cancellation_metadata("operator")
    stages = {policy["stage"] for policy in metadata["resume_policies"]}

    assert metadata["cancelled"] is True
    assert metadata["reason"] == "operator"
    assert metadata["stops_future_work"] is True
    assert {"download_archive", "build_cache", "import_to_influx"} <= stages
