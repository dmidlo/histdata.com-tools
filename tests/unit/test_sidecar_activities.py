"""Tests for Temporal sidecar activity wrappers."""

from __future__ import annotations

from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.activities import (
    default_activities,
    repository_refresh_activity,
)


def test_repository_refresh_activity_returns_stage_result(
    monkeypatch,
    tmp_path,
) -> None:
    """Repository refresh should be callable as a registered activity."""
    remote_repo = {
        "eurusd": {"start": "200005", "end": "202212"},
        "hash": "remote",
        "hash_utc": 10.0,
    }
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_repository_data_from_url",
        lambda url: remote_repo,
    )
    request = RunRequest(
        request_id="run-repo",
        pairs=("eurusd",),
        data_directory=str(tmp_path),
        available_remote_data=True,
    )

    result = repository_refresh_activity({"request": request.to_dict()})

    assert result["stage"] == "repository_refresh"
    assert result["status"] == WorkStatus.COMPLETED.value
    assert result["metrics"]["available_data"] == {
        "eurusd": {"start": "200005", "end": "202212"}
    }
    assert result["artifacts"][0]["kind"] == "repository"


def test_default_activities_register_repository_refresh() -> None:
    """The worker default activity set should include repo refresh."""
    assert default_activities() == (repository_refresh_activity,)
