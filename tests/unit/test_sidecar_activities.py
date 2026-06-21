"""Tests for Temporal sidecar activity wrappers."""

from __future__ import annotations

from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.activities import (
    dataset_plan_activity,
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


def test_dataset_plan_activity_returns_explicit_work_items(tmp_path) -> None:
    """Dataset planning should be callable as a registered activity."""
    request = RunRequest(
        request_id="run-plan",
        pairs=("eurusd",),
        formats=("ascii",),
        timeframes=("M1",),
        start_yearmonth="202201",
        end_yearmonth="202203",
        data_directory=str(tmp_path),
    )

    result = dataset_plan_activity({"request": request.to_dict()})

    assert result["result"]["stage"] == "dataset_plan"
    assert result["result"]["status"] == WorkStatus.COMPLETED.value
    assert result["result"]["metrics"]["work_item_count"] == 1
    assert len(result["work_items"]) == 1
    assert result["work_items"][0]["url"] == (
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/1-minute-bar-quotes/eurusd/2022"
    )
    assert result["work_items"][0]["data_datemonth"] == "2022"


def test_default_activities_register_operation_activities() -> None:
    """The worker default activity set should include migrated activities."""
    assert default_activities() == (
        repository_refresh_activity,
        dataset_plan_activity,
    )
