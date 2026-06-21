"""Tests for Temporal sidecar activity wrappers."""

from __future__ import annotations

from histdatacom.activity_stages import UrlValidationError
from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.activities import (
    dataset_plan_activity,
    default_activities,
    repository_refresh_activity,
    validate_urls_activity,
)


def _form_html(*, token: str = "token") -> str:
    """Return a minimal HistData download form."""
    return f"""
    <html>
      <form id="file_down">
        <input id="tk" value="{token}">
        <input id="date" value="2022">
        <input id="datemonth" value="2022">
        <input id="platform" value="ASCII">
        <input id="timeframe" value="M1">
        <input id="fxpair" value="eurusd">
      </form>
    </html>
    """


def _validation_payload(tmp_path) -> dict:
    """Return a minimal validation activity payload."""
    request = RunRequest(
        request_id="run-validate",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-validation",
            "status": WorkStatus.URL_NEW.value,
            "url": (
                "http://www.histdata.com/download-free-forex-data/"
                "?/ascii/1-minute-bar-quotes/eurusd/2022"
            ),
        },
    }


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


def test_validate_urls_activity_returns_form_metadata(
    monkeypatch,
    tmp_path,
) -> None:
    """URL validation should be callable as a sidecar activity."""
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        lambda url, timeout: {
            "html": _form_html(),
            "encoding": "gzip",
            "bytes_length": "123",
        },
    )
    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["result"]["stage"] == "validate_url"
    assert result["result"]["status"] == WorkStatus.URL_VALID.value
    assert result["work_item"]["data_tk"] == "token"
    assert result["work_item"]["encoding"] == "gzip"


def test_validate_urls_activity_returns_no_data(monkeypatch, tmp_path) -> None:
    """Missing tokens should flow through the registered activity."""
    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        lambda url, timeout: {
            "html": _form_html(token=""),
            "encoding": "gzip",
            "bytes_length": "123",
        },
    )

    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["result"]["status"] == WorkStatus.URL_NO_REPO_DATA.value
    assert result["result"]["failure"] is None
    assert not result["forward"]


def test_validate_urls_activity_returns_failed(monkeypatch, tmp_path) -> None:
    """Malformed validation failures should flow through the activity."""

    def fetch(url: str, timeout: int):
        raise UrlValidationError("MALFORMED_HEADERS", "missing length")

    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        fetch,
    )

    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["result"]["status"] == WorkStatus.FAILED.value
    assert result["result"]["failure"]["code"] == "MALFORMED_HEADERS"
    assert not result["result"]["failure"]["retryable"]


def test_validate_urls_activity_returns_retried(monkeypatch, tmp_path) -> None:
    """Retryable validation failures should flow through the activity."""

    def fetch(url: str, timeout: int):
        raise UrlValidationError(
            "URL_FETCH_RETRYABLE",
            "timeout",
            retryable=True,
        )

    monkeypatch.setattr(
        "histdatacom.activity_stages.fetch_histdata_page_data",
        fetch,
    )

    result = validate_urls_activity(_validation_payload(tmp_path))

    assert result["result"]["status"] == WorkStatus.RETRIED.value
    assert result["result"]["failure"]["code"] == "URL_FETCH_RETRYABLE"
    assert result["result"]["failure"]["retryable"]


def test_default_activities_register_operation_activities() -> None:
    """The worker default activity set should include migrated activities."""
    assert default_activities() == (
        repository_refresh_activity,
        dataset_plan_activity,
        validate_urls_activity,
    )
