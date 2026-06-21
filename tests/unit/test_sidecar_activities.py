"""Tests for Temporal sidecar activity wrappers."""

from __future__ import annotations

import io
import shutil
import zipfile
from pathlib import Path

from histdatacom.activity_stages import UrlValidationError
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    convert_polars_datetime_to_utc_ms,
    read_ascii_file_to_polars,
    write_polars_cache,
)
from histdatacom.runtime_contracts import RunRequest, WorkStatus
from histdatacom.sidecar.activities import (
    build_cache_activity,
    dataset_plan_activity,
    default_activities,
    download_archives_activity,
    extract_csv_activity,
    merge_cache_activity,
    repository_refresh_activity,
    validate_urls_activity,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"
EXPECTED_M1_DATETIMES = [1328072400000, 1328072460000, 1328072520000]


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


def _zip_bytes() -> bytes:
    """Return a minimal valid ZIP payload."""
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("DAT_ASCII_EURUSD_M1_2022.csv", "rows")
    return stream.getvalue()


def _download_payload(tmp_path) -> dict:
    """Return a minimal download activity payload with existing ZIP."""
    zip_path = tmp_path / "DAT_ASCII_EURUSD_M1_2022.zip"
    zip_path.write_bytes(_zip_bytes())
    request = RunRequest(
        request_id="run-download",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-download",
            "status": WorkStatus.URL_VALID.value,
            "url": (
                "http://www.histdata.com/download-free-forex-data/"
                "?/ascii/1-minute-bar-quotes/eurusd/2022"
            ),
            "data_dir": f"{tmp_path}/",
            "zip_filename": zip_path.name,
        },
    }


def _extraction_payload(tmp_path) -> dict:
    """Return a minimal extraction activity payload with existing ZIP."""
    zip_path = tmp_path / "DAT_ASCII_EURUSD_M1_2022.zip"
    zip_path.write_bytes(_zip_bytes())
    request = RunRequest(
        request_id="run-extract",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-extract",
            "status": WorkStatus.CSV_ZIP.value,
            "data_dir": f"{tmp_path}/",
            "zip_filename": zip_path.name,
        },
    }


def _cache_payload(tmp_path) -> dict:
    """Return a minimal cache activity payload with an existing CSV."""
    filename = "DAT_ASCII_EURUSD_M1_201202.csv"
    shutil.copyfile(FIXTURES / filename, tmp_path / filename)
    request = RunRequest(
        request_id="run-cache",
        data_directory=str(tmp_path),
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": "work-cache",
            "status": WorkStatus.CSV_FILE.value,
            "data_dir": f"{tmp_path}/",
            "csv_filename": filename,
            "zip_filename": "missing.zip",
            "data_format": "ascii",
            "data_timeframe": "M1",
            "data_fxpair": "eurusd",
        },
    }


def _merge_payload(tmp_path) -> dict:
    """Return a merge activity payload with two cache artifacts."""
    source = convert_polars_datetime_to_utc_ms(
        read_ascii_file_to_polars(
            FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
            "M1",
        ),
        "M1",
    )
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    write_polars_cache(source.slice(0, 1), first_dir / CACHE_FILENAME)
    write_polars_cache(source.slice(1, 2), second_dir / CACHE_FILENAME)
    request = RunRequest(
        request_id="run-merge",
        data_directory=str(tmp_path),
        api_return_type="polars",
    )
    return {
        "request": request.to_dict(),
        "work_items": [
            {
                "work_id": "work-second",
                "status": WorkStatus.CACHE_READY.value,
                "data_dir": f"{second_dir}/",
                "cache_filename": CACHE_FILENAME,
                "cache_line_count": "2",
                "cache_start": str(EXPECTED_M1_DATETIMES[1]),
                "cache_end": str(EXPECTED_M1_DATETIMES[2]),
                "data_format": "ascii",
                "data_timeframe": "M1",
                "data_fxpair": "eurusd",
            },
            {
                "work_id": "work-first",
                "status": WorkStatus.CACHE_READY.value,
                "data_dir": f"{first_dir}/",
                "cache_filename": CACHE_FILENAME,
                "cache_line_count": "1",
                "cache_start": str(EXPECTED_M1_DATETIMES[0]),
                "cache_end": str(EXPECTED_M1_DATETIMES[0]),
                "data_format": "ascii",
                "data_timeframe": "M1",
                "data_fxpair": "eurusd",
            },
        ],
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


def test_download_archives_activity_reuses_existing_zip(tmp_path) -> None:
    """Archive downloads should be callable as a registered activity."""
    result = download_archives_activity(_download_payload(tmp_path))

    assert result["result"]["stage"] == "download_archive"
    assert result["result"]["status"] == WorkStatus.CSV_ZIP.value
    assert result["result"]["metrics"]["decision"] == "reuse_existing"
    assert result["work_item"]["status"] == WorkStatus.CSV_ZIP.value
    assert result["result"]["artifacts"][0]["kind"] == "zip"
    assert result["result"]["artifacts"][0]["sha256"]


def test_extract_csv_activity_extracts_existing_zip(tmp_path) -> None:
    """Archive extraction should be callable as a registered activity."""
    result = extract_csv_activity(_extraction_payload(tmp_path))

    assert result["result"]["stage"] == "extract_csv"
    assert result["result"]["status"] == WorkStatus.CSV_FILE.value
    assert result["result"]["metrics"]["decision"] == "extracted"
    assert result["result"]["metrics"]["zip_deleted"] is True
    assert result["work_item"]["status"] == WorkStatus.CSV_FILE.value
    assert result["work_item"]["csv_filename"] == (
        "DAT_ASCII_EURUSD_M1_2022.csv"
    )
    assert result["result"]["artifacts"][0]["kind"] == "csv"
    assert result["result"]["artifacts"][0]["sha256"]


def test_build_cache_activity_builds_polars_cache(tmp_path) -> None:
    """Polars cache builds should be callable as a registered activity."""
    result = build_cache_activity(_cache_payload(tmp_path))

    assert result["result"]["stage"] == "build_cache"
    assert result["result"]["status"] == WorkStatus.CACHE_READY.value
    assert result["result"]["metrics"]["decision"] == "built"
    assert result["result"]["metrics"]["cache_line_count"] == 3
    assert result["result"]["metrics"]["cache_start"] == str(
        EXPECTED_M1_DATETIMES[0]
    )
    assert result["result"]["metrics"]["cache_end"] == str(
        EXPECTED_M1_DATETIMES[-1]
    )
    assert result["result"]["metrics"]["timeframe"] == "M1"
    assert result["result"]["artifacts"][0]["kind"] == "cache"
    assert result["work_item"]["status"] == WorkStatus.CACHE_READY.value
    assert result["work_item"]["cache_filename"] == CACHE_FILENAME
    assert result["work_item"]["cache_line_count"] == "3"
    assert (tmp_path / CACHE_FILENAME).exists()


def test_merge_cache_activity_returns_bounded_merge_metadata(tmp_path) -> None:
    """Cache merge should return summaries and refs, not frame payloads."""
    result = merge_cache_activity(_merge_payload(tmp_path))

    assert result["result"]["stage"] == "merge_cache"
    assert result["result"]["status"] == WorkStatus.COMPLETED.value
    assert result["result"]["metrics"]["record_count"] == 2
    assert result["result"]["metrics"]["set_count"] == 1
    assert result["result"]["metrics"]["materialized"] is False
    assert "data" not in result
    merge_set = result["merge_sets"][0]
    assert merge_set["timeframe"] == "M1"
    assert merge_set["pair"] == "eurusd"
    assert merge_set["line_count"] == 3
    assert merge_set["start"] == str(EXPECTED_M1_DATETIMES[0])
    assert merge_set["end"] == str(EXPECTED_M1_DATETIMES[2])
    assert [artifact["kind"] for artifact in merge_set["artifacts"]] == [
        "cache",
        "cache",
    ]


def test_default_activities_register_operation_activities() -> None:
    """The worker default activity set should include migrated activities."""
    assert default_activities() == (
        repository_refresh_activity,
        dataset_plan_activity,
        validate_urls_activity,
        download_archives_activity,
        extract_csv_activity,
        build_cache_activity,
        merge_cache_activity,
    )
