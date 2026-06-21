"""Tests for sidecar runtime contracts."""

from __future__ import annotations

import json

from histdatacom.options import Options
from histdatacom.records import Record
from histdatacom.runtime_contracts import (
    ArtifactRef,
    FailureInfo,
    RunRequest,
    StageResult,
    StatusEvent,
    WorkItem,
    WorkStatus,
    derive_work_id,
    status_has_csv_artifact,
)

ASCII_M1_URL = (
    "http://www.histdata.com/download-free-forex-data/"
    "?/ascii/1-minute-bar-quotes/eurusd/2022"
)


def test_work_status_normalizes_legacy_and_future_states() -> None:
    """Legacy Record statuses should map into stable sidecar values."""
    assert WorkStatus.from_value("") is WorkStatus.PLANNED
    assert WorkStatus.from_value("URL_VALID") is WorkStatus.URL_VALID
    assert WorkStatus.from_value("url_valid") is WorkStatus.URL_VALID
    assert WorkStatus.from_value("CSV") is WorkStatus.CSV_FILE
    assert WorkStatus.from_value("MISSING") is WorkStatus.URL_NO_REPO_DATA
    assert WorkStatus.from_value("FAILED") is WorkStatus.FAILED
    assert WorkStatus.from_value("RETRIED") is WorkStatus.RETRIED
    assert WorkStatus.from_value("SKIPPED") is WorkStatus.SKIPPED
    assert WorkStatus.from_value("CANCELLED") is WorkStatus.CANCELLED
    assert WorkStatus.from_value("CANCELED") is WorkStatus.CANCELLED
    assert (
        WorkStatus.from_value("WORKFLOW_EXECUTION_STATUS_CANCELED")
        is WorkStatus.CANCELLED
    )
    assert WorkStatus.from_value("TERMINATED") is WorkStatus.FAILED
    assert WorkStatus.from_value("TIMED_OUT") is WorkStatus.FAILED
    assert (
        WorkStatus.from_value("WORKFLOW_EXECUTION_STATUS_TIMED_OUT")
        is WorkStatus.FAILED
    )
    assert WorkStatus.from_value("COMPLETED") is WorkStatus.COMPLETED
    assert WorkStatus.from_value("unexpected") is WorkStatus.UNKNOWN
    assert WorkStatus.COMPLETED.terminal
    assert not WorkStatus.URL_VALID.terminal
    assert status_has_csv_artifact("CSV")
    assert status_has_csv_artifact(WorkStatus.CSV_ZIP)
    assert status_has_csv_artifact("csv_file")
    assert not status_has_csv_artifact(WorkStatus.URL_VALID)


def test_work_item_round_trips_legacy_record_shape() -> None:
    """WorkItem should preserve current Record metadata for compatibility."""
    record = Record(
        url=ASCII_M1_URL,
        status="CSV_FILE",
        encoding="gzip",
        bytes_length="1024",
        data_date="2022",
        data_year="2022",
        data_month="",
        data_datemonth="2022",
        data_format="ASCII",
        data_timeframe="M1",
        data_fxpair="eurusd",
        data_dir="/tmp/histdatacom/ASCII/M1/eurusd/2022/",
        data_tk="token",
        zip_filename="DAT_ASCII_EURUSD_M1_2022.zip",
        csv_filename="DAT_ASCII_EURUSD_M1_2022.csv",
        cache_filename=".data",
        cache_line_count="100",
        cache_start="1643673600000",
        cache_end="1675209540000",
        zip_persist="False",
    )

    item = WorkItem.from_record(record)
    restored = WorkItem.from_dict(json.loads(json.dumps(item.to_dict())))

    assert item.work_id == derive_work_id(
        ASCII_M1_URL,
        "ASCII",
        "M1",
        "eurusd",
        "2022",
    )
    assert restored == item
    assert restored.to_record_kwargs()["status"] == "CSV_FILE"
    assert restored.to_record_kwargs()["zip_filename"] == record.zip_filename


def test_work_item_preserves_unknown_legacy_status_text() -> None:
    """Unknown legacy strings should not be discarded during migration."""
    record = Record(url=ASCII_M1_URL, status="CUSTOM_STATUS")

    item = WorkItem.from_record(record)

    assert item.status is WorkStatus.UNKNOWN
    assert item.status_text == "CUSTOM_STATUS"
    assert item.legacy_status == "CUSTOM_STATUS"
    assert item.with_status(WorkStatus.SKIPPED).legacy_status == "SKIPPED"


def test_run_request_round_trips_options_payload() -> None:
    """Options should convert into JSON-safe sidecar run requests."""
    options = Options()
    options.pairs = {"eurusd", "gbpusd"}
    options.formats = {"ascii"}
    options.timeframes = {"M1", "T"}
    options.start_yearmonth = "2020-01"
    options.end_yearmonth = "2020-02"
    options.data_directory = "~/histdata"
    options.api_return_type = "polars"
    options.cpu_utilization = "high"
    options.batch_size = 2500
    options.by = "start_dsc"
    options.validate_urls = True
    options.download_data_archives = True
    options.extract_csvs = True

    request = RunRequest.from_options(options, request_id="run-test")
    restored = RunRequest.from_dict(json.loads(json.dumps(request.to_dict())))

    assert restored == request
    assert restored.request_id == "run-test"
    assert restored.pairs == ("eurusd", "gbpusd")
    assert restored.timeframes == ("M1", "T")
    assert restored.batch_size == "2500"
    assert restored.metadata["repo_sort"] == "start_dsc"
    assert restored.validate_urls
    assert restored.download_data_archives
    assert restored.extract_csvs


def test_stage_result_round_trips_artifacts_events_and_failure() -> None:
    """Activity results should remain JSON-compatible and structured."""
    result = StageResult(
        work_id="work-1",
        stage="download",
        status=WorkStatus.RETRIED,
        artifacts=(
            ArtifactRef(
                kind="zip",
                path="/tmp/file.zip",
                size_bytes=512,
                sha256="abc",
                metadata={"source": "histdata"},
            ),
        ),
        events=(
            StatusEvent(
                status=WorkStatus.RETRIED,
                stage="download",
                message="retrying after timeout",
                work_id="work-1",
                timestamp_utc="2026-06-21T00:00:00Z",
                metadata={"attempt": 2},
            ),
        ),
        failure=FailureInfo(
            code="HTTP_TIMEOUT",
            message="request timed out",
            retryable=True,
            detail={"timeout_seconds": 10},
        ),
        metrics={"attempts": 2},
    )

    restored = StageResult.from_dict(json.loads(json.dumps(result.to_dict())))

    assert restored == result
    assert restored.failure is not None
    assert restored.failure.retryable
    assert restored.artifacts[0].metadata["source"] == "histdata"
