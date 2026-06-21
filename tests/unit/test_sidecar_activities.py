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
    import_to_influx_activity,
    merge_cache_activity,
    repository_refresh_activity,
    validate_urls_activity,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"
EXPECTED_M1_DATETIMES = [1328072400000, 1328072460000, 1328072520000]
EXPECTED_M1_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
    "openbid=1.3066,highbid=1.3066,lowbid=1.30656,closebid=1.30656 "
    "1328072400000"
)
EXPECTED_TICK_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=T "
    "bidquote=1.3066,askquote=1.30677 1328072403660"
)


class FakeInfluxWriter:
    """Context-managed Influx writer test double."""

    instances: list["FakeInfluxWriter"] = []
    fail_with: Exception | None = None

    def __init__(self, args: dict) -> None:
        self.args = dict(args)
        self.batches: list[list[str]] = []
        self.closed = False
        self.instances.append(self)

    def __enter__(self) -> "FakeInfluxWriter":
        return self

    def __exit__(self, *args: object) -> None:
        self.closed = True

    def write_lines(self, lines: list[str]) -> None:
        if self.fail_with is not None:
            raise self.fail_with
        self.batches.append(list(lines))


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


def _influx_payload(
    tmp_path,
    *,
    timeframe: str = "M1",
    batch_size: str = "2",
    delete_after_influx: bool = False,
) -> dict:
    """Return an Influx activity payload with a cache artifact."""
    filename = f"DAT_ASCII_EURUSD_{timeframe}_201202.csv"
    source = convert_polars_datetime_to_utc_ms(
        read_ascii_file_to_polars(FIXTURES / filename, timeframe),
        timeframe,
    )
    write_polars_cache(source, tmp_path / CACHE_FILENAME)
    zip_filename = f"DAT_ASCII_EURUSD_{timeframe}_201202.zip"
    if delete_after_influx:
        (tmp_path / zip_filename).write_bytes(_zip_bytes())
    request = RunRequest(
        request_id=f"run-influx-{timeframe}",
        data_directory=str(tmp_path),
        batch_size=batch_size,
        import_to_influxdb=True,
        delete_after_influx=delete_after_influx,
    )
    return {
        "request": request.to_dict(),
        "work_item": {
            "work_id": f"work-influx-{timeframe}",
            "status": WorkStatus.CACHE_READY.value,
            "data_dir": f"{tmp_path}/",
            "cache_filename": CACHE_FILENAME,
            "cache_line_count": "3",
            "cache_start": str(EXPECTED_M1_DATETIMES[0]),
            "cache_end": str(EXPECTED_M1_DATETIMES[-1]),
            "zip_filename": zip_filename,
            "data_format": "ascii",
            "data_timeframe": timeframe,
            "data_fxpair": "eurusd",
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


def test_import_to_influx_activity_writes_m1_batches(
    monkeypatch,
    tmp_path,
) -> None:
    """Influx activity should write bounded M1 line-protocol batches."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = None
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )

    result = import_to_influx_activity(_influx_payload(tmp_path))

    [writer] = FakeInfluxWriter.instances
    assert writer.args["batch_size"] == "2"
    assert [len(batch) for batch in writer.batches] == [2, 1]
    assert writer.batches[0][0] == EXPECTED_M1_LINE
    assert writer.closed
    assert result["work_item"]["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert result["result"]["stage"] == "import_to_influx"
    assert result["result"]["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert result["result"]["metrics"]["batch_count"] == 2
    assert result["result"]["metrics"]["line_count"] == 3
    assert result["result"]["metrics"]["heartbeat_count"] == 2
    assert "data" not in result


def test_import_to_influx_activity_writes_tick_batches(
    monkeypatch,
    tmp_path,
) -> None:
    """Influx activity should preserve tick bid/ask line formatting."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = None
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )

    result = import_to_influx_activity(
        _influx_payload(tmp_path, timeframe="T", batch_size="2")
    )

    [writer] = FakeInfluxWriter.instances
    assert [len(batch) for batch in writer.batches] == [2, 1]
    assert writer.batches[0][0] == EXPECTED_TICK_LINE
    assert result["result"]["metrics"]["line_count"] == 3


def test_import_to_influx_activity_reports_retryable_write_failure(
    monkeypatch,
    tmp_path,
) -> None:
    """Writer failures should be explicit and retry-aware."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = OSError("temporary influx failure")
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )

    result = import_to_influx_activity(_influx_payload(tmp_path))

    assert result["work_item"]["status"] == WorkStatus.RETRIED.value
    assert result["result"]["status"] == WorkStatus.RETRIED.value
    assert result["result"]["failure"]["code"] == "INFLUX_IMPORT_RETRYABLE"
    assert result["result"]["failure"]["retryable"] is True
    assert result["result"]["failure"]["detail"]["idempotent_retry"] is True
    FakeInfluxWriter.fail_with = None


def test_import_to_influx_activity_reports_optional_dependency_failure(
    monkeypatch,
    tmp_path,
) -> None:
    """Missing Influx extra should remain explicit under the activity path."""

    def missing_influx_writer(args: dict) -> FakeInfluxWriter:  # noqa: ARG001
        raise SystemExit(
            "InfluxDB import not installed. please run:\n\n  "
            "pip install histdatacom[influx]"
        )

    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        missing_influx_writer,
    )

    result = import_to_influx_activity(_influx_payload(tmp_path))

    assert result["work_item"]["status"] == WorkStatus.FAILED.value
    assert result["result"]["failure"]["code"] == (
        "INFLUX_OPTIONAL_DEPENDENCY_MISSING"
    )
    assert "histdatacom[influx]" in result["result"]["failure"]["message"]


def test_import_to_influx_activity_honors_delete_after_influx(
    monkeypatch,
    tmp_path,
) -> None:
    """Successful imports should preserve existing cleanup behavior."""
    FakeInfluxWriter.instances.clear()
    FakeInfluxWriter.fail_with = None
    monkeypatch.setattr(
        "histdatacom.sidecar.activities._influx_batch_writer",
        FakeInfluxWriter,
    )
    payload = _influx_payload(tmp_path, delete_after_influx=True)
    zip_path = tmp_path / payload["work_item"]["zip_filename"]
    cache_path = tmp_path / CACHE_FILENAME

    result = import_to_influx_activity(payload)

    assert result["result"]["status"] == WorkStatus.INFLUX_UPLOAD.value
    assert not zip_path.exists()
    assert not cache_path.exists()


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
        import_to_influx_activity,
    )
