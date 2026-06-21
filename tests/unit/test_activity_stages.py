"""Tests for queue-free stage functions."""

from __future__ import annotations

import os
import shutil
import zipfile
from pathlib import Path
from urllib.error import URLError

import requests

from histdatacom.activity_stages import (
    UrlPageData,
    build_cache_work_item,
    dataset_plan_stage,
    download_archive_work_item,
    extract_csv_work_item,
    fetch_histdata_page_data,
    import_to_influx_work_item,
    merge_cache_work_items,
    read_repository_data_file,
    repository_data_with_record,
    repository_refresh_stage,
    validate_url_work_item,
)
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    convert_polars_datetime_to_utc_ms,
    read_ascii_file_to_polars,
    write_polars_cache,
)
from histdatacom.records import Record
from histdatacom.runtime_contracts import WorkItem, WorkStatus, derive_work_id

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"
ASCII_M1_URL = (
    "http://www.histdata.com/download-free-forex-data/"
    "?/ascii/1-minute-bar-quotes/eurusd/2022"
)
EXPECTED_M1_DATETIMES = [1328072400000, 1328072460000, 1328072520000]
EXPECTED_M1_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
    "openbid=1.3066,highbid=1.3066,lowbid=1.30656,closebid=1.30656 "
    "1328072400000"
)


class _FakeResponse:
    """Tiny requests.Response stand-in for validation fetch tests."""

    def __init__(
        self,
        *,
        headers: dict[str, str],
        content: bytes,
    ) -> None:
        self.headers = headers
        self.content = content
        self.text = ""
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:
        """Match the requests response API."""


def _args(tmp_path: Path) -> dict[str, object]:
    """Return explicit stage args for tests."""
    return {
        "default_download_dir": f"{tmp_path}{os.sep}",
        "batch_size": "2",
        "delete_after_influx": False,
    }


def _m1_frame() -> object:
    """Return the normalized Polars M1 fixture frame."""
    raw = read_ascii_file_to_polars(
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
        "M1",
    )
    return convert_polars_datetime_to_utc_ms(raw, "M1")


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


def test_validate_url_work_item_returns_updated_item_without_queue(
    tmp_path: Path,
) -> None:
    """URL validation should return an updated work item and memento."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)

    def scrape(record_: Record) -> Record:
        record_.data_tk = "token"
        record_.data_date = "2022"
        record_.data_datemonth = "2022"
        record_.data_format = "ASCII"
        record_.data_timeframe = "M1"
        record_.data_fxpair = "eurusd"
        return record_

    output = validate_url_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        scrape_record_info=scrape,
        check_for_valid_download=lambda record_: None,
    )

    assert output.forward
    assert output.result.stage == "validate_url"
    assert output.result.status is WorkStatus.URL_VALID
    assert output.work_item.status is WorkStatus.URL_VALID
    assert output.work_item.data_tk == "token"
    assert record.status == WorkStatus.URL_NEW.value
    assert (Path(output.work_item.data_dir) / ".meta").exists()


def test_validate_url_work_item_parses_form_metadata(
    tmp_path: Path,
) -> None:
    """URL validation should parse form metadata without record callbacks."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)

    output = validate_url_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        fetch_page_data=lambda url, timeout: UrlPageData(
            html=_form_html(),
            encoding="gzip",
            bytes_length="123",
            headers={},
        ),
    )

    assert output.forward
    assert output.result.status is WorkStatus.URL_VALID
    assert output.work_item.status is WorkStatus.URL_VALID
    assert output.work_item.data_tk == "token"
    assert output.work_item.encoding == "gzip"
    assert output.work_item.bytes_length == "123"
    assert output.result.metrics["encoding"] == "gzip"


def test_validate_url_work_item_missing_data_does_not_forward(
    tmp_path: Path,
) -> None:
    """Missing HistData pages should become terminal explicit outputs."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)

    output = validate_url_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        scrape_record_info=lambda record_: record_,
        check_for_valid_download=lambda record_: (_ for _ in ()).throw(
            ValueError
        ),
    )

    assert not output.forward
    assert output.result.status is WorkStatus.URL_NO_REPO_DATA
    assert output.work_item.status is WorkStatus.URL_NO_REPO_DATA
    assert output.result.metrics["missing_repo_data"] is True
    assert (Path(output.work_item.data_dir) / ".meta").exists()


def test_validate_url_work_item_missing_token_is_no_data(
    tmp_path: Path,
) -> None:
    """Missing form tokens should be an explicit no-data result."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)

    output = validate_url_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        fetch_page_data=lambda url, timeout: UrlPageData(
            html=_form_html(token=""),
            encoding="gzip",
            bytes_length="123",
            headers={},
        ),
    )

    assert not output.forward
    assert output.result.status is WorkStatus.URL_NO_REPO_DATA
    assert output.work_item.status is WorkStatus.URL_NO_REPO_DATA
    assert output.result.failure is None
    assert output.result.metrics["missing_repo_data"] is True


def test_validate_url_work_item_malformed_headers_fail(
    tmp_path: Path,
) -> None:
    """Malformed fetch headers should be a structured failed result."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)

    def get(url: str, *, timeout: int) -> _FakeResponse:
        return _FakeResponse(headers={}, content=_form_html().encode())

    output = validate_url_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        fetch_page_data=lambda url, timeout: fetch_histdata_page_data(
            url,
            timeout,
            request_get=get,
        ),
    )

    assert not output.forward
    assert output.result.status is WorkStatus.FAILED
    assert output.work_item.status is WorkStatus.FAILED
    assert output.result.failure is not None
    assert output.result.failure.code == "MALFORMED_HEADERS"
    assert not output.result.failure.retryable


def test_validate_url_work_item_network_failure_is_retried(
    tmp_path: Path,
) -> None:
    """Network failures should be retryable validation outcomes."""
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)

    def get(url: str, *, timeout: int) -> _FakeResponse:
        raise requests.Timeout("connect timeout")

    output = validate_url_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        fetch_page_data=lambda url, timeout: fetch_histdata_page_data(
            url,
            timeout,
            request_get=get,
        ),
    )

    assert not output.forward
    assert output.result.status is WorkStatus.RETRIED
    assert output.work_item.status is WorkStatus.RETRIED
    assert output.result.failure is not None
    assert output.result.failure.code == "URL_FETCH_RETRYABLE"
    assert output.result.failure.retryable


def test_download_archive_work_item_returns_zip_artifact(
    tmp_path: Path,
) -> None:
    """Archive download should be callable with an injected downloader."""
    data_dir = tmp_path / "ASCII" / "M1" / "eurusd" / "2022"
    data_dir.mkdir(parents=True)
    record = Record(
        url=ASCII_M1_URL,
        status=WorkStatus.URL_VALID.value,
        data_dir=f"{data_dir}{os.sep}",
    )

    def download(record_: Record) -> None:
        record_.zip_filename = "DAT_ASCII_EURUSD_M1_2022.zip"
        Path(record_.data_dir, record_.zip_filename).write_bytes(b"zip-bytes")

    output = download_archive_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        download_file=download,
    )

    assert output.forward
    assert output.work_item.status is WorkStatus.CSV_ZIP
    assert output.work_item.zip_filename == "DAT_ASCII_EURUSD_M1_2022.zip"
    assert output.result.artifacts[0].kind == "zip"
    assert record.status == WorkStatus.URL_VALID.value


def test_extract_csv_work_item_extracts_data_member(tmp_path: Path) -> None:
    """CSV extraction should not require queue objects."""
    archive_path = tmp_path / "archive.zip"
    with zipfile.ZipFile(archive_path, "w") as archive:
        archive.writestr("DAT_ASCII_EURUSD_M1_2022.csv", b"rows")
    record = Record(
        data_dir=f"{tmp_path}{os.sep}",
        zip_filename=archive_path.name,
        status=WorkStatus.CSV_ZIP.value,
    )

    output = extract_csv_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
    )

    assert output.work_item.status is WorkStatus.CSV_FILE
    assert output.work_item.csv_filename == "DAT_ASCII_EURUSD_M1_2022.csv"
    assert (tmp_path / output.work_item.csv_filename).read_bytes() == b"rows"
    assert not archive_path.exists()
    assert record.status == WorkStatus.CSV_ZIP.value


def test_build_cache_work_item_writes_cache_from_csv(
    tmp_path: Path,
) -> None:
    """Cache build should return a cache-ready item and artifact metadata."""
    filename = "DAT_ASCII_EURUSD_M1_201202.csv"
    shutil.copyfile(FIXTURES / filename, tmp_path / filename)
    record = Record(
        data_dir=f"{tmp_path}{os.sep}",
        csv_filename=filename,
        zip_filename="missing.zip",
        data_format="ascii",
        data_timeframe="M1",
        data_fxpair="eurusd",
        status=WorkStatus.CSV_FILE.value,
    )

    output = build_cache_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
    )

    assert output.work_item.status is WorkStatus.CACHE_READY
    assert output.work_item.cache_filename == CACHE_FILENAME
    assert output.work_item.cache_line_count == "3"
    assert output.result.status is WorkStatus.CACHE_READY
    assert output.result.metrics["cache_created"] is True
    assert output.result.metrics["cache_line_count"] == 3
    assert output.result.artifacts[0].path == str(tmp_path / CACHE_FILENAME)


def test_merge_cache_work_items_uses_explicit_inputs(tmp_path: Path) -> None:
    """Cache merge should not read queue globals."""
    frame = _m1_frame()
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first_dir.mkdir()
    second_dir.mkdir()
    write_polars_cache(frame.slice(0, 1), first_dir / CACHE_FILENAME)
    write_polars_cache(frame.slice(1, 2), second_dir / CACHE_FILENAME)
    first = WorkItem.from_record(
        Record(
            data_dir=f"{first_dir}{os.sep}",
            cache_filename=CACHE_FILENAME,
            cache_start=str(EXPECTED_M1_DATETIMES[0]),
            data_fxpair="eurusd",
            data_timeframe="M1",
        )
    )
    second = WorkItem.from_record(
        Record(
            data_dir=f"{second_dir}{os.sep}",
            cache_filename=CACHE_FILENAME,
            cache_start=str(EXPECTED_M1_DATETIMES[1]),
            data_fxpair="eurusd",
            data_timeframe="M1",
        )
    )

    output = merge_cache_work_items([second, first], return_type="polars")

    assert output.result.status is WorkStatus.COMPLETED
    assert output.result.metrics["record_count"] == 2
    assert output.data.select("datetime").to_series().to_list() == (
        EXPECTED_M1_DATETIMES
    )


def test_import_to_influx_work_item_emits_batches_without_writer(
    tmp_path: Path,
) -> None:
    """Influx import should expose line batches without a live client."""
    write_polars_cache(_m1_frame(), tmp_path / CACHE_FILENAME)
    record = Record(
        data_dir=f"{tmp_path}{os.sep}",
        cache_filename=CACHE_FILENAME,
        data_format="ascii",
        data_timeframe="M1",
        data_fxpair="eurusd",
        status=WorkStatus.CACHE_READY.value,
    )
    emitted: list[list[str]] = []

    output = import_to_influx_work_item(
        WorkItem.from_record(record),
        args=_args(tmp_path),
        emit_lines=emitted.append,
    )

    assert output.work_item.status is WorkStatus.INFLUX_UPLOAD
    assert output.result.status is WorkStatus.INFLUX_UPLOAD
    assert output.result.metrics == {"batch_count": 2, "line_count": 3}
    assert [len(batch) for batch in emitted] == [2, 1]
    assert emitted[0][0] == EXPECTED_M1_LINE


def test_dataset_plan_stage_emits_stable_historical_m1_work_item(
    tmp_path: Path,
) -> None:
    """Historical M1 ranges should plan yearly HistData archive units."""
    output = dataset_plan_stage(
        start_yearmonth="202201",
        end_yearmonth="202203",
        formats=("ascii",),
        pairs=("eurusd",),
        timeframes=("M1",),
        default_download_dir=f"{tmp_path}{os.sep}",
        current_yearmonth="202606",
    )

    assert output.result.status is WorkStatus.COMPLETED
    assert output.result.metrics["work_item_count"] == 1
    assert len(output.work_items) == 1
    [item] = output.work_items
    assert item.work_id == derive_work_id(
        "dataset_plan",
        "ascii",
        "M1",
        "eurusd",
        "2022",
        "",
    )
    assert item.status is WorkStatus.URL_NEW
    assert item.url == (
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/1-minute-bar-quotes/eurusd/2022"
    )
    assert item.data_datemonth == "2022"
    assert (
        item.data_dir
        == f"{tmp_path}{os.sep}ASCII{os.sep}M1{os.sep}eurusd{os.sep}2022{os.sep}"
    )


def test_dataset_plan_stage_preserves_current_year_m1_monthly_edge(
    tmp_path: Path,
) -> None:
    """Current-year M1 data should plan monthly URLs like legacy code."""
    output = dataset_plan_stage(
        start_yearmonth="202401",
        end_yearmonth="202403",
        formats=("ascii",),
        pairs=("eurusd",),
        timeframes=("M1",),
        default_download_dir=f"{tmp_path}{os.sep}",
        current_yearmonth="202403",
    )

    assert [
        item.url.rsplit("/", maxsplit=2)[-2:] for item in output.work_items
    ] == [
        ["2024", "1"],
        ["2024", "2"],
        ["2024", "3"],
    ]
    assert [item.data_datemonth for item in output.work_items] == [
        "202401",
        "202402",
        "202403",
    ]


def test_dataset_plan_stage_preserves_tick_monthly_behavior() -> None:
    """Tick data should plan one work item per month."""
    output = dataset_plan_stage(
        start_yearmonth="202201",
        end_yearmonth="202203",
        formats=("ascii",),
        pairs=("eurusd",),
        timeframes=("T",),
        current_yearmonth="202606",
    )

    assert [item.data_datemonth for item in output.work_items] == [
        "202201",
        "202202",
        "202203",
    ]
    assert [item.url for item in output.work_items] == [
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/1",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/2",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/3",
    ]


def test_dataset_plan_stage_is_deterministic_for_sets_and_generators() -> None:
    """Plan output order and IDs should not depend on input container order."""
    first = dataset_plan_stage(
        start_yearmonth="202201",
        end_yearmonth="202201",
        formats={"ascii"},
        pairs={"gbpusd", "eurusd"},
        timeframes={"T", "M1"},
        current_yearmonth="202606",
    )
    second = dataset_plan_stage(
        start_yearmonth="202201",
        end_yearmonth="202201",
        formats=(item for item in ("ascii",)),
        pairs=(item for item in ("eurusd", "gbpusd")),
        timeframes=(item for item in ("M1", "T")),
        current_yearmonth="202606",
    )

    assert [item.work_id for item in first.work_items] == [
        item.work_id for item in second.work_items
    ]
    assert [item.url for item in first.work_items] == [
        item.url for item in second.work_items
    ]


def test_repository_refresh_stage_writes_artifact_and_available_data(
    tmp_path: Path,
) -> None:
    """Repository refresh should use explicit data and artifact results."""
    remote_repo = {
        "eurusd": {"start": "200005", "end": "202212"},
        "gbpusd": {"start": "200005", "end": "202212"},
        "hash": "remote",
        "hash_utc": 10.0,
    }

    output = repository_refresh_stage(
        repo_data={},
        repo_file_exists=False,
        repo_local_path=tmp_path / ".repo",
        pairs=("eurusd",),
        by="pair_asc",
        available_remote_data=True,
        fetch_remote_repository=lambda url: remote_repo,
    )

    assert output.result.status is WorkStatus.COMPLETED
    assert output.available_data == {
        "eurusd": {"start": "200005", "end": "202212"}
    }
    assert output.filter_pairs == ()
    assert output.repo_file_exists is True
    assert output.result.artifacts[0].kind == "repository"
    assert output.result.metrics["available_data"] == output.available_data
    written = read_repository_data_file(tmp_path / ".repo")
    assert written["eurusd"] == {"start": "200005", "end": "202212"}
    assert "hash" in written
    assert "hash_utc" in written


def test_repository_refresh_stage_network_error_is_structured_failure(
    tmp_path: Path,
) -> None:
    """Network failures should be activity failures with retry metadata."""
    local_repo = {"eurusd": {"start": "200005", "end": "202212"}}

    def fail_fetch(url: str) -> dict:
        raise URLError("offline")

    output = repository_refresh_stage(
        repo_data=local_repo,
        repo_file_exists=True,
        repo_local_path=tmp_path / ".repo",
        pairs=("eurusd",),
        available_remote_data=True,
        fetch_remote_repository=fail_fetch,
    )

    assert output.result.status is WorkStatus.FAILED
    assert output.result.failure is not None
    assert output.result.failure.code == "REPOSITORY_NETWORK_ERROR"
    assert output.result.failure.retryable is True
    assert output.available_data == local_repo
    assert output.result.metrics["available_data"] == local_repo


def test_repository_refresh_stage_accepts_one_shot_pair_iterable(
    tmp_path: Path,
) -> None:
    """Pair filtering and work IDs should not depend on iterable reuse."""
    local_repo = {
        "eurusd": {"start": "200005", "end": "202212"},
        "gbpusd": {"start": "200005", "end": "202212"},
    }
    pairs = (pair for pair in ("eurusd",))

    output = repository_refresh_stage(
        repo_data=local_repo,
        repo_file_exists=True,
        repo_local_path=tmp_path / ".repo",
        pairs=pairs,
        by="pair_asc",
    )

    assert output.available_data == {
        "eurusd": {"start": "200005", "end": "202212"}
    }
    assert output.result.metrics["filter_pairs"] == []


def test_repository_data_with_record_updates_ranges_without_globals() -> None:
    """Repository range updates should work from explicit inputs."""
    existing = {"eurusd": {"start": "202201", "end": "202201"}}
    record = Record(
        data_fxpair="EURUSD",
        data_datemonth="202212",
    )

    updated = repository_data_with_record(existing, record)

    assert updated == {"eurusd": {"start": "202201", "end": "202212"}}
    assert existing == {"eurusd": {"start": "202201", "end": "202201"}}
