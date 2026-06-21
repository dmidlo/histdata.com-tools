"""Pytest unit tests for histdatacom.scraper.scraper.py."""

from __future__ import annotations

import json
import os
import zipfile
from pathlib import Path

from histdatacom import config
from histdatacom.records import Record, Records
from histdatacom.runtime_contracts import WorkStatus
from histdatacom.scraper.scraper import Scraper
from histdatacom.scraper.urls import Urls

ASCII_M1_URL = (
    "http://www.histdata.com/download-free-forex-data/"
    "?/ascii/1-minute-bar-quotes/eurusd/2022"
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


def test_scraper() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def _scraper_without_init() -> Scraper:
    """Return a Scraper instance without initializing repo/url collaborators."""
    scraper = object.__new__(Scraper)
    scraper.check_if_queue_is_needed = lambda: False
    scraper.check_for_repo_action = lambda: True
    scraper.set_repo_datum = lambda record: None
    return scraper


def _configure_stage_queues(record: Record, tmp_path: Path) -> None:
    """Install isolated global queues for current scheduler characterization."""
    config.CURRENT_QUEUE = Records()
    config.NEXT_QUEUE = Records()
    config.CURRENT_QUEUE.put(record)
    config.ARGS = {
        "default_download_dir": f"{tmp_path}{os.sep}",
        "from_api": False,
    }


def test_validate_url_transitions_new_record_to_valid_and_writes_meta(
    tmp_path: Path,
) -> None:
    """Document validate stage success behavior before workflow migration."""
    scraper = _scraper_without_init()
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)
    _configure_stage_queues(record, tmp_path)

    scraper._get_page_data = lambda url, timeout: {  # type: ignore[method-assign]
        "html": _form_html(),
        "encoding": "gzip",
        "bytes_length": "123",
    }

    scraper._validate_url(record, config.ARGS)

    meta_path = Path(record.data_dir) / ".meta"
    metadata = json.loads(meta_path.read_text(encoding="UTF-8"))

    assert record.status == WorkStatus.URL_VALID.value
    assert config.NEXT_QUEUE.get().status == WorkStatus.URL_VALID.value
    assert metadata["status"] == WorkStatus.URL_VALID.value
    assert metadata["data_tk"] == "token"


def test_validate_url_transitions_missing_record_without_requeue(
    tmp_path: Path,
) -> None:
    """Document current missing-data behavior before workflow migration."""
    scraper = _scraper_without_init()
    scraper.check_for_repo_action = lambda: False
    record = Record(url=ASCII_M1_URL, status=WorkStatus.URL_NEW.value)
    _configure_stage_queues(record, tmp_path)
    scraper._get_page_data = lambda url, timeout: {  # type: ignore[method-assign]
        "html": _form_html(token=""),
        "encoding": "gzip",
        "bytes_length": "123",
    }

    scraper._validate_url(record, config.ARGS)

    meta_path = Path(record.data_dir) / ".meta"
    metadata = json.loads(meta_path.read_text(encoding="UTF-8"))

    assert record.status == WorkStatus.URL_NO_REPO_DATA.value
    assert config.NEXT_QUEUE.empty()
    assert metadata["status"] == WorkStatus.URL_NO_REPO_DATA.value


def test_request_file_uses_local_post_headers(monkeypatch) -> None:
    """Download requests should not mutate global POST_HEADERS."""
    original_headers = dict(config.POST_HEADERS)
    captured: list[dict[str, str]] = []

    class Response:
        headers = {
            "Content-Disposition": "attachment; filename=archive.zip",
        }
        content = b"zip"

    def post(url, *, data, headers, timeout):  # noqa:ANN001
        captured.append(headers)
        return Response()

    monkeypatch.setattr("histdatacom.scraper.scraper.requests.post", post)
    record = Record(
        url=ASCII_M1_URL,
        data_tk="token",
        data_date="2022",
        data_datemonth="2022",
        data_format="ASCII",
        data_timeframe="M1",
        data_fxpair="eurusd",
    )

    Scraper._request_file(record, 1)

    assert config.POST_HEADERS == original_headers
    assert captured[0] is not config.POST_HEADERS
    assert captured[0]["Referer"] == ASCII_M1_URL


def test_download_zip_transitions_valid_record_to_csv_zip(
    tmp_path: Path,
) -> None:
    """Document download stage success behavior before workflow migration."""
    scraper = _scraper_without_init()
    data_dir = tmp_path / "ASCII" / "M1" / "eurusd" / "2022"
    data_dir.mkdir(parents=True)
    record = Record(
        url=ASCII_M1_URL,
        status=WorkStatus.URL_VALID.value,
        data_dir=f"{data_dir}{os.sep}",
        zip_filename="DAT_ASCII_EURUSD_M1_2022.zip",
    )
    with zipfile.ZipFile(data_dir / record.zip_filename, "w") as archive:
        archive.writestr("DAT_ASCII_EURUSD_M1_2022.csv", "rows")
    _configure_stage_queues(record, tmp_path)

    scraper._download_zip(record, config.ARGS)

    meta_path = Path(record.data_dir) / ".meta"
    metadata = json.loads(meta_path.read_text(encoding="UTF-8"))

    assert record.status == WorkStatus.CSV_ZIP.value
    assert config.NEXT_QUEUE.get().status == WorkStatus.CSV_ZIP.value
    assert metadata["status"] == WorkStatus.CSV_ZIP.value


def test_populate_initial_queue_uses_deterministic_plan(
    tmp_path: Path,
) -> None:
    """Legacy queue population should adapt planned work items to records."""
    scraper = _scraper_without_init()
    scraper.urls = Urls()
    config.CURRENT_QUEUE = Records()
    config.NEXT_QUEUE = Records()
    config.ARGS = {
        "start_yearmonth": "202201",
        "end_yearmonth": "202203",
        "formats": {"ascii"},
        "timeframes": {"T"},
        "default_download_dir": f"{tmp_path}{os.sep}",
        "zip_persist": False,
    }
    config.FILTER_PAIRS = {"eurusd"}

    scraper.populate_initial_queue()

    records = []
    while not config.CURRENT_QUEUE.empty():
        records.append(config.CURRENT_QUEUE.get())

    assert [record.url for record in records] == [
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/1",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/2",
        "http://www.histdata.com/download-free-forex-data/"
        "?/ascii/tick-data-quotes/eurusd/2022/3",
    ]
    assert [record.data_datemonth for record in records] == [
        "202201",
        "202202",
        "202203",
    ]
    meta_path = Path(records[0].data_dir) / ".meta"
    metadata = json.loads(meta_path.read_text(encoding="UTF-8"))
    assert metadata["status"] == WorkStatus.URL_NEW.value
    assert metadata["data_format"] == "ASCII"
