"""Pytest unit tests for histdatacom.scraper.scraper.py."""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import pytest

from histdatacom import config
from histdatacom.exceptions import ArchiveDownloadError
from histdatacom.helper_args import helper_runtime_args
from histdatacom.legacy_boundary import LegacyHelperSideEffectWarning
from histdatacom.manifest_store import ManifestStatusStore
from histdatacom.records import Record
from histdatacom.runtime_contracts import WorkStatus
from histdatacom.scraper.scraper import Scraper
from histdatacom.scraper.urls import Urls

ASCII_TICK_URL = (
    "http://www.histdata.com/download-free-forex-data/"
    "?/ascii/tick-data-quotes/eurusd/2022/1"
)


def _form_html(*, token: str = "token") -> str:
    """Return a minimal HistData download form."""
    return f"""
    <html>
      <form id="file_down">
        <input id="tk" value="{token}">
        <input id="date" value="2022">
        <input id="datemonth" value="202201">
        <input id="platform" value="ASCII">
        <input id="timeframe" value="T">
        <input id="fxpair" value="eurusd">
      </form>
    </html>
    """


def test_scraper() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def _scraper_without_init(args: dict | None = None) -> Scraper:
    """Return a Scraper instance without initializing repo/url collaborators."""
    scraper = object.__new__(Scraper)
    scraper.args = helper_runtime_args(args)
    scraper.filter_pairs = set(scraper.args["pairs"]) or None
    scraper.check_if_repo_validation_is_needed = lambda: False
    scraper.check_for_repo_action = lambda: True
    scraper.set_repo_datum = lambda record: None
    return scraper


def _stage_args(tmp_path: Path) -> dict:
    """Return isolated args for direct stage characterization."""
    return {
        "default_download_dir": f"{tmp_path}{os.sep}",
        "from_api": False,
    }


def test_validate_url_transitions_new_record_to_valid_manifest(
    tmp_path: Path,
) -> None:
    """Validate stage success should return the forwarded record."""
    args = _stage_args(tmp_path)
    scraper = _scraper_without_init(args)
    record = Record(url=ASCII_TICK_URL, status=WorkStatus.URL_NEW.value)

    scraper._get_page_data = lambda url, timeout: {  # type: ignore[method-assign]
        "html": _form_html(),
        "encoding": "gzip",
        "bytes_length": "123",
    }

    result = scraper._validate_url(record, args)

    meta_path = Path(record.data_dir) / ".meta"
    [item] = ManifestStatusStore(tmp_path).list_work_items()

    assert result is record
    assert record.status is WorkStatus.URL_VALID
    assert not meta_path.exists()
    assert item.status is WorkStatus.URL_VALID
    assert item.data_tk == "token"


def test_validate_url_transitions_missing_record_without_requeue(
    tmp_path: Path,
) -> None:
    """Missing data should not be forwarded."""
    args = _stage_args(tmp_path)
    scraper = _scraper_without_init(args)
    scraper.check_for_repo_action = lambda: False
    record = Record(url=ASCII_TICK_URL, status=WorkStatus.URL_NEW.value)
    scraper._get_page_data = lambda url, timeout: {  # type: ignore[method-assign]
        "html": _form_html(token=""),
        "encoding": "gzip",
        "bytes_length": "123",
    }

    result = scraper._validate_url(record, args)

    meta_path = Path(record.data_dir) / ".meta"
    [item] = ManifestStatusStore(tmp_path).list_work_items()

    assert result is None
    assert record.status is WorkStatus.URL_NO_REPO_DATA
    assert not meta_path.exists()
    assert item.status is WorkStatus.URL_NO_REPO_DATA


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

    monkeypatch.setattr("histdatacom.activity_stages.requests.post", post)
    record = Record(
        url=ASCII_TICK_URL,
        data_tk="token",
        data_date="2022",
        data_datemonth="202201",
        data_format="ASCII",
        data_timeframe="T",
        data_fxpair="eurusd",
        data_dir="/tmp/",
    )

    Scraper._request_file(record, 1)

    assert config.POST_HEADERS == original_headers
    assert captured[0] is not config.POST_HEADERS
    assert captured[0]["Referer"] == ASCII_TICK_URL


def test_request_file_rejects_unsupported_raw_dimensions(monkeypatch) -> None:
    """The legacy private request seam must fail before network access."""
    monkeypatch.setattr(
        "histdatacom.activity_stages.requests.post",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("unsupported input reached network")
        ),
    )
    record = Record(
        url=ASCII_TICK_URL,
        data_tk="token",
        data_date="2022",
        data_datemonth="202201",
        data_format="METATRADER",
        data_timeframe="T",
        data_fxpair="eurusd",
        data_dir="/tmp",
    )

    with pytest.raises(ArchiveDownloadError) as exc_info:
        Scraper._request_file(record, 1)

    assert exc_info.value.code == "UNSUPPORTED_RAW_INPUT"


def test_write_file_rejects_retired_archive_member(tmp_path: Path) -> None:
    """The legacy private writer must not persist platform raw payloads."""
    record = Record(
        url=ASCII_TICK_URL,
        data_dir=f"{tmp_path}{os.sep}",
        zip_filename="opaque.zip",
        data_format="ASCII",
        data_timeframe="T",
    )
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as archive:
        archive.writestr("DAT_NT_EURUSD_T_LAST_2022.csv", "rows")

    with pytest.raises(ArchiveDownloadError) as exc_info:
        Scraper._write_file(record, stream.getvalue())

    assert exc_info.value.code == "UNSUPPORTED_RAW_INPUT"
    assert not (tmp_path / record.zip_filename).exists()

    record.data_format = "METATRADER"
    record.zip_filename = "DAT_ASCII_EURUSD_T_2022.zip"
    supported_stream = io.BytesIO()
    with zipfile.ZipFile(supported_stream, "w") as archive:
        archive.writestr("DAT_ASCII_EURUSD_T_2022.csv", "rows")

    with pytest.raises(ArchiveDownloadError) as exc_info:
        Scraper._write_file(record, supported_stream.getvalue())

    assert exc_info.value.code == "UNSUPPORTED_RAW_INPUT"
    assert not (tmp_path / record.zip_filename).exists()


def test_download_zip_transitions_valid_record_to_csv_zip(
    tmp_path: Path,
) -> None:
    """Download stage success should return the forwarded record."""
    scraper = _scraper_without_init()
    data_dir = tmp_path / "ASCII" / "T" / "eurusd" / "2022" / "1"
    data_dir.mkdir(parents=True)
    record = Record(
        url=ASCII_TICK_URL,
        status=WorkStatus.URL_VALID.value,
        data_dir=f"{data_dir}{os.sep}",
        zip_filename="DAT_ASCII_EURUSD_T_202201.zip",
        data_format="ASCII",
        data_timeframe="T",
    )
    with zipfile.ZipFile(data_dir / record.zip_filename, "w") as archive:
        archive.writestr("DAT_ASCII_EURUSD_T_202201.csv", "rows")
    args = _stage_args(tmp_path)
    scraper.args = helper_runtime_args(args)

    result = scraper._download_zip(record, args)

    meta_path = Path(record.data_dir) / ".meta"
    [item] = ManifestStatusStore(tmp_path).list_work_items()

    assert result is record
    assert record.status is WorkStatus.CSV_ZIP
    assert not meta_path.exists()
    assert item.status is WorkStatus.CSV_ZIP


def test_populate_initial_records_uses_deterministic_plan(
    tmp_path: Path,
) -> None:
    """Dataset planning should adapt planned work items to records."""
    args = {
        "start_yearmonth": "202201",
        "end_yearmonth": "202203",
        "formats": {"ascii"},
        "pairs": {"eurusd"},
        "timeframes": {"T"},
        "default_download_dir": f"{tmp_path}{os.sep}",
        "zip_persist": False,
    }
    scraper = _scraper_without_init(args)
    scraper.urls = Urls()

    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Scraper.plan_initial_records",
    ):
        records = scraper.plan_initial_records(args)

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
    item = ManifestStatusStore(tmp_path).get_work_item_for_record(records[0])
    assert item is not None
    assert not meta_path.exists()
    assert item.status is WorkStatus.URL_NEW
    assert item.data_format == "ASCII"


def test_plan_initial_records_resyncs_runtime_pair_filter(
    tmp_path: Path,
) -> None:
    """Explicit runtime args should not reuse a stale helper pair filter."""
    args = {
        "start_yearmonth": "202201",
        "end_yearmonth": "202201",
        "formats": {"ascii"},
        "pairs": {"eurusd"},
        "timeframes": {"T"},
        "default_download_dir": f"{tmp_path}{os.sep}",
        "zip_persist": False,
    }
    scraper = _scraper_without_init(args)
    scraper.urls = Urls()

    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Scraper.plan_initial_records",
    ):
        records = scraper.plan_initial_records({**args, "pairs": {"gbpusd"}})

    assert scraper.filter_pairs == {"gbpusd"}
    assert [record.data_fxpair for record in records] == ["gbpusd"]
    assert records[0].url.endswith("/gbpusd/2022/1")


def test_scraper_instances_keep_pair_filters_isolated(tmp_path: Path) -> None:
    """Scraper helper filters should not bleed through module globals."""
    first = Scraper(
        {
            "default_download_dir": f"{tmp_path / 'first'}{os.sep}",
            "pairs": {"eurusd"},
        }
    )
    second = Scraper(
        {
            "default_download_dir": f"{tmp_path / 'second'}{os.sep}",
            "pairs": {"gbpusd"},
        }
    )

    assert first.filter_pairs == {"eurusd"}
    assert second.filter_pairs == {"gbpusd"}
    assert first.repo.filter_pairs == {"eurusd"}
    assert second.repo.filter_pairs == {"gbpusd"}
