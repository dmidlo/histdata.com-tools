"""Tests for the Temporal sidecar-era legacy helper boundary."""

from __future__ import annotations

import os
import warnings
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.activity_stages import dataset_plan_stage
from histdatacom.helper_args import helper_runtime_args
from histdatacom.legacy_boundary import LegacyHelperSideEffectWarning
from histdatacom.records import Record
from histdatacom.runtime_contracts import WorkStatus
from histdatacom.scraper.scraper import Scraper


def _scraper_without_init(args: Mapping[str, object] | None = None) -> Scraper:
    """Return a scraper helper with only runtime-arg state initialized."""
    scraper = object.__new__(Scraper)
    scraper.args = helper_runtime_args(args)
    scraper.filter_pairs = set(scraper.args["pairs"]) or None
    scraper.check_if_repo_validation_is_needed = lambda: False
    scraper.check_for_repo_action = lambda: True
    scraper.set_repo_datum = lambda record: None
    return scraper


def test_scraper_validation_and_download_helpers_warn_without_network(
    tmp_path: Path,
) -> None:
    """Direct Scraper side-effect batches should be visibly bounded."""
    record = Record(
        url="http://example.test/archive", status=WorkStatus.URL_NEW
    )
    scraper = _scraper_without_init(
        {"default_download_dir": f"{tmp_path}{os.sep}"}
    )
    scraper._validate_url = lambda item, args: item  # type: ignore[method-assign]
    scraper._download_zip = lambda item, args: item  # type: ignore[method-assign]

    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Scraper.validate_urls",
    ):
        assert scraper.validate_urls([record]) == [record]
    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Scraper.download_zips",
    ):
        assert scraper.download_zips([record]) == [record]


def test_scraper_archive_download_helper_warns_without_network(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The direct archive helper should warn before the download adapter runs."""
    calls: list[Record] = []
    record = Record(url="http://example.test/archive")
    monkeypatch.setattr(
        "histdatacom.scraper.scraper.download_histdata_archive_to_record",
        lambda item, **kwargs: calls.append(item),
    )

    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Scraper.get_zip_file",
    ):
        Scraper.get_zip_file(record)

    assert calls == [record]


def test_api_cache_validation_helpers_warn_without_cache_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct Api cache helpers should make sidecar bypass explicit."""
    from histdatacom.api import Api

    record = SimpleNamespace(data_format="zip", data_timeframe="M1")
    monkeypatch.setattr(
        Api,
        "_validate_cache",
        classmethod(lambda cls, item, args: item),
    )

    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Api.test_for_cache_or_create",
    ):
        Api.test_for_cache_or_create(record, {})
    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Api.validate_caches",
    ):
        assert Api().validate_caches([record]) == [record]


def test_influx_import_helper_warns_without_live_influx(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Direct Influx imports should warn while stage adapters remain callable."""
    import histdatacom.influx as influx_module

    class FakeBatchWriter:
        def __init__(self, args: Mapping[str, object]) -> None:
            self.args = args

        def __enter__(self) -> "FakeBatchWriter":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def write_lines(self, lines: list[str]) -> None:
            return None

    record = SimpleNamespace(data_fxpair="eurusd")
    monkeypatch.setattr(influx_module, "InfluxBatchWriter", FakeBatchWriter)
    monkeypatch.setattr(
        influx_module.Influx,
        "_import_file",
        lambda self, item, args, emit_lines: item,
    )

    with pytest.warns(
        LegacyHelperSideEffectWarning,
        match="Influx.import_data",
    ):
        assert influx_module.Influx().import_data([record]) == [record]


def test_activity_stage_helpers_do_not_emit_legacy_boundary_warnings(
    tmp_path: Path,
) -> None:
    """Activity-stage helpers should stay clean for Temporal workers."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        output = dataset_plan_stage(
            start_yearmonth="202201",
            end_yearmonth="202201",
            formats={"ascii"},
            pairs={"eurusd"},
            timeframes={"tick-data-quotes"},
            default_download_dir=f"{tmp_path}{os.sep}",
            current_yearmonth="202201",
        )

    assert output.work_items
    assert not any(
        issubclass(item.category, LegacyHelperSideEffectWarning)
        for item in caught
    )
