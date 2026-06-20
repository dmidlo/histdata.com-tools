"""Pytest unit tests for histdatacom.influx.py."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from histdatacom.histdata_ascii import CACHE_FILENAME


FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"


def test_influx() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def test_influx_parser_accepts_rows_from_polars_cache(tmp_path: Path) -> None:
    """Influx row parsing should accept rows read from the new cache format."""
    from histdatacom.api import Api
    from histdatacom.influx import Influx

    source_record = SimpleNamespace(data_timeframe="M1")
    frame = Api._import_file_to_polars(
        source_record,
        FIXTURES / "DAT_ASCII_EURUSD_M1_201202.csv",
    )
    Api._export_datatable_to_jay(frame, str(tmp_path / CACHE_FILENAME))
    row = next(Api.import_jay_data(str(tmp_path / CACHE_FILENAME)).iter_rows())
    influx_record = SimpleNamespace(
        data_fxpair="eurusd",
        data_format="ascii",
        data_timeframe="M1",
    )

    assert Influx()._parse_jay_row(row, influx_record) == (
        "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
        "openbid=1.3066,highbid=1.3066,lowbid=1.30656,closebid=1.30656 "
        "1328072400000"
    )
