"""Pytest unit tests for histdatacom.influx.py."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from histdatacom.histdata_ascii import CACHE_FILENAME

FIXTURES = Path(__file__).parents[1] / "fixtures" / "histdata_ascii"

EXPECTED_M1_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=M1 "
    "openbid=1.3066,highbid=1.3066,lowbid=1.30656,closebid=1.30656 "
    "1328072400000"
)
EXPECTED_TICK_LINE = (
    "eurusd,source=histdata.com,format=ascii,timeframe=T "
    "bidquote=1.3066,askquote=1.30677 1328072403660"
)


def test_influx() -> None:
    """Test pytest path resolution."""
    assert True  # noqa:S101 # sourcery skip # act


def _read_cache_frame(tmp_path: Path, timeframe: str, filename: str) -> object:
    """Create and read a Polars cache frame for an Influx fixture."""
    from histdatacom.api import Api

    source_record = SimpleNamespace(data_timeframe=timeframe)
    frame = Api._import_file_to_polars(
        source_record,
        FIXTURES / filename,
    )
    Api._write_cache_data(frame, str(tmp_path / CACHE_FILENAME))
    return Api.import_cache_data(str(tmp_path / CACHE_FILENAME))


@pytest.mark.parametrize(
    ("timeframe", "filename", "expected_line"),
    (
        ("M1", "DAT_ASCII_EURUSD_M1_201202.csv", EXPECTED_M1_LINE),
        ("T", "DAT_ASCII_EURUSD_T_201202.csv", EXPECTED_TICK_LINE),
    ),
)
def test_influx_parser_accepts_rows_from_polars_cache(
    tmp_path: Path, timeframe: str, filename: str, expected_line: str
) -> None:
    """Influx row parsing should accept rows read from the new cache format."""
    from histdatacom.influx import Influx

    row = next(_read_cache_frame(tmp_path, timeframe, filename).iter_rows())
    influx_record = SimpleNamespace(
        data_fxpair="eurusd",
        data_format="ascii",
        data_timeframe=timeframe,
    )

    assert Influx()._parse_cache_row(row, influx_record) == expected_line


def test_influx_polars_row_batches_honor_integer_batch_size(
    tmp_path: Path,
) -> None:
    """Influx batching should slice Polars frames without all-row buffering."""
    from histdatacom.influx import _coerce_batch_size, _iter_polars_row_batches

    frame = _read_cache_frame(
        tmp_path,
        "M1",
        "DAT_ASCII_EURUSD_M1_201202.csv",
    )

    batches = list(_iter_polars_row_batches(frame, _coerce_batch_size("2")))

    assert [len(batch) for batch in batches] == [2, 1]
    assert [row[0] for batch in batches for row in batch] == [
        1328072400000,
        1328072460000,
        1328072520000,
    ]


@pytest.mark.parametrize("batch_size", (0, -1, "0", "not-an-int", None))
def test_influx_batch_size_requires_positive_integer(
    batch_size: object,
) -> None:
    """Invalid Influx batch sizes should fail before import processing."""
    from histdatacom.influx import _coerce_batch_size

    with pytest.raises(ValueError, match="positive integer"):
        _coerce_batch_size(batch_size)


def test_import_cache_batches_polars_rows_into_influx_sink(
    tmp_path: Path,
) -> None:
    """Import batching should submit bounded row groups to a sink."""
    from histdatacom.influx import Influx

    class FakeSink:
        def __init__(self) -> None:
            self.items: list[list[str]] = []

        def put(self, item: list[str]) -> None:
            self.items.append(item)

    frame = _read_cache_frame(
        tmp_path,
        "M1",
        "DAT_ASCII_EURUSD_M1_201202.csv",
    )
    assert frame.height == 3
    sink = FakeSink()
    record = SimpleNamespace(
        data_dir=str(tmp_path) + "/",
        cache_filename=CACHE_FILENAME,
        data_fxpair="eurusd",
        data_format="ascii",
        data_timeframe="M1",
    )
    args = {"batch_size": "2"}

    Influx()._import_cache(
        record,
        args,
        sink,  # type: ignore[arg-type]
    )

    assert [len(item) for item in sink.items] == [2, 1]
    assert sink.items[0][0] == EXPECTED_M1_LINE


def test_influx_batch_writer_writes_direct_synchronous_batches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The direct writer should not require process plumbing."""
    import histdatacom.influx as influx_module

    class FakePrecision:
        MS = "ms"

    class FakeWriteApi:
        def __init__(self) -> None:
            self.writes: list[dict[str, object]] = []
            self.closed = False

        def write(self, **kwargs: object) -> None:
            self.writes.append(kwargs)

        def close(self) -> None:
            self.closed = True

    class FakeClient:
        instances: list["FakeClient"] = []

        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs
            self.write_api_instance = FakeWriteApi()
            self.write_options: object = None
            self.closed = False
            self.instances.append(self)

        def write_api(self, *, write_options: object) -> FakeWriteApi:
            self.write_options = write_options
            return self.write_api_instance

        def close(self) -> None:
            self.closed = True

    monkeypatch.setattr(
        influx_module,
        "_load_influx_client_api",
        lambda: (FakeClient, FakePrecision, "sync-options"),
    )

    with influx_module.InfluxBatchWriter(
        {
            "INFLUX_ORG": "org",
            "INFLUX_BUCKET": "bucket",
            "INFLUX_URL": "http://localhost:8086",
            "INFLUX_TOKEN": "token",
        }
    ) as writer:
        writer.write_lines(["line-1", "line-2"])

    [client] = FakeClient.instances
    assert client.kwargs == {
        "url": "http://localhost:8086",
        "token": "token",
        "org": "org",
        "debug": False,
    }
    assert client.write_options == "sync-options"
    assert client.write_api_instance.writes == [
        {
            "org": "org",
            "bucket": "bucket",
            "record": ["line-1", "line-2"],
            "write_precision": "ms",
        }
    ]
    assert client.write_api_instance.closed
    assert client.closed
