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

    assert Influx()._parse_jay_row(row, influx_record) == expected_line


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
def test_influx_batch_size_requires_positive_integer(batch_size: object) -> None:
    """Invalid Influx batch sizes should fail before queue processing."""
    from histdatacom.influx import _coerce_batch_size

    with pytest.raises(ValueError, match="positive integer"):
        _coerce_batch_size(batch_size)


def test_import_jay_batches_polars_rows_into_influx_queue(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Import batching should submit bounded row groups to the queue flow."""
    from histdatacom.influx import Influx
    import histdatacom.influx as influx_module

    class FakeFuture:
        def result(self) -> None:
            return None

    class FakeExecutor:
        def __init__(
            self,
            max_workers: int,
            initializer: object,
            initargs: tuple[object, ...],
        ) -> None:
            assert max_workers == 1
            initializer(*initargs)  # type: ignore[operator]

        def __enter__(self) -> "FakeExecutor":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def submit(
            self, func: object, rows: list[tuple[object, ...]], record: object
        ) -> FakeFuture:
            submitted_batch_sizes.append(len(rows))
            func(rows, record)  # type: ignore[operator]
            return FakeFuture()

    class FakeQueue:
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
    queue = FakeQueue()
    record = SimpleNamespace(
        data_dir=str(tmp_path) + "/",
        jay_filename=CACHE_FILENAME,
        data_fxpair="eurusd",
        data_format="ascii",
        data_timeframe="M1",
    )
    args = {"batch_size": "2"}
    submitted_batch_sizes: list[int] = []
    monkeypatch.setattr(
        influx_module,
        "ProcessPoolExecutor",
        FakeExecutor,
    )

    Influx()._import_jay(
        record,
        args,
        SimpleNamespace(),
        SimpleNamespace(),
        queue,  # type: ignore[arg-type]
    )

    assert submitted_batch_sizes == [2, 1]
    assert [len(item) for item in queue.items] == [2, 1]
    assert queue.items[0][0] == EXPECTED_M1_LINE
