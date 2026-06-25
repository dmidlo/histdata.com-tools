"""Python API quickstart matching the README examples."""

from __future__ import annotations

from typing import Any, Callable

import histdatacom
from histdatacom.options import Options

HistDataRunner = Callable[[Options], Any]


def build_extract_options() -> Options:
    """Return script/application ETL options for a small extract job."""
    options = Options()
    options.extract_csvs = True
    options.formats = {"ascii"}
    options.timeframes = {"tick-data-quotes"}
    options.pairs = {"eurusd"}
    options.start_yearmonth = "2021-04"
    options.end_yearmonth = "2021-05"
    options.cpu_utilization = "medium"
    return options


def build_polars_options() -> Options:
    """Return notebook/dataframe options for one small Polars result."""
    options = Options()
    options.api_return_type = "polars"
    options.formats = {"ascii"}
    options.timeframes = {"1-minute-bar-quotes"}
    options.pairs = {"eurusd"}
    options.start_yearmonth = "2021-04"
    options.end_yearmonth = "2021-05"
    options.cpu_utilization = "medium"
    return options


def submit_extract_job(runner: HistDataRunner = histdatacom) -> Any:
    """Submit CLI-shaped ETL work from a script or application."""
    return runner(build_extract_options())


def load_polars_frame(runner: HistDataRunner = histdatacom) -> Any:
    """Load a dataframe/table result for notebook or programmatic use."""
    return runner(build_polars_options())


def main() -> None:
    """Run the dataframe quickstart and print a compact result summary."""
    data = load_polars_frame()
    print(type(data))
    print(getattr(data, "shape", "shape unavailable"))


if __name__ == "__main__":
    main()
