"""Api functions for histdatacom.

Raises:
    ValueError: On failed cache creation
    SystemExit: On failed cache creation

Returns:
    "PolarsDataFrame" | "DataFrame" | "Table":
        - (PolarsDataFrame) if options.api_return_type = "polars"
        - (DataFrame) if options.api_return_type = "pandas"
        - (Table) if options.api_return_type = "arrow"
"""

from __future__ import annotations

import sys  # sourcery skip
from pathlib import Path
from typing import TYPE_CHECKING

from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from histdatacom import config
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count
from histdatacom.histdata_ascii import (
    CACHE_FILENAME,
    convert_polars_datetime_to_utc_ms,
    read_polars_cache,
    read_ascii_file_to_polars,
    write_polars_cache,
)
from histdatacom.runtime_contracts import status_has_csv_artifact
from histdatacom.scraper.scraper import Scraper
from histdatacom.utils import check_installed_module

if TYPE_CHECKING:
    from pandas.core.frame import DataFrame
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table

    from histdatacom.records import Record, Records


class Api:  # noqa:H601
    """Api functions for histdatacom.

    Raises:
        ValueError: On failed cache creation
        SystemExit: On failed cache creation

    Returns:
        "PolarsDataFrame" | "DataFrame" | "Table":
            - (PolarsDataFrame) if options.api_return_type = "polars"
            - (DataFrame) if options.api_return_type = "pandas"
            - (Table) if options.api_return_type = "arrow"
    """

    @classmethod
    def _create_cache(cls, record: "Record", args: dict) -> None:
        """Create cache file based on single record's data.

        creates a Polars dataframe, saves it in Arrow IPC format
        using and updating relevant information in a Record of work.

        Args:
            record (Record): a histdatacom.records.Record
            args (dict): args received from argparse
        """
        zip_path = Path(record.data_dir, record.zip_filename)
        csv_path = Path(record.data_dir, record.csv_filename)

        if zip_path.exists():
            file_data = cls._import_file_to_polars(record, zip_path)
        elif csv_path.exists():
            file_data = cls._import_file_to_polars(record, csv_path)
        else:
            raise ValueError("expected downloaded ZIP or CSV source file")

        record.cache_filename = CACHE_FILENAME
        cache_path = record.data_dir + record.cache_filename
        cls._write_cache_data(file_data, cache_path)

        record.cache_line_count = file_data.height
        record.cache_start = str(
            cls._extract_single_value_from_frame(file_data, 0, "datetime")
        )
        record.cache_end = str(
            cls._extract_single_value_from_frame(
                file_data, file_data.height - 1, "datetime"
            )
        )
        record.write_memento_file(base_dir=args["default_download_dir"])

    @classmethod
    def test_for_cache_or_create(cls, record: Record, args: dict) -> None:
        """Test for record's cache file. if it doesn't exist, create it.

           a helper method to ensure the existence of a Record's cache file
           prior to further processing from the API or Influx classes

        Args:
            record (Record): a histdatacom.records.Record
            args (dict): args received from argparse
        """
        if str.lower(  # noqa:BLK001
            record.data_format
        ) == "ascii" and record.data_timeframe in [
            "T",
            "M1",
        ]:
            cache_path = Path(record.data_dir, CACHE_FILENAME)
            if not cache_path.exists():
                if not status_has_csv_artifact(record.status):
                    Scraper.get_zip_file(record)
                cls._create_cache(record, args)

    @classmethod
    def _validate_cache(
        cls,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
    ) -> None:
        """Validate Cache prior to possible merge operation.

           A Wrapper to be passed to an individual process within the process
           pool to test for or create a Polars cache file based on a Record of
           Work's information.  Receives a unit of work from the pool, performs
           validation, readies the Record for further processing, and marks the
           current work as complete.

        Args:
            record (Record): a Histdatacom.records.Record
            args (dict): arguments received from argparse
            records_current (Records):
                Current Work Records Queue
            records_next (Records):
                Records Queue for Further Work

        Raises:
            Exception: Unknown Exception
        """
        try:
            cls.test_for_cache_or_create(record, args)
            records_next.put(record)
        except Exception:
            print("Unexpected error:", sys.exc_info())  # noqa:T201
            record.delete_momento_file()
            raise
        finally:
            records_current.task_done()

    @classmethod
    def _extract_single_value_from_frame(
        cls, frame: "PolarsDataFrame", row: int, column: str
    ) -> int:
        """Extract a single value from a dataframe.

        Args:
            frame (PolarsDataFrame): dataframe
            row (int): frame row
            column (str): frame column

        Returns:
            int: cell value
        """
        return int(frame.item(row, column))

    @classmethod
    def _import_file_to_polars(
        cls, record: Record, zip_path: Path
    ) -> "PolarsDataFrame":
        """Import file as a raw Polars dataframe.

        # noqa: DAR402

        Args:
            record (Record): a record from the work queue.
            zip_path (Path): path to record's zip.

        Raises:
            ValueError: Error on null value.
            SystemExit: Exit on Error.


        Returns:
            DataFrame: polars.DataFrame
        """
        try:
            raw_frame = cls._import_frame_with_headers(
                record.data_timeframe,
                zip_path,
            )
            return convert_polars_datetime_to_utc_ms(
                raw_frame,
                record.data_timeframe,
            )
        except ValueError as err:
            raise SystemExit(1) from err

    @classmethod
    def _import_frame_with_headers(  # noqa:BLK001
        cls, timeframe: str, zip_path: Path
    ) -> "PolarsDataFrame":
        """Import a raw Polars dataframe with headers for M1 and Tick data.

        Args:
            timeframe (str): M1 or T
            zip_path (Path): Path

        Raises:
            ValueError: when not M1 or T

        Returns:
            DataFrame: polars.DataFrame
        """
        return read_ascii_file_to_polars(zip_path, timeframe)

    @classmethod
    def _write_cache_data(  # noqa:BLK001
        cls, data_frame: "PolarsDataFrame", file_path: str
    ) -> None:
        """Export a Polars frame to the transitional cache path.

        Args:
            data_frame (PolarsDataFrame): Polars dataframe.
            file_path (str): dest path
        """
        write_polars_cache(data_frame, Path(file_path))

    @classmethod
    def import_cache_data(cls, cache_path: str) -> "PolarsDataFrame":
        """Read cache file into a Polars dataframe.

        Args:
            cache_path (str): source path

        Returns:
            DataFrame: polars.DataFrame
        """
        return read_polars_cache(Path(cache_path))

    def validate_caches(self) -> None:
        """Initialize a process pool to validate cache files."""
        pool = ProcessPool(
            self._validate_cache,
            config.ARGS,
            "Staging",
            "data files...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
        )
        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    def merge_caches(self) -> list | PolarsDataFrame | DataFrame | Table:
        """Merge caches for start_yearmonth and end_yearmonth range.

        Returns:
            list | PolarsDataFrame | DataFrame | Table:
                merged data for the configured API return type
        """
        records_to_merge: list = self._dequeue_records_for_merge()
        sets_to_merge: list = self._collate_sets_to_merge(records_to_merge)

        if not sets_to_merge:
            return []

        for tp_set in sets_to_merge:
            self._merge_records(tp_set)

        return (  # noqa:BLK001
            sets_to_merge[0]["data"]  # noqa:BLK001
            if len(sets_to_merge) == 1
            else sets_to_merge
        )

    def _merge_records(self, tp_set_dict: dict) -> None:
        """Sort and Merge records from a timeframe/pair set.

        Args:
            tp_set_dict (dict): {
                                 "timeframe": timeframe,
                                 "pair": pair,
                                 "records": [],
                                 "data": PolarsDataFrame | None,
                                }
        """
        import polars as pl

        tp_set_dict["records"].sort(key=lambda record: record.cache_start)

        frames = []
        records_count = len(tp_set_dict["records"])
        if records_count:
            with Progress(
                TextColumn(text_format="[cyan]Merging records..."),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn(),
            ) as progress:
                task_id = progress.add_task("merge", total=records_count)

                for m_record in tp_set_dict["records"]:
                    cache_path = m_record.data_dir + m_record.cache_filename
                    frames.append(self.import_cache_data(cache_path))
                    progress.advance(task_id)

        merged = pl.concat(frames) if frames else pl.DataFrame()

        match config.ARGS["api_return_type"]:
            case "arrow":
                check_installed_module("arrow", True)
                tp_set_dict["data"] = merged.to_arrow()
            case "pandas":
                check_installed_module("pandas", True)
                tp_set_dict["data"] = merged.to_pandas()
            case "polars":
                check_installed_module("polars", True)
                tp_set_dict["data"] = merged

    def _dequeue_records_for_merge(self) -> list:
        """Empty the queue of relevant records.

        Empty the queue of relevant records and return records whose cache
        files exist.

        Returns:
            list: records_to_merge
        """
        records_to_merge: list = []
        while not config.CURRENT_QUEUE.empty():  # type: ignore
            record = config.CURRENT_QUEUE.get()  # type: ignore

            if record is None:
                break

            if (
                record.cache_filename == CACHE_FILENAME
                and Path(record.data_dir, record.cache_filename).exists()
            ):
                records_to_merge.append(record)

        return records_to_merge

    def _collate_sets_to_merge(self, records_to_merge: list) -> list:
        """Organize records into pair/timeframe sets.

        Args:
            records_to_merge (list): list of records

        Returns:
            list: _description_
        """
        sets_to_merge = []
        sets_by_key = {}
        for m_record in records_to_merge:
            key = (m_record.data_timeframe, m_record.data_fxpair)
            if key not in sets_by_key:
                tp_set_dict = {
                    "timeframe": m_record.data_timeframe,
                    "pair": m_record.data_fxpair,
                    "records": [],
                    "data": None,
                }
                sets_by_key[key] = tp_set_dict
                sets_to_merge.append(tp_set_dict)

            sets_by_key[key]["records"].append(m_record)

        return sets_to_merge
