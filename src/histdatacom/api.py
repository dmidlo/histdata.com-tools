"""Api functions for histdatacom.

Raises:
    ValueError: On failed jay creation
    SystemExit: On failed jay creation

Returns:
    "Frame" | "DataFrame" | "Table":
        - (Frame) if options.api_return_type = "datatable"
        - (DataFrame) if options.api_return_type = "pandas"
        - (Table) if options.api_return_type = "arrow"
"""
from __future__ import annotations

import itertools
import sys  # sourcery skip
from pathlib import Path
from typing import TYPE_CHECKING, Tuple

import datatable as dt  # noqa:I900
from datatable import Frame, f, update  # noqa:I900
from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from histdatacom import config
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count
from histdatacom.scraper.scraper import Scraper

if TYPE_CHECKING:
    from datatable import FExpr  # noqa:I900
    from pandas.core.frame import DataFrame
    from pyarrow import Table

    from histdatacom.records import Record, Records

dt.options.progress.enabled = False


class Api:  # noqa:H601
    """Api functions for histdatacom.

    Raises:
        ValueError: On failed jay creation
        SystemExit: On failed jay creation

    Returns:
        "Frame" | "DataFrame" | "Table":
            - (Frame) if options.api_return_type = "datatable"
            - (DataFrame) if options.api_return_type = "pandas"
            - (Table) if options.api_return_type = "arrow"
    """

    @classmethod
    def _create_jay(cls, record: "Record", args: dict) -> None:
        """Create .jay file based on single record's data.

        creates a datatable file, saves it in dt's native jay format
        using and updating relevant information in a Record of work.

        Args:
            record (Record): a histdatacom.records.Record
            args (dict): args received from argparse
        """
        zip_path = Path(record.data_dir, record.zip_filename)
        csv_path = Path(record.data_dir, record.csv_filename)

        if zip_path.exists():
            file_data = cls._import_file_to_datatable(record, zip_path)
        elif csv_path.exists():
            file_data = cls._import_file_to_datatable(record, csv_path)

        record.jay_filename = ".data"
        jay_path = record.data_dir + record.jay_filename
        cls._export_datatable_to_jay(file_data, jay_path)

        record.jay_line_count = file_data.nrows
        record.jay_start = str(
            cls._extract_single_value_from_frame(file_data, 0, "datetime")
        )
        record.jay_end = str(
            cls._extract_single_value_from_frame(
                file_data, file_data.nrows - 1, "datetime"
            )
        )
        record.write_memento_file(base_dir=args["default_download_dir"])

    @classmethod
    def test_for_jay_or_create(cls, record: Record, args: dict) -> None:
        """Test for record's jay file. if it doesn't exist, create it.

           a helper method to ensure the existence of a Record's jay file
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
            jay_path = Path(record.data_dir, ".data")
            if not jay_path.exists():
                if "CSV" not in record.status:
                    Scraper.get_zip_file(record)
                cls._create_jay(record, args)

    @classmethod
    def _validate_jay(
        cls,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
    ) -> None:
        """Validate Jay prior to possible merge operation.

           A Wrapper to be passed to an individual process within the process
           pool to test for or create a datatable jay file based on a Record of
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
            cls.test_for_jay_or_create(record, args)
            records_next.put(record)
        except Exception:
            print("Unexpected error:", sys.exc_info())  # noqa:T201
            record.delete_momento_file()
            raise
        finally:
            records_current.task_done()

    @classmethod
    def _extract_single_value_from_frame(
        cls, frame: "Frame", row: int, column: str
    ) -> int:
        """Extract Single Value from datatable Frame.

        Args:
            frame (Frame): datatable.Frame
            row (int): datatable frame row
            column (str): datatable frame column

        Returns:
            int: datatable time64
        """
        return int(frame[row, column])

    @classmethod
    def _import_file_to_datatable(cls, record: Record, zip_path: Path) -> Frame:
        """Import file as datatable Frame and convert timestamp to UTC.

        # noqa: DAR402

        Args:
            record (Record): a record from the work queue.
            zip_path (Path): path to record's zip.

        Raises:
            ValueError: Error on null value.
            SystemExit: Exit on Error.


        Returns:
            Frame: datatable.Frame
        """
        # pylint: disable=expression-not-assigned
        try:
            match record.data_timeframe:
                case "M1":
                    frame_data: Frame = cls._import_frame_with_headers(
                        record.data_timeframe, zip_path
                    )

                    ascii_m1_etc_ms_timestamp = cls._strptime_fexpr_for_frame(
                        record.data_timeframe
                    )

                    ascii_m1_utc_ms_timestamp = (  # noqa:BLK001
                        cls._adjust_est_timestamp_to_utc(  # noqa:BLK001
                            ascii_m1_etc_ms_timestamp
                        )
                    )

                    # pylint: disable-next=unsubscriptable-object
                    frame_data[:, update(datetime=ascii_m1_utc_ms_timestamp)]
                case "T":
                    frame_data = cls._import_frame_with_headers(
                        record.data_timeframe, zip_path
                    )

                    ascii_t_etc_ms_timestamp = cls._strptime_fexpr_for_frame(
                        record.data_timeframe
                    )

                    ascii_t_utc_ms_timestamp = cls._adjust_est_timestamp_to_utc(
                        ascii_t_etc_ms_timestamp
                    )

                    # pylint: disable-next=unsubscriptable-object
                    frame_data[:, update(datetime=ascii_t_utc_ms_timestamp)]
                case _:
                    raise ValueError

            # pylint: disable-next=unsupported-assignment-operation
            frame_data["vol"] = dt.int32
            return frame_data  # noqa:TC300
        except ValueError as err:
            raise SystemExit from err

    @classmethod
    def _import_frame_with_headers(  # noqa:BLK001
        cls, timeframe: str, zip_path: Path
    ) -> "Frame":
        """Import datatable.Frame with headers for M1 and Tick data.

        Args:
            timeframe (str): M1 or T
            zip_path (Path): Path

        Raises:
            ValueError: when not M1 or T

        Returns:
            Frame: datatable.Frame
        """
        match timeframe:
            case "M1":
                frame = dt.fread(
                    zip_path,
                    header=False,
                    columns=[
                        "datetime",
                        "open",
                        "high",
                        "low",
                        "close",
                        "vol",
                    ],
                    multiple_sources="ignore",
                )
            case "T":
                frame = dt.fread(
                    zip_path,
                    header=False,
                    columns=["datetime", "bid", "ask", "vol"],
                    multiple_sources="ignore",
                )
            case _:
                raise ValueError
        return frame  # noqa:R504

    @classmethod
    def _strptime_fexpr_for_frame(cls, timeframe: str) -> "FExpr":
        """Convert csv datetime to unix timestamp in EST.

           Convert csv datetime to unix timestamp in EST with NO
           DAYLIGHT SAVINGS TIME.

        Args:
            timeframe (str): M1 or T

        Returns:
            FExpr: datatable.FExpr
        """
        match timeframe:
            case "M1":
                ascii_m1_str_splitter = dt.time.ymdt(  # sourcery skip
                    f.datetime[0:4].as_type(int),
                    f.datetime[4:6].as_type(int),
                    f.datetime[6:8].as_type(int),
                    f.datetime[9:11].as_type(int),
                    f.datetime[11:13].as_type(int),
                    f.datetime[13:15].as_type(int),
                )
                temp_time = ascii_m1_str_splitter.as_type(int)
            case "T":
                ascii_t_str_splitter = dt.time.ymdt(
                    f.datetime[0:4].as_type(int),
                    f.datetime[4:6].as_type(int),
                    f.datetime[6:8].as_type(int),
                    f.datetime[9:11].as_type(int),
                    f.datetime[11:13].as_type(int),
                    f.datetime[13:15].as_type(int),
                    10**6 * f.datetime[15:18].as_type(int),
                )
                temp_time = ascii_t_str_splitter.as_type(int)

        return temp_time // 10**6

    @classmethod
    def _adjust_est_timestamp_to_utc(cls, est_fexpr: "FExpr") -> "FExpr":
        """Convert ESTnoDST millisecond timestamp to UTC.

        Args:
            est_fexpr (FExpr): datatable.FExpr

        Returns:
            FExpr: datatable.FExpr
        """
        return est_fexpr + 18000000

    @classmethod
    def _export_datatable_to_jay(  # noqa:BLK001
        cls, data_frame: "Frame", file_path: str
    ) -> None:
        """Export datatable frame to jay.

        Args:
            data_frame (Frame): datatable.Frame
            file_path (str): dest path
        """
        data_path = file_path
        data_frame.to_jay(data_path)

    @classmethod
    def import_jay_data(cls, jay_path: str) -> "Frame":
        """Read jay file in to datatable Frame.

        Args:
            jay_path (str): source path

        Returns:
            Frame: datatable.Frame
        """
        return dt.fread(jay_path)

    def validate_jays(self) -> None:
        """Initialize a process pool to validate jay files."""
        pool = ProcessPool(
            self._validate_jay,
            config.ARGS,
            "Staging",
            "data files...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
        )
        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    def merge_jays(self) -> list | Frame | DataFrame | Table:
        """Merge jays for start_yearmonth and end_yearmonth range.

        Returns:
            list | Frame | DataFrame | Table: _description_
        """
        records_to_merge: list
        pairs: list
        timeframes: list

        records_to_merge, pairs, timeframes = self._dequeue_records_for_merge()
        sets_to_merge: list = self._collate_sets_to_merge(
            records_to_merge, pairs, timeframes
        )

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
                                 "data": datatable.Frame | None,
                                }
        """
        match tp_set_dict["timeframe"]:
            case "T":
                merged = dt.Frame(names=["datetime", "bid", "ask", "vol"])
            case "M1":
                merged = dt.Frame(
                    names=[
                        "datetime",
                        "open",
                        "high",
                        "low",
                        "close",
                        "vol",
                    ]
                )

        tp_set_dict["records"].sort(key=lambda record: record.jay_start)

        records_count = len(tp_set_dict)
        with Progress(
            TextColumn(text_format="[cyan]Merging records..."),
            BarColumn(),
            "[progress.percentage]{task.percentage:>3.0f}%",
            TimeElapsedColumn(),
        ) as progress:
            progress.add_task("merge", total=records_count)

            for m_record in tp_set_dict["records"]:
                jay_path = m_record.data_dir + m_record.jay_filename
                jay_data = self.import_jay_data(jay_path)
                merged.rbind(jay_data)

            match config.ARGS["api_return_type"]:
                case "datatable":
                    tp_set_dict["data"] = merged
                case "arrow":
                    tp_set_dict["data"] = merged.to_arrow()
                case "pandas":
                    tp_set_dict["data"] = merged.to_pandas()

    def _dequeue_records_for_merge(self) -> Tuple[list, list, list]:
        """Empty the queue of relevant records.

        Empty the queue of relevant records, create additional
        lists for each pair and timeframe to appear in the set
        of records.

        Returns:
            Tuple[list, list, list]: records_to_merge, pairs, timeframes
        """
        records_to_merge: list = []
        pairs: list = []
        timeframes: list = []
        while not config.CURRENT_QUEUE.empty():  # type: ignore
            record = config.CURRENT_QUEUE.get()  # type: ignore

            if record is None:
                break

            if (
                record.jay_filename == ".data"
                and Path(record.data_dir, record.jay_filename).exists()
            ):
                pairs.append(record.data_fxpair)
                timeframes.append(record.data_timeframe)
                records_to_merge.append(record)

        return records_to_merge, pairs, timeframes

    def _collate_sets_to_merge(
        self, records_to_merge: list, pairs: list, timeframes: list
    ) -> list:  # noqa:TAE002
        """Organize records into pair/timeframe sets.

        Args:
            records_to_merge (list): list of records
            pairs (list): list of pairs
            timeframes (list): list of timeframes

        Returns:
            list: _description_
        """
        sets_to_merge = []
        for timeframe, pair in itertools.product(set(timeframes), set(pairs)):
            tp_set_dict = {
                "timeframe": timeframe,
                "pair": pair,
                "records": [],
                "data": None,
            }
            for m_record in records_to_merge:
                if (
                    m_record.data_timeframe == timeframe
                    and m_record.data_fxpair == pair
                ):
                    tp_set_dict["records"].append(m_record)
            sets_to_merge.append(tp_set_dict)

        return sets_to_merge
