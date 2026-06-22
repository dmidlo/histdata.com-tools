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

from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich.progress import BarColumn, Progress, TextColumn, TimeElapsedColumn

from histdatacom.activity_stages import (
    apply_stage_output_to_record,
    build_cache_work_item,
    create_cache_file,
    merge_cache_records,
    merge_cache_work_items,
)
from histdatacom.histdata_ascii import (
    convert_polars_datetime_to_utc_ms,
    read_polars_cache,
    read_ascii_file_to_polars,
    write_polars_cache,
)
from histdatacom.helper_args import helper_runtime_args
from histdatacom.legacy_boundary import warn_legacy_side_effect
from histdatacom.observability import ProgressState, progress_increment
from histdatacom.runtime_contracts import WorkItem, WorkStatus
from histdatacom.scraper.scraper import Scraper
from histdatacom.utils import normalize_api_return_type

if TYPE_CHECKING:
    from pandas.core.frame import DataFrame
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table

    from histdatacom.records import Record


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

    def __init__(
        self,
        args: Mapping[str, Any] | None = None,
        *,
        return_type: str | None = None,
    ) -> None:
        """Initialize the helper with explicit runtime arguments."""
        self.args: dict[str, Any] = helper_runtime_args(args)
        self.return_type = _resolve_api_return_type(
            return_type or self.args.get("api_return_type")
        )

    @classmethod
    def _create_cache(cls, record: "Record", args: dict) -> None:
        """Create cache file based on single record's data.

        creates a Polars dataframe, saves it in Arrow IPC format
        using and updating relevant information in a Record of work.

        Args:
            record (Record): a histdatacom.records.Record
            args (dict): args received from argparse
        """
        create_cache_file(record, args)

    @classmethod
    def test_for_cache_or_create(cls, record: Record, args: dict) -> None:
        """Test for record's cache file. if it doesn't exist, create it.

           a helper method to ensure the existence of a Record's cache file
           prior to further processing from the API or Influx classes

        Args:
            record (Record): a histdatacom.records.Record
            args (dict): args received from argparse
        """
        warn_legacy_side_effect("Api.test_for_cache_or_create")
        if str.lower(  # noqa:BLK001
            record.data_format
        ) == "ascii" and record.data_timeframe in [
            "T",
            "M1",
        ]:
            output = build_cache_work_item(
                WorkItem.from_record(record),
                args=args,
                download_file=Scraper.get_zip_file,
            )
            apply_stage_output_to_record(output, record)

    @classmethod
    def _validate_cache(
        cls,
        record: Record,
        args: dict,
    ) -> Record | None:
        """Validate Cache prior to possible merge operation.

           Test for or create a Polars cache file based on a Record of Work's
           information. Receives explicit work and returns the record when it is
           ready for further processing.

        Args:
            record (Record): a Histdatacom.records.Record
            args (dict): arguments received from argparse

        """
        output = build_cache_work_item(
            WorkItem.from_record(record),
            args=args,
            download_file=Scraper.get_zip_file,
        )
        apply_stage_output_to_record(output, record)
        return record if output.forward else None

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
            record (Record): a record to import.
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

    def validate_caches(
        self,
        records: list[Record],
        args: Mapping[str, Any] | None = None,
    ) -> list[Record]:
        """Validate explicit records and return cache-ready records."""
        warn_legacy_side_effect("Api.validate_caches")
        runtime_args = helper_runtime_args(self.args, args)
        validated = [
            self._validate_cache(record, runtime_args) for record in records
        ]
        return [record for record in validated if record is not None]

    def merge_caches(
        self,
        records_to_merge: list[Record] | None = None,
        *,
        return_type: str | None = None,
    ) -> list | PolarsDataFrame | DataFrame | Table:
        """Merge caches for start_yearmonth and end_yearmonth range.

        Returns:
            list | PolarsDataFrame | DataFrame | Table:
                merged data for the configured API return type
        """
        warn_legacy_side_effect("Api.merge_caches")
        return self.merge_records(
            records_to_merge or [],
            return_type=return_type,
        )

    def merge_records(
        self,
        records_to_merge: list,
        *,
        return_type: str | None = None,
    ) -> list | PolarsDataFrame | DataFrame | Table:
        """Merge explicit cache records into the configured API return type."""
        if not records_to_merge:
            return []
        resolved_return_type = _resolve_api_return_type(
            return_type or self.return_type
        )

        merge_output = merge_cache_work_items(
            [WorkItem.from_record(record) for record in records_to_merge],
            return_type=resolved_return_type,
            materialize=True,
        )
        if merge_output.result.status is WorkStatus.SKIPPED:
            return []
        return merge_output.data

    def _merge_records(
        self,
        tp_set_dict: dict,
        *,
        return_type: str | None = None,
    ) -> None:
        """Sort and Merge records from a timeframe/pair set.

        Args:
            tp_set_dict (dict): {
                                 "timeframe": timeframe,
                                 "pair": pair,
                                 "records": [],
                                 "data": PolarsDataFrame | None,
                                }
        """
        records_count = len(tp_set_dict["records"])
        if records_count:
            progress_state = ProgressState(
                stage="merge_cache",
                total=float(records_count),
                unit="records",
                status=WorkStatus.COMPLETED,
            )
            with Progress(
                TextColumn(text_format="[cyan]Merging records..."),
                BarColumn(),
                "[progress.percentage]{task.percentage:>3.0f}%",
                TimeElapsedColumn(),
            ) as progress:
                task_id = progress.add_task("merge", total=records_count)

                for _ in tp_set_dict["records"]:
                    event = progress_state.advance(
                        message="Merged cache record."
                    )
                    progress.advance(task_id, progress_increment(event))

        tp_set_dict["data"] = merge_cache_records(
            tp_set_dict["records"],
            return_type=_resolve_api_return_type(
                return_type or self.return_type
            ),
        )

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


def _resolve_api_return_type(return_type: object) -> str:
    normalized = normalize_api_return_type(
        str(return_type) if return_type else None
    )
    return normalized or "polars"
