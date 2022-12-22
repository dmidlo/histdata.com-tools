"""Records queue and Record work object for queue."""
import contextlib
from dataclasses import dataclass
import json
import os
from pathlib import Path
from queue import Queue
from typing import Any, Optional

from rich import print  # pylint: disable=redefined-builtin

from histdatacom import config
from histdatacom.fx_enums import Format, Timeframe
from histdatacom.utils import (
    create_full_path,
    get_query_string,
)

# Testing order todo.
#
# Leaf Functions - Test these first
#   - These functions call nothing else
#   - single-unit tests.
#
# TODO: Record __init__()
# TODO: Record _to_dict()
# TODO: Record delete_momento_file()
# TODO: Records __init__()
# TODO: Records dump_to_queue()

# Regular Functions
#   - These functions call other functions
#   - single-unit and integration/multi-unit tests
#
# TODO: Record _set_record_data_dir()
# TODO: Record _create_record_data_dir()
# TODO: Record restore_momento()
# TODO: Record write_memento_file()
# TODO: Record __call__()


@dataclass
class Record:  # noqa:H601
    """A work record for the queue."""

    url: str = ""
    status: str = ""
    encoding: str = ""
    bytes_length: str = ""
    data_date: str = ""
    data_year: str = ""
    data_month: str = ""
    data_datemonth: str = ""
    data_format: str = ""
    data_timeframe: str = ""
    data_fxpair: str = ""
    data_dir: str = ""
    data_tk: str = ""
    zip_filename: str = ""
    csv_filename: str = ""
    jay_filename: str = ""
    jay_line_count: str = ""
    jay_start: str = ""
    jay_end: str = ""
    zip_persist: str = ""

    def write_memento_file(self, base_dir: str = "") -> None:
        """Write record to disk.

        # noqa: DAR402

        Args:
            base_dir (str): Defaults to "".

        Raises:
            ValueError: function requires base_dir.
            SystemExit: Exit on error.
        """
        try:
            if self.data_dir == "":
                if base_dir:
                    self._create_record_data_dir(base_dir=base_dir)
                else:
                    raise ValueError

            if not Path(self.data_dir).exists():
                create_full_path(self.data_dir)

            momento_path = Path(self.data_dir, config.INFO_FILE_NAME)

            with momento_path.open("w", encoding="UTF-8") as target:
                json.dump(self._to_dict(), target)

        except ValueError as err:
            print(  # noqa:T201,BLK100
                "Error: create_record_data_dir not provided base_dir="
            )
            raise SystemExit from err

    def delete_momento_file(self) -> None:
        """Delete memento file."""
        momento_path = Path(self.data_dir, config.INFO_FILE_NAME)
        if momento_path.exists():
            momento_path.unlink()

    def restore_momento(self, base_dir: str) -> bool:
        """Restore momento from file.

        Args:
            base_dir (str): base data directory

        Returns:
            bool: True (success) | False (failure)
        """
        self._set_record_data_dir(base_dir)

        momento_path = Path(self.data_dir, config.INFO_FILE_NAME)
        if not momento_path.exists():
            return False
        record_dict: dict = {}

        with (
            momento_path.open(  # noqa:BLK100
                "r", encoding="UTF-8"
            ) as json_read,
            contextlib.suppress(Exception),
        ):
            while True:
                record_dict |= json.load(json_read)

        self(**record_dict)
        return True

    def _to_dict(self) -> dict:
        """Return dict representation of Record.

        Returns:
            dict: dict representation of Record.
        """
        return {
            "url": self.url,
            "status": self.status,
            "encoding": self.encoding,
            "bytes_length": self.bytes_length,
            "data_date": self.data_date,
            "data_year": self.data_year,
            "data_month": self.data_month,
            "data_datemonth": self.data_datemonth,
            "data_format": self.data_format,
            "data_timeframe": self.data_timeframe,
            "data_fxpair": self.data_fxpair,
            "data_dir": self.data_dir,
            "data_tk": self.data_tk,
            "zip_filename": self.zip_filename,
            "csv_filename": self.csv_filename,
            "jay_line_count": self.jay_line_count,
            "jay_start": self.jay_start,
            "jay_end": self.jay_end,
            "jay_filename": self.jay_filename,
            "zip_persist": self.zip_persist,
        }

    def _set_record_data_dir(  # noqa:CFQ004,BLK100
        self, base_dir: Optional[str]
    ) -> str:
        """Set Record's data directory.

        Args:
            base_dir (Optional[str]): base data directory.

        Returns:
            str: self.data_dir  # record's data.
        """
        query_string_args = get_query_string(self.url)
        length = len(query_string_args)

        csv_format = Format(query_string_args[1]).name
        timeframe = Timeframe(query_string_args[2]).name

        record_data_dir = f"{base_dir}{csv_format}{os.sep}{timeframe}{os.sep}"

        if length == 3:
            self.data_dir = record_data_dir
            return self.data_dir

        pair = query_string_args[3]
        record_data_dir = record_data_dir + pair.lower() + os.sep

        if length == 4:
            self.data_dir = record_data_dir
            return self.data_dir

        year = query_string_args[4]
        record_data_dir = record_data_dir + year + os.sep

        if length == 5:
            self.data_dir = record_data_dir
            return self.data_dir

        month = query_string_args[5]
        record_data_dir = record_data_dir + month + os.sep

        if length == 6:
            self.data_dir = record_data_dir
            return self.data_dir

        return self.data_dir

    def _create_record_data_dir(self, base_dir: str = "") -> None:
        """Create Record's data directory and populate its attribute.

        # noqa: DAR402

        Args:
            base_dir (str): Defaults to "".

        Raises:
            ValueError: no base_dir provided.
            SystemExit: exit on error.
        """
        try:
            if self.data_dir != "":
                create_full_path(self.data_dir)
            elif base_dir != "":
                create_full_path(self._set_record_data_dir(base_dir))
            else:
                raise ValueError
        except ValueError as err:
            print(  # noqa:BLK100,T201
                "Error: create_record_data_dir not provided base_dir="
            )
            raise SystemExit from err

    def __call__(self, **kwargs: str) -> Any:
        """Set instance attribute by kwargs.

        Args:
            kwargs (str): instance attributes

        Returns:
            Any: self.
        """
        for arg_name, arg_value in kwargs.items():
            setattr(self, arg_name, arg_value)
        return self


class Records(Queue):  # noqa:H601
    """Custom Queue class for Records and SyncManager."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Inherit and initialize from queue.Queue.

        Args:
            args (Any): for compatibility with queue.Queue
            kwargs (Any): for compatibility with queue.Queue
        """
        Queue.__init__(self, *args, **kwargs)

    def dump_to_queue(  # noqa:CCR001
        self, dst_queue: Queue, count: int | None = 0
    ) -> None:
        """Transfer queue contents from one queue to another.

        Args:
            dst_queue (Queue): destination queue
            count (int): Dump N number of records. Defaults to 0.
        """
        if count == 0:
            count = None
        else:
            counter = 0

        while not self.empty():
            record = self.get()

            if record is None:
                break

            if count is None:
                dst_queue.put(record)
            elif counter < count:
                dst_queue.put(record)
                counter += 1
            else:
                self.put(record)
                break
