"""Record work object used by foreground, sidecar, and cache code."""

import json
import os
from pathlib import Path
from typing import Any

from rich import print  # pylint: disable=redefined-builtin

from histdatacom.fx_enums import Format, Timeframe
from histdatacom.manifest_store import (
    ManifestStatusStore,
    delete_record_from_manifest,
    restore_record_from_manifest,
)
from histdatacom.utils import (
    create_full_path,
    get_query_string,
)


class Record:  # noqa:H601
    """A mutable work record DTO."""

    def __init__(self, **kwargs: str) -> None:
        """Initialize record attributes.

        Args:
            kwargs (str): record attributes
        """
        self.url = kwargs.get("url", "")
        self.status = kwargs.get("status", "")
        self.encoding = kwargs.get("encoding", "")
        self.bytes_length = kwargs.get("bytes_length", "")
        self.data_date = kwargs.get("data_date", "")
        self.data_year = kwargs.get("data_year", "")
        self.data_month = kwargs.get("data_month", "")
        self.data_datemonth = kwargs.get("data_datemonth", "")
        self.data_format = kwargs.get("data_format", "")
        self.data_timeframe = kwargs.get("data_timeframe", "")
        self.data_fxpair = kwargs.get("data_fxpair", "")
        self.data_dir = kwargs.get("data_dir", "")
        self.data_tk = kwargs.get("data_tk", "")
        self.zip_filename = kwargs.get("zip_filename", "")
        self.csv_filename = kwargs.get("csv_filename", "")
        self.cache_filename = kwargs.get("cache_filename", "")
        self.cache_line_count = kwargs.get("cache_line_count", "")
        self.cache_start = kwargs.get("cache_start", "")
        self.cache_end = kwargs.get("cache_end", "")
        self.zip_persist = kwargs.get("zip_persist", "")

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

            ManifestStatusStore(base_dir or self.data_dir).write_record(self)
            momento_path = Path(self.data_dir, ".meta")

            with momento_path.open("w", encoding="UTF-8") as target:
                json.dump(self._to_dict(), target)

        except ValueError as err:
            print(  # noqa:T201,BLK100
                "Error: create_record_data_dir not provided base_dir="
            )
            raise SystemExit(1) from err

    def delete_momento_file(self, base_dir: str = "") -> None:
        """Delete memento file."""
        momento_path = Path(self.data_dir, ".meta")
        if momento_path.exists():
            momento_path.unlink()
        delete_record_from_manifest(self, base_dir=base_dir)

    def restore_momento(self, base_dir: str) -> bool:
        """Restore momento from .meta file.

        Args:
            base_dir (str): base data directory

        Returns:
            bool: True (success) | False (failure)
        """
        self._set_record_data_dir(base_dir)
        return bool(restore_record_from_manifest(self, base_dir=base_dir))

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
            "data_tk": self.data_tk,
            "zip_filename": self.zip_filename,
            "csv_filename": self.csv_filename,
            "cache_line_count": self.cache_line_count,
            "cache_start": self.cache_start,
            "cache_end": self.cache_end,
            "cache_filename": self.cache_filename,
            "zip_persist": self.zip_persist,
        }

    def _set_record_data_dir(self, base_dir: str) -> str:  # noqa:CFQ004
        """Set Record's data directory.

        Args:
            base_dir (str): base data directory.

        Returns:
            str: self.data_dir  # record's data.
        """
        query_string_args = get_query_string(self.url)
        length = len(query_string_args)

        csv_format = Format(query_string_args[1]).name
        timeframe = Timeframe(query_string_args[2]).name

        record_data_path = Path(base_dir) / csv_format / timeframe
        record_data_dir = f"{record_data_path}{os.sep}"

        if length == 3:
            self.data_dir = record_data_dir
            return self.data_dir

        pair = query_string_args[3]
        record_data_path = record_data_path / pair.lower()
        record_data_dir = f"{record_data_path}{os.sep}"

        if length == 4:
            self.data_dir = record_data_dir
            return self.data_dir

        year = query_string_args[4]
        record_data_path = record_data_path / year
        record_data_dir = f"{record_data_path}{os.sep}"

        if length == 5:
            self.data_dir = record_data_dir
            return self.data_dir

        month = query_string_args[5]
        record_data_path = record_data_path / month
        record_data_dir = f"{record_data_path}{os.sep}"

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
            raise SystemExit(1) from err

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
