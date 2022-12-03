"""Extract CSVs from zip archive."""
from __future__ import annotations

import sys
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from rich import print  # pylint: disable=redefined-builtin

from histdatacom import config
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count

if TYPE_CHECKING:
    from histdatacom.records import Record, Records


class Csv:  # noqa:H601
    """Extract CSV documents from zip archives."""

    def extract_csvs(self) -> None:
        """Execute process pool with extract_csv."""
        pool = ProcessPool(
            self._extract_csv,
            config.ARGS,
            "Extracting",
            "CSVs...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)

    def _extract_csv(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
    ) -> None:
        """Extract single csv file. Called by extract_csvs.

        # noqa: DAR402

        Args:
            record (Record): a record from the work queue.
            args (dict): from config.ARGS
            records_current (Records): config.CURRENT_QUEUE
            records_next (Records): config.NEXT_QUEUE

        Raises:
            OSError: OS Error.
            SystemExit: exit on error
        """
        try:
            if "CSV_ZIP" in record.status:
                zip_path = Path(record.data_dir, record.zip_filename)

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    [record.csv_filename] = [
                        name
                        for name in zip_ref.namelist()
                        if (".csv" or ".xlsx") in name
                    ]
                    zip_ref.extract(record.csv_filename, path=record.data_dir)

                zip_path.unlink()
                record.status = "CSV_FILE"
                record.write_memento_file(base_dir=args["default_download_dir"])
            records_next.put(record)
        except OSError as err:
            print("Unexpected error:", sys.exc_info())  # noqa:T201
            record.delete_momento_file()
            raise SystemExit from err
        finally:
            records_current.task_done()
