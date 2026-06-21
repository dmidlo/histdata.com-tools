"""Extract CSVs from zip archive."""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

from rich import print  # pylint: disable=redefined-builtin

from histdatacom import config
from histdatacom.concurrency import ProcessPool, get_pool_cpu_count
from histdatacom.runtime_contracts import WorkStatus

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
            if WorkStatus.CSV_ZIP.value in record.status:
                zip_path = Path(record.data_dir, record.zip_filename)

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    data_members = [
                        name
                        for name in zip_ref.namelist()
                        if name.lower().endswith((".csv", ".xlsx"))
                    ]
                    if len(data_members) != 1:
                        raise ValueError(
                            "expected ZIP archive to contain one CSV/XLSX file"
                        )
                    [record.csv_filename] = data_members
                    zip_ref.extract(record.csv_filename, path=record.data_dir)

                zip_path.unlink()
                record.status = WorkStatus.CSV_FILE.value
                record.write_memento_file(base_dir=args["default_download_dir"])
            records_next.put(record)
        except (OSError, ValueError) as err:
            print("Unexpected error:", sys.exc_info())  # noqa:T201
            record.delete_momento_file()
            raise SystemExit(1) from err
        finally:
            records_current.task_done()
