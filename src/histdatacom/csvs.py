import sys
import os
import zipfile

from rich import print  # pylint: disable=redefined-builtin

from histdatacom import config

from histdatacom.records import Record
from histdatacom.records import Records

from histdatacom.concurrency import ProcessPool
from histdatacom.concurrency import get_pool_cpu_count


class Csv:
    def extract_csv(
        self,
        record: Record,
        args: dict,
        records_current: Records,
        records_next: Records,
    ) -> None:
        try:
            if "CSV_ZIP" in record.status:
                zip_path = record.data_dir + record.zip_filename

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    [record.csv_filename] = [
                        x for x in zip_ref.namelist() if (".csv" or ".xlsx") in x
                    ]
                    zip_ref.extract(record.csv_filename, path=record.data_dir)

                os.remove(zip_path)
                record.status = "CSV_FILE"
                record.write_info_file(base_dir=args["default_download_dir"])
            records_next.put(record)
        except Exception:
            print("Unexpected error:", sys.exc_info())
            record.delete_info_file()
            raise
        finally:
            records_current.task_done()

    def extract_csvs(self) -> None:

        pool = ProcessPool(
            self.extract_csv,
            config.ARGS,
            "Extracting",
            "CSVs...",
            get_pool_cpu_count(config.ARGS["cpu_utilization"]),
        )

        pool(config.CURRENT_QUEUE, config.NEXT_QUEUE)
