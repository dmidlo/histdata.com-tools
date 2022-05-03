from histdatacom.concurrency import get_pool_cpu_count
from histdatacom.concurrency import ProcessPool
import sys
import os
import zipfile
from rich import print


class _CSVs:
    def __init__(self, args_, records_current_, records_next_):
        # setting relationship to global outer parent
        self.args = args_

        global records_current
        records_current = records_current_

        global records_next
        records_next = records_next_

    def extract_csv(self, record, args, records_current, records_next):
        try:
            if "CSV_ZIP" in record.status:
                zip_path = record.data_dir + record.zip_filename

                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    [record.csv_filename] = [x for x in zip_ref.namelist() if (".csv" or ".xlsx") in x]
                    zip_ref.extract(record.csv_filename, path=record.data_dir)

                os.remove(zip_path)
                record.status = "CSV_FILE"
                record.write_info_file(base_dir=args['default_download_dir'])
            records_next.put(record)
        except Exception:
            print("Unexpected error:", sys.exc_info())
            record.delete_info_file()
            raise
        finally:
            records_current.task_done()

    def extract_csvs(self, records_current, records_next):

        pool = ProcessPool(self.extract_csv,
                           self.args,
                           "Extracting", "CSVs...",
                           get_pool_cpu_count(self.args['cpu_utilization']))
        
        pool(records_current, records_next)
