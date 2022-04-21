from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import multiprocessing
import sys
import os
import zipfile
from rich.progress import Progress
from rich.progress import TextColumn
from rich.progress import BarColumn
from rich.progress import TimeElapsedColumn
from rich import print


class _CSVs:
    def __init__(self, args_, records_current_, records_next_):
        # setting relationship to global outer parent
        self.args = args_

        global records_current
        records_current = records_current_

        global records_next
        records_next = records_next_

    def init_counters(self, records_current_, records_next_, args_):
        global records_current
        records_current = records_current_
        global records_next
        records_next = records_next_
        global args
        args = args_

    def extract_csv(self, record):
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

        records_count = records_current.qsize()
        with Progress(TextColumn(text_format=f"[cyan]Extracting {records_count} CSVs..."),
                      BarColumn(),
                      "[progress.percentage]{task.percentage:>3.0f}%",
                      TimeElapsedColumn()) as progress:

            task_id = progress.add_task("[cyan]Extracting CSVs", total=records_count)
            with ProcessPoolExecutor(max_workers=(multiprocessing.cpu_count() - 1),
                                     initializer=self.init_counters,
                                     initargs=(records_current,
                                     records_next,
                                     self.args.copy())) as executor:
                futures = []

                while not records_current.empty():
                    record = records_current.get()

                    if record is None:
                        return

                    future = executor.submit(self.extract_csv, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)

                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    futures.remove(future)
                    del future

        records_current.join()
        records_next.dump_to_queue(records_current)
