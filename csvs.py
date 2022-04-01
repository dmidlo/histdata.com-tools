from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing, sys, os, zipfile
from rich.progress import Progress
import csv

from datetime import datetime
import pytz
from fx_enums import TimeFormat

from rich import print

class _CSVs:

    def __init__(self, args, records_current_, records_next_):
        # setting relationship to global outer parent
        self.args = args
        
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

    def extractCSV(self, record):

        if ("ZIP" in record.status):

            zip_path = record.data_dir + record.zip_filename
            status_elements = record.status.split("_")

            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    [record.csv_filename] = [x for x in zip_ref.namelist() if ".csv" in x]
                    zip_ref.extract(record.csv_filename, path=record.data_dir)

                os.remove(zip_path)
            except:
                print("Unexpected error:", sys.exc_info()[0])
                record.status = f"CSV_{status_elements[1]}_ZIP_FAIL"
                record.delete_info_file()
                records_next.put(record)
                raise
            else:
                record.status = f"CSV_{status_elements[1]}_FILE"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
                records_current.task_done()
                return
        else:
            records_next.put(record)

        records_current.task_done()

    def extractCSVs(self, records_current, records_next):
        with Progress() as progress:
            records_count = records_current.qsize()
            task_id = progress.add_task(f"[cyan]Extracting {records_count} CSVs...", total=records_count)
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

                    future = executor.submit(self.extractCSV, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)
                
                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    records_current.task_done()
                    futures.remove(future)
                    del future

        records_current.join()

        records_next.dump_to_queue(records_current)
        records_current.write_pickle(f"{self.args['working_data_directory']}/{self.args['queue_filename']}")
    
    def cleanCSV(self, record):
        
        if ("FILE" in record.status):
            csv_path = record.data_dir + record.csv_filename
            temp_csv_path = record.data_dir + "temp.csv"
            status_elements = record.status.split("_")

            try:
                header = ["Source", "Platform", "Instrument","Timeframe","msSinceEpochUTC","BidQuote","AskQuote","Volume"]
                
                with open(temp_csv_path, 'w', newline="") as destcsv:
                    dest_csv_writer = csv.writer(destcsv)
                    dest_csv_writer.writerow(header)

                    with open(csv_path, "r") as srccsv:
                        dialect = csv.Sniffer().sniff(srccsv.read(), delimiters=",; ")
                        srccsv.seek(0)
                        src_csv_reader = csv.reader(srccsv, dialect)

                        for row in src_csv_reader:
                            timestamp_utc = self.convert_datetime(row[0])

                            row_data = ["histdata.com", record.data_platform, record.data_fxpair, record.data_timeframe] + timestamp_utc + row[1:]
                            dest_csv_writer.writerow(row_data)

                os.remove(csv_path)
                os.rename(temp_csv_path, csv_path)

            except:
                print("Unexpected error:", sys.exc_info()[0])
                record.status = f"CSV_{status_elements[1]}_CLEAN_FAIL"
                record.delete_info_file()
                records_next.put(record)
                raise

            finally:
                record.status = f"CSV_{status_elements[1]}_CLEAN"
                record.write_info_file(base_dir=args['default_download_dir'])
                records_next.put(record)
                records_current.task_done()
                return
        else:
            records_next.put(record)

        records_current.task_done()


    def cleanCSVs(self, records_current, records_next):
        with Progress() as progress:
            records_count = records_current.qsize()
            task_id = progress.add_task(f"[cyan]Cleaning {records_count} CSVs...", total=records_count)
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

                    future = executor.submit(self.cleanCSV, record)
                    progress.advance(task_id, 0.25)
                    futures.append(future)
                
                for future in as_completed(futures):
                    progress.advance(task_id, 0.75)
                    records_current.task_done()
                    futures.remove(future)
                    del future

        records_current.join()

        records_next.dump_to_queue(records_current)
        records_current.write_pickle(f"{self.args['working_data_directory']}/{self.args['queue_filename']}")

    @classmethod
    def convert_datetime(cls, est_timestamp):

        date_object = datetime.strptime(est_timestamp, TimeFormat.ASCII_T.value)
        tz_date_object = date_object.replace(tzinfo=pytz.timezone("Etc/GMT+5"))
        utc_milli_timestamp = int(tz_date_object.timestamp() * 1000)

        return utc_milli_timestamp