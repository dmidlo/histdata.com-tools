import sys
from multiprocessing import managers
from contextlib import contextmanager
from histdatacom.cli import ArgParser
from histdatacom.cli import ArgsNamespace
from histdatacom.records import Records
from histdatacom.utils import set_working_data_dir
from histdatacom.utils import load_influx_yaml
from histdatacom.urls import _URLs
from histdatacom.csvs import _CSVs
from histdatacom.influx import _Influx


class _HistDataCom:
    """A module to pull market data from histdata.com and import it into influxDB"""

    def __init__(self,
                 records_current,
                 records_next,
                 csv_chunks_queue,
                 options):

        """ Initialization for _HistDataCom Class"""
        # Set User () or Default Arguments respectively utilizing the self.ArgParser
        # and self.ArgsNamespace classes.
        #   - ArgParser()():
        #       - ()(): use an IIFE to allow argparse to get garbage collected
        #       - ()(): ArgParser.__call__ returns updated ArgsNamespace object
        #       - vars(...): get the __dict__ representation of the object
        #       - ArgParser._arg_list_to_set(...)
        #           - Normalize iterable user arguments whose values are lists and
        #             make them sets instead
        #       - .copy(): decouple for GC using a hard copy of user args
        self.args = ArgParser._arg_list_to_set(vars(ArgParser(options)())).copy()
        self.args['default_download_dir'] = set_working_data_dir(self.args['data_directory'])
        self.args['queue_filename'] = ".queue"

        if self.args["import_to_influxdb"] == 1:
            influx_yaml = load_influx_yaml()
            self.args['INFLUX_ORG'] = influx_yaml['influxdb']['org']
            self.args['INFLUX_BUCKET'] = influx_yaml['influxdb']['bucket']
            self.args['INFLUX_URL'] = influx_yaml['influxdb']['url']
            self.args['INFLUX_TOKEN'] = influx_yaml['influxdb']['token']

        self.records_current = records_current
        self.records_next = records_next

        self.urls = _URLs(self.args, self.records_current, self.records_next)
        self.csvs = _CSVs(self.args, self.records_current, self.records_next)

        if self.args["import_to_influxdb"] == 1:
            self.csv_chunks_queue = csv_chunks_queue
            self.influx = _Influx(self.args,
                                  self.records_current,
                                  self.records_next,
                                  self.csv_chunks_queue)

    def run(self):
        self.urls.populate_initial_queue(self.records_current, self.records_next)

        if self.args["validate_urls"]:
            self.urls.validate_urls(self.records_current, self.records_next)

        if self.args["download_data_archives"]:
            self.urls.download_zips(self.records_current, self.records_next)

        if self.args["extract_csvs"]:
            self.csvs.extract_csvs(self.records_current, self.records_next)

        if self.args["import_to_influxdb"]:
            self.influx.import_data(self.records_current,
                                    self.records_next,
                                    self.csv_chunks_queue)


class HistDataCom():
    # TODO: presently there is no execution api for calls from other programs.
    # TODO  **kwargs is staged here to pass an ArgsNamespace object into _HistDataCom
    # TODO  for developers to configure
    def __init__(self, options):
        self.options = options
        self.records_manager=managers.SyncManager()
        self.records_manager.register("Records", Records)
    
    @contextmanager
    def __call__(self):
        try:
            self.records_manager.start()

            global records_current
            records_current = self.records_manager.Records()

            global records_next
            records_next = self.records_manager.Records()

            global csv_chunks_queue
            csv_chunks_queue = self.records_manager.Queue()

            scraper = _HistDataCom(records_current,
                                    records_next,
                                    csv_chunks_queue,
                                    self.options)
            scraper.run()

            #self.records_manager.shutdown()
        finally:
            pass
        


def main(options=None):
    if not options:
        options = ArgsNamespace()
    else:
        options.from_api = True

    HistDataCom(options)()


if __name__ == '__main__':
    sys.exit(main())
