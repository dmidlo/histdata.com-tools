import sys
from histdatacom.cli import ArgParser
from histdatacom.options import Options
from histdatacom.utils import set_working_data_dir
from histdatacom.utils import load_influx_yaml
from histdatacom.urls import _URLs
from histdatacom.api import _API
from histdatacom.csvs import _CSVs
from histdatacom.influx import _Influx
from histdatacom.concurrency import QueueManager


class _HistDataCom:
    """A module to pull market data from histdata.com and import it into influxDB"""

    def __init__(self,
                 records_current,
                 records_next,
                 csv_chunks_queue,
                 options):

        """ Initialization for _HistDataCom Class"""
        # Set User () or Default Arguments respectively utilizing the self.ArgParser
        # and self.Options classes.
        #   - ArgParser()():
        #       - ()(): use an IIFE to allow argparse to get garbage collected
        #       - ()(): ArgParser.__call__ returns updated Options object
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
        self.api = _API(self.args, self.records_current, self.records_next)

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
            if self.args["from_api"]:
                self.api.validate_jays(self.records_current, self.records_next)
                return self.api.merge_jays(self.records_current, self.records_next)

        if self.args["extract_csvs"]:
            self.csvs.extract_csvs(self.records_current, self.records_next)

        if self.args["import_to_influxdb"]:
            self.influx.import_data(self.records_current,
                                    self.records_next,
                                    self.csv_chunks_queue)


def main(options=None):
    if not options:
        options = Options()
        QueueManager(options)(_HistDataCom)
    else:
        options.from_api = True
        return QueueManager(options)(_HistDataCom)


if __name__ == '__main__':
    sys.exit(main())
