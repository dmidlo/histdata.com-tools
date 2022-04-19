from multiprocessing import managers
from histdatacom.cli import ArgParser
from histdatacom.records import Records
from histdatacom.utils import set_working_data_dir, load_influx_yaml
from histdatacom.urls import _URLs
from histdatacom.csvs import _CSVs
from histdatacom.influx import _Influx
import sys

# !Manually Applied Backport of:
# https://github.com/python/cpython/commit/8aa45de6c6d84397b772bad7e032744010bbd456
# Improvements to the Manager / proxied shared values code
# broke handling of proxied objects without a custom proxy type,
# as the AutoProxy function was not updated.
#! Will need to do this everytime venv is recreated.

class _HistDataCom:
    """A module to pull market data from histdata.com and import it into influxDB"""

    def __init__(self, records_current,
                        records_next,
                        csv_chunks_queue,
                        **kwargs): 

        """ Initialization for _HistDataCom Class"""
        # Set User () or Default Arguments respectively utilizing the self.ArgParser and self.ArgsNamespace classes.
        #   - ArgParser()():
        #       - ()(): use an IIFE to allow argparse to get garbage collected
        #       - ()(): ArgParser.__call__ returns updated ArgsNamespace object
        #       - vars(...): get the __dict__ representation of the object
        #       - ArgParser._arg_list_to_set(...)
        #           - Normalize iterable user arguments whose values are lists and make them sets instead
        #       - .copy(): decouple for GC using a hard copy of user args
        self.args = ArgParser._arg_list_to_set(vars(ArgParser()())).copy()
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

        if self.args["clean_data"] or self.args["import_to_influxdb"]:
            self.csvs.clean_data(self.records_current, self.records_next)

        if self.args["import_to_influxdb"]:
            self.influx.import_data(self.records_current,
                                    self.records_next,
                                    self.csv_chunks_queue)

class HistDataCom():
    # TODO: presently there is no execution api for calls from other programs.
    # TODO  **kwargs is staged here to pass an ArgsNamespace object into _HistDataCom
    # TODO  for developers to configure
    def __init__(self, **kwargs):
        pass
    
    def __call__(self,
        records_manager = managers.SyncManager(),
        **kwargs):

        records_manager.register("Records", Records)
        records_manager.start()

        global records_current
        records_current = records_manager.Records()

        global records_next
        records_next = records_manager.Records()

        global csv_chunks_queue
        csv_chunks_queue = records_manager.Queue()

        scraper = _HistDataCom(records_current, 
                                records_next,
                                csv_chunks_queue)
        scraper.run()

def main():
    HistDataCom()()

if __name__ == '__main__':    
    sys.exit(main())