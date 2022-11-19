from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe

class Options:
    """ An intra-class DTO for Default Arguments for _HistDataCom class. """
    # argparse uses a thin class to create a namespace for cli/shell arguments to live in
    # normally argparse.ArgumentParser.parse_args(namespace=...) creates this namespace and
    # writes user's cli args to it.  Preemptively creating here to hold default args; if the
    # user enters args in the shell, these values will be respectively overwritten
    def __init__(self):
        self.available_remote_data = False
        self.update_remote_data = False
        self.by = "pair_asc"
        self.validate_urls = False
        self.download_data_archives = False
        self.extract_csvs = False
        self.import_to_influxdb = False
        self.pairs = Pairs.list_keys()
        self.formats = Format.list_values()
        self.timeframes = Timeframe.list_keys()
        self.start_yearmonth = ""
        self.end_yearmonth = ""
        self.data_directory = "data"
        self.from_api = False
        self.api_return_type = "datatable"
        self.cpu_utilization = "medium"
        self.batch_size = "5000"
        self.delete_after_influx = False
        self.zip_persist = False
