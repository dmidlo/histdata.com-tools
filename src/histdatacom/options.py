"""Options object for histdatacom."""

from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe


class Options:
    """An intra-class DTO for Default Arguments for _HistDataCom class."""

    # argparse uses a thin class to create a namespace for cli/shell arguments
    # to live in normally argparse.ArgumentParser.parse_args(namespace=...)
    # creates this namespace and writes user's cli args to it.  Preemptively
    # creating here to hold default args; if the user enters args in the shell,
    # these values will be respectively overwritten
    def __init__(self) -> None:
        """Initialize attributes with default values."""
        self.available_remote_data: bool = False
        self.update_remote_data: bool = False
        self.by: str = "pair_asc"  # pylint: disable=invalid-name
        self.validate_urls: bool = False
        self.download_data_archives: bool = False
        self.extract_csvs: bool = False
        self.import_to_influxdb: bool = False
        self.pairs: set = Pairs.list_keys()
        self.formats: set = Format.list_values()
        self.timeframes: set = Timeframe.list_keys()
        self.start_yearmonth: str | None = ""
        self.end_yearmonth: str | None = ""
        self.data_directory: str = "data"
        self.from_api: bool = False
        self.api_return_type: str = "datatable"
        self.cpu_utilization: str = "medium"
        self.batch_size: str = "5000"
        self.delete_after_influx: bool = False
        self.zip_persist: bool = False
