"""Options object for histdatacom."""

from dataclasses import dataclass, field

from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe


@dataclass
class Options:
    """An intra-class DTO for Default Arguments for _HistDataCom class."""

    # argparse uses a thin class to create a namespace for cli/shell arguments
    # to live in normally argparse.ArgumentParser.parse_args(namespace=...)
    # creates this namespace and writes user's cli args to it.  Preemptively
    # creating here to hold default args; if the user enters args in the shell,
    # these values will be respectively overwritten
    version: bool = False
    available_remote_data: bool = False
    update_remote_data: bool = False
    by: str = "pair_asc"  # pylint: disable=invalid-name
    validate_urls: bool = False
    download_data_archives: bool = False
    extract_csvs: bool = False
    import_to_influxdb: bool = False
    pairs: set = field(default_factory=set)
    formats: set = field(default_factory=set)
    timeframes: set = field(default_factory=set)
    start_yearmonth: str | None = ""
    end_yearmonth: str | None = ""
    data_directory: str = "data"
    from_api: bool = False
    api_return_type: str | None = None
    cpu_utilization: str = "medium"
    batch_size: str = "5000"
    delete_after_influx: bool = False
    zip_persist: bool = False

    def __post_init__(self) -> None:
        """Populate default sets for Pairs, Formats, and Timeframes."""
        self.pairs = Pairs.list_keys()
        self.formats = Format.list_values()
        self.timeframes = Timeframe.list_keys()
