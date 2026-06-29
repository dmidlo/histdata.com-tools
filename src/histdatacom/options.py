"""Options object for histdatacom."""

from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe


class Options:
    """An intra-class DTO for Default Arguments for _HistDataCom class."""

    __slots__ = (
        "api_return_type",
        "available_remote_data",
        "batch_size",
        "build_cache",
        "by",
        "config_path",
        "cpu_utilization",
        "data_directory",
        "data_quality",
        "delete_after_influx",
        "download_data_archives",
        "end_yearmonth",
        "extract_csvs",
        "formats",
        "from_api",
        "import_to_influxdb",
        "metadata",
        "orchestration_start",
        "orchestration_keep_runtime",
        "orchestration_wait_result",
        "pair_groups",
        "pairs",
        "quality_check_groups",
        "quality_fail_on",
        "quality_max_errors",
        "quality_max_warnings",
        "quality_preflight",
        "quality_preflight_evidence_allow_stale",
        "quality_preflight_evidence_max_age_seconds",
        "quality_preflight_evidence_path",
        "quality_preflight_report_path",
        "quality_preflight_sample_size",
        "quality_paths",
        "quality_profile",
        "quality_profile_path",
        "quality_report_path",
        "repo_quality_columns",
        "repo_quality_refresh",
        "start_yearmonth",
        "timeframes",
        "update_remote_data",
        "use_orchestration",
        "validate_urls",
        "verbosity",
        "version",
        "zip_persist",
    )

    # argparse uses a thin class to create a namespace for cli/shell arguments
    # to live in normally argparse.ArgumentParser.parse_args(namespace=...)
    # creates this namespace and writes user's cli args to it.  Preemptively
    # creating here to hold default args; if the user enters args in the shell,
    # these values will be respectively overwritten
    def __init__(self) -> None:
        """Initialize attributes with default values."""
        self.version: bool = False
        self.available_remote_data: bool = False
        self.update_remote_data: bool = False
        self.by: str = "pair_asc"  # pylint: disable=invalid-name
        self.validate_urls: bool = False
        self.download_data_archives: bool = False
        self.extract_csvs: bool = False
        self.build_cache: bool = False
        self.import_to_influxdb: bool = False
        self.pair_groups: set = set()
        self.pairs: set = Pairs.list_keys()
        self.formats: set = Format.list_values()
        self.timeframes: set = Timeframe.list_keys()
        self.start_yearmonth: str | None = ""
        self.end_yearmonth: str | None = ""
        self.data_directory: str = "data"
        self.from_api: bool = False
        self.api_return_type: str | None = None
        self.config_path: str | None = None
        self.cpu_utilization: str = "medium"
        self.batch_size: str = "5000"
        self.delete_after_influx: bool = False
        self.zip_persist: bool = False
        self.data_quality: bool = False
        self.quality_paths: tuple[str, ...] = ()
        self.quality_check_groups: set[str] = {"all"}
        self.quality_report_path: str | None = None
        self.quality_fail_on: str = "error"
        self.quality_max_errors: int = 0
        self.quality_max_warnings: int = 0
        self.quality_preflight: bool = False
        self.quality_preflight_evidence_allow_stale: bool = False
        self.quality_preflight_evidence_max_age_seconds: int = 86400
        self.quality_preflight_evidence_path: str | None = None
        self.quality_preflight_sample_size: int = 4
        self.quality_preflight_report_path: str | None = None
        self.quality_profile_path: str | None = None
        self.quality_profile: dict = {}
        self.repo_quality_refresh: bool = False
        self.repo_quality_columns: bool = False
        self.use_orchestration: bool = True
        self.orchestration_start: bool = True
        self.orchestration_keep_runtime: bool = False
        self.orchestration_wait_result: bool = True
        self.verbosity: int = 0
        self.metadata: dict = {}

    def to_dict(self) -> dict[str, object]:
        """Return a mutable mapping of option names to current values."""
        return {name: getattr(self, name) for name in self.__slots__}
