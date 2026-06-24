"""Options object for histdatacom."""

from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe

_REMOVED_ORCHESTRATION_OPTION_REPLACEMENTS = {
    "use_sidecar": "use_orchestration",
    "sidecar_start": "orchestration_start",
    "sidecar_wait_result": "orchestration_wait_result",
}


class Options:
    """An intra-class DTO for Default Arguments for _HistDataCom class."""

    def __setattr__(self, name: str, value: object) -> None:
        """Reject removed public option names before they become stale state."""
        replacement = _REMOVED_ORCHESTRATION_OPTION_REPLACEMENTS.get(name)
        if replacement is not None:
            raise AttributeError(
                f"Options.{name} has been removed; use "
                f"Options.{replacement}."
            )
        super().__setattr__(name, value)

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
        self.import_to_influxdb: bool = False
        self.pairs: set = Pairs.list_keys()
        self.formats: set = Format.list_values()
        self.timeframes: set = Timeframe.list_keys()
        self.start_yearmonth: str | None = ""
        self.end_yearmonth: str | None = ""
        self.data_directory: str = "data"
        self.from_api: bool = False
        self.api_return_type: str | None = None
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
        self.quality_profile_path: str | None = None
        self.quality_profile: dict = {}
        self.repo_quality_refresh: bool = False
        self.repo_quality_columns: bool = False
        self.use_orchestration: bool = True
        self.orchestration_start: bool = True
        self.orchestration_wait_result: bool = True
        self.metadata: dict = {}
