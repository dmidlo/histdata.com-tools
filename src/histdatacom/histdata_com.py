"""Run main application. Core logic.

Raises:
    SystemExit: Exit when complete.

Returns:
    QueueManager: multi-process serial communication manager
    repo_data (set): a set of repo pairs with start and end date ranges.
    Data (PolarsDataFrame | DataFrame | Table):
        a Polars DataFrame, pandas DataFrame, or pyarrow Table
    List Of Data:   [
                        {
                            "timeframe": timeframe,
                            "pair": pair,
                            "records": [record, record, ...],
                            "data": PolarsDataFrame | DataFrame | Table,
                        },
                        ...
                        ...
                    ]

"""

from __future__ import annotations

import json
import sys
from typing import TYPE_CHECKING

import histdatacom
from histdatacom import Options, config
from histdatacom.cli import ArgParser
from histdatacom.concurrency import QueueManager
from histdatacom.csvs import Csv
from histdatacom.runtime_contracts import RunRequest
from histdatacom.scraper.repo import Repo
from histdatacom.scraper.scraper import Scraper
from histdatacom.sidecar.client import (
    SidecarUnavailableError,
    submit_run_request_and_observe_sync,
)
from histdatacom.utils import (
    load_influx_yaml,
    set_working_data_dir,
    check_installed_module,
    normalize_api_return_type,
)

if TYPE_CHECKING:
    from pandas import DataFrame
    from polars import DataFrame as PolarsDataFrame
    from pyarrow import Table


class _HistDataCom:  # noqa:R701
    """Pull market data from histdata.com and import it into influxDB."""

    def __init__(self, options: Options) -> None:  # noqa:CCR001
        # pylint: disable=import-outside-toplevel
        """Initialize _HistDataCom Class.

        Args:
            options (Options): from histdata.options import Options

        Set User () or Default Arguments respectively utilizing the
        self.ArgParser and self.Options classes.
          - ArgParser()():
              - ()(): use an IIFE to allow argparse to get garbage collected
              - ()(): ArgParser.__call__ returns updated Options object
              - vars(...): get the __dict__ representation of the object
              - ArgParser._arg_list_to_set(...)
                  - Normalize iterable user arguments whose values are lists and
                    make them sets instead
          - .copy(): decouple for GC using a hard copy of user args
        """
        self.options = ArgParser(options)()
        config.ARGS = ArgParser.arg_list_to_set(  # noqa:BLK100
            vars(self.options)  # noqa:WPS110
        ).copy()
        config.ARGS["default_download_dir"] = set_working_data_dir(
            config.ARGS["data_directory"]
        )
        config.ARGS["api_return_type"] = normalize_api_return_type(
            config.ARGS["api_return_type"]
        )
        self.options.api_return_type = config.ARGS["api_return_type"]

        if config.ARGS["version"] or self._uses_sidecar():
            return

        if config.ARGS["import_to_influxdb"]:
            check_installed_module("influxdb_client")
            influx_yaml = load_influx_yaml()
            config.ARGS["INFLUX_ORG"] = influx_yaml["influxdb"]["org"]
            config.ARGS["INFLUX_BUCKET"] = influx_yaml["influxdb"]["bucket"]
            config.ARGS["INFLUX_URL"] = influx_yaml["influxdb"]["url"]
            config.ARGS["INFLUX_TOKEN"] = influx_yaml["influxdb"]["token"]

        self.repo = Repo()
        self.scraper = Scraper()
        self.csvs = Csv()

        if (
            config.ARGS["from_api"]
            and config.ARGS["api_return_type"]
            and not config.ARGS["version"]
            and not config.ARGS["available_remote_data"]
            and not config.ARGS["update_remote_data"]
        ):
            check_installed_module(config.ARGS["api_return_type"])
            from histdatacom.api import Api

            self.api = Api()

        if config.ARGS["import_to_influxdb"]:
            config.ARGS["api_return_type"] = "polars"
            check_installed_module(config.ARGS["api_return_type"])
            from histdatacom.influx import Influx

            self.influx = Influx()

        if (  # noqa:BLK100
            config.ARGS["available_remote_data"]  # noqa:BLK100
            or config.ARGS["update_remote_data"]
        ):
            if self.repo.test_for_repo_data_file():
                self.repo.read_repo_data_file()
            self.repo.update_repo_from_github()

    def run(  # noqa:CCR001,CFQ004,CCR001,R701
        self,
    ) -> list | dict | PolarsDataFrame | DataFrame | Table | None:
        """Execute. histdatacom's execution order.

        Returns:
            list | dict | PolarsDataFrame | DataFrame | Table | None:

            Data (PolarsDataFrame | DataFrame | Table):
                    a Polars DataFrame, pandas DataFrame, or pyarrow Table.
            List of dicts:  [
                                {
                                    "timeframe": timeframe,
                                    "pair": pair,
                                    "records": [record, record, ...],
                                    "data": PolarsDataFrame | DataFrame | Table,
                                },
                                ...
                                ...
                            ]


        """
        if config.ARGS["version"]:
            if not config.ARGS["from_api"]:
                print(histdatacom.__version__)  # noqa:T201
            return histdatacom.__version__

        if self._uses_sidecar():
            return self._run_sidecar_job()

        if (  # noqa:BLK100
            config.ARGS["available_remote_data"]  # noqa:BLK100
            or config.ARGS["update_remote_data"]
        ):
            return self.repo.get_available_repo_data()
        del self.repo  # noqa:WPS100

        self.scraper.populate_initial_queue()

        if config.ARGS["validate_urls"]:
            self.scraper.validate_urls()

        if config.ARGS["download_data_archives"]:
            self.scraper.download_zips()
            del self.scraper  # noqa:WPS100
            if config.ARGS["from_api"] and config.ARGS["api_return_type"]:
                self.api.validate_caches()
                return self.api.merge_caches()

        if config.ARGS["extract_csvs"]:
            self.csvs.extract_csvs()
        del self.csvs  # noqa:WPS100

        if config.ARGS["import_to_influxdb"]:
            self.influx.import_data()

        return None

    def _uses_sidecar(self) -> bool:
        """Return whether this foreground run should submit to the sidecar."""
        return bool(config.ARGS.get("use_sidecar"))

    def _run_sidecar_job(self) -> dict:
        """Submit this run to the Temporal sidecar client boundary."""
        request = RunRequest.from_options(self.options)
        try:
            result = submit_run_request_and_observe_sync(
                request,
                start_if_needed=bool(config.ARGS["sidecar_start"]),
                wait_for_result=bool(config.ARGS["sidecar_wait_result"]),
            )
        except SidecarUnavailableError as err:
            if config.ARGS["from_api"]:
                raise
            print(f"error: {err}", file=sys.stderr)  # noqa:T201
            raise SystemExit(1) from err

        payload = result.to_dict()
        if not config.ARGS["from_api"]:
            print(json.dumps(payload, indent=2, sort_keys=True))  # noqa:T201
        return payload


def main(
    options: Options | None = None,
) -> list | dict | PolarsDataFrame | DataFrame | Table | int | None:
    """Execute. Entry-point for histdatacom.

    Args:
        options (Options): a histdatacom.options Options object.

    Returns:
        list | dict | PolarsDataFrame | DataFrame | Table | None:

            Data (PolarsDataFrame | DataFrame | Table):
                    a Polars DataFrame, pandas DataFrame, or pyarrow Table.
            List of dicts:  [
                                {
                                    "timeframe": timeframe,
                                    "pair": pair,
                                    "records": [record, record, ...],
                                    "data": PolarsDataFrame | DataFrame | Table,
                                },
                                ...
                                ...
                            ]
    """
    if not options and len(sys.argv) > 1 and sys.argv[1] == "sidecar":
        from histdatacom.sidecar.cli import main as sidecar_main

        return sidecar_main(sys.argv[2:])

    if not options:
        options = Options()
        if _argv_requests_sidecar_job(sys.argv):
            _HistDataCom(options).run()
            return None
        QueueManager(options)(_HistDataCom)
        return None
    options.from_api = True
    if options.use_sidecar:
        return _HistDataCom(options).run()
    return QueueManager(options)(_HistDataCom)


def _argv_requests_sidecar_job(argv: list[str]) -> bool:
    """Return whether the foreground CLI command should bypass queues."""
    return "--sidecar" in argv


if __name__ == "__main__":
    raise SystemExit(main())
