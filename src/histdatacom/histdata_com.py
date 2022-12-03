"""Run main application. Core logic.

Raises:
    SystemExit: Exit when complete.

Returns:
    QueueManager: multi-process serial communication manager
    repo_data (set): a set of repo pairs with start and end date ranges.
    Data (Frame | DataFrame | Table):
        a datatable Frame, a pandas DataFrame, or a pyarrow Table
    List Of Data:   [
                        {
                            "timeframe": timeframe,
                            "pair": pair,
                            "records": [record, record, ...],
                            "data": Frame | DataFrame | Table,
                        },
                        ...
                        ...
                    ]

"""
from __future__ import annotations

from typing import TYPE_CHECKING

from histdatacom import Options, config
from histdatacom.api import Api
from histdatacom.cli import ArgParser
from histdatacom.concurrency import QueueManager
from histdatacom.csvs import Csv
from histdatacom.influx import Influx
from histdatacom.scraper.repo import Repo
from histdatacom.scraper.scraper import Scraper
from histdatacom.utils import (
    load_influx_yaml,
    set_working_data_dir,
)

if TYPE_CHECKING:
    from datatable import Frame  # noqa:I900
    from pandas import DataFrame
    from pyarrow import Table


class _HistDataCom:
    """Pull market data from histdata.com and import it into influxDB."""

    def __init__(self, options: Options) -> None:
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
        config.ARGS = ArgParser.arg_list_to_set(  # noqa:BLK100
            vars(ArgParser(options)())  # noqa:WPS110
        ).copy()
        config.ARGS["default_download_dir"] = set_working_data_dir(
            config.ARGS["data_directory"]
        )

        if config.ARGS["import_to_influxdb"] == 1:
            influx_yaml = load_influx_yaml()
            config.ARGS["INFLUX_ORG"] = influx_yaml["influxdb"]["org"]
            config.ARGS["INFLUX_BUCKET"] = influx_yaml["influxdb"]["bucket"]
            config.ARGS["INFLUX_URL"] = influx_yaml["influxdb"]["url"]
            config.ARGS["INFLUX_TOKEN"] = influx_yaml["influxdb"]["token"]

        self.repo = Repo()
        self.scraper = Scraper()
        self.csvs = Csv()
        self.api = Api()

        if (  # noqa:BLK100
            config.ARGS["available_remote_data"]  # noqa:BLK100
            or config.ARGS["update_remote_data"]
        ):
            if self.repo.test_for_repo_data_file():
                self.repo.read_repo_data_file()
            self.repo.update_repo_from_github()

        if config.ARGS["import_to_influxdb"] == 1:
            self.influx = Influx()

    def run(  # noqa:CCR001
        self,
    ) -> list | dict | Frame | DataFrame | Table | None:  # noqa:CCR001
        """Execute. histdatacom's execution order.

        Returns:
            list | dict | Frame | DataFrame | Table | None:

            Data (Frame | DataFrame | Table):
                    a datatable Frame, a pandas DataFrame, or a pyarrow Table
            List of dicts:  [
                                {
                                    "timeframe": timeframe,
                                    "pair": pair,
                                    "records": [record, record, ...],
                                    "data": Frame | DataFrame | Table,
                                },
                                ...
                                ...
                            ]


        """
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
            if config.ARGS["from_api"]:
                self.api.validate_jays()
                return self.api.merge_jays()
        del self.api  # noqa:WPS100

        if config.ARGS["extract_csvs"]:
            self.csvs.extract_csvs()
        del self.csvs  # noqa:WPS100

        if config.ARGS["import_to_influxdb"]:
            self.influx.import_data()

        return None


def main(
    options: Options | None = None,
) -> list | dict | Frame | DataFrame | Table:
    """Execute. Entry-point for histdatacom.

    Args:
        options (Options): a histdatacom.options Options object.

    Returns:
        list | dict | Frame | DataFrame | Table:

            Data (Frame | DataFrame | Table):
                    a datatable Frame, a pandas DataFrame, or a pyarrow Table
            List of dicts:  [
                                {
                                    "timeframe": timeframe,
                                    "pair": pair,
                                    "records": [record, record, ...],
                                    "data": Frame | DataFrame | Table,
                                },
                                ...
                                ...
                            ]
    """
    if not options:
        options = Options()
        QueueManager(options)(_HistDataCom)
        return None
    options.from_api = True
    return QueueManager(options)(_HistDataCom)


if __name__ == "__main__":
    raise SystemExit(main())
