from pyarrow import Table
from datatable import Frame
from pandas import DataFrame

from histdatacom import config
from histdatacom import Options
from histdatacom import QueueManager
from histdatacom import ArgParser
from histdatacom import Csv
from histdatacom import Api
from histdatacom import Influx

from histdatacom.scraper.repo import Repo
from histdatacom.scraper.scraper import Scraper

from histdatacom.utils import Utils


class _HistDataCom:
    """A module to pull market data from histdata.com and import it into influxDB"""

    def __init__(self, options: Options) -> None:

        """Initialization for _HistDataCom Class"""
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
        config.ARGS = ArgParser._arg_list_to_set(vars(ArgParser(options)())).copy()
        config.ARGS["default_download_dir"] = Utils.set_working_data_dir(
            config.ARGS["data_directory"]
        )

        Repo.set_repo_url()
        Scraper.set_base_url()
        Scraper.set_post_headers()

        if config.ARGS["import_to_influxdb"] == 1:
            influx_yaml = Utils.load_influx_yaml()
            config.ARGS["INFLUX_ORG"] = influx_yaml["influxdb"]["org"]
            config.ARGS["INFLUX_BUCKET"] = influx_yaml["influxdb"]["bucket"]
            config.ARGS["INFLUX_URL"] = influx_yaml["influxdb"]["url"]
            config.ARGS["INFLUX_TOKEN"] = influx_yaml["influxdb"]["token"]

        self.csvs = Csv()
        self.api = Api()

        if config.ARGS["available_remote_data"] or config.ARGS["update_remote_data"]:
            if Repo.test_for_repo_data_file():
                Repo.read_repo_data_file()
            Repo.update_repo_from_github()

        if config.ARGS["import_to_influxdb"] == 1:
            self.influx = Influx()

    def run(self) -> list | dict | Frame | DataFrame | Table | None:
        if config.ARGS["available_remote_data"] or config.ARGS["update_remote_data"]:
            return Repo.get_available_repo_data()

        Scraper.populate_initial_queue()

        if config.ARGS["validate_urls"]:
            Scraper.validate_urls()

        if config.ARGS["download_data_archives"]:
            Scraper.download_zips()
            if config.ARGS["from_api"]:
                self.api.validate_jays()
                return self.api.merge_jays()

        if config.ARGS["extract_csvs"]:
            self.csvs.extract_csvs()

        if config.ARGS["import_to_influxdb"]:
            self.influx.import_data()

        return None


def main(options: Options | None = None) -> list | dict | Frame | DataFrame | Table:
    if not options:
        options = Options()
        QueueManager(options)(_HistDataCom)
        return None
    else:
        options.from_api = True
        return QueueManager(options)(_HistDataCom)


if __name__ == "__main__":
    raise SystemExit(main())
