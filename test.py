from pathlib import Path
from shutil import rmtree
import sh
import random

from rich import print

import histdatacom
from histdatacom import Options
from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe


class Testhistdatacom:
    def __init__(self):
        self.number_of_pairs = random.randint(1, 4)

        self.options = Options()
        self.options.by: str = "start_asc"  # pylint: disable=invalid-name
        self.options.pairs: set = set(random.sample(list(Pairs.list_keys()), 1))
        self.options.formats: set = {"ascii"}
        self.options.timeframes: set = {"tick-data-quotes"}
        self.options.data_directory: str = "data"
        self.options.cpu_utilization: str = "high"
        self.options.batch_size: str = "5000"
        self.options.delete_after_influx: bool = False
        self.options.zip_persist: bool = False
        self.result = None

    def test_py_api_available_remote_data(self):
        print("Testing histdatacom -A from api.")
        self.options.available_remote_data = True
        self.options.pairs = Pairs.list_keys()
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_update_and_validate_remote_data(self):
        print("Testing histdatacom -U from api.")
        self.options.update_remote_data = True
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_download_data(self):
        print("Testing histdatacom -D from api.")
        self.options.download_data_archives = True
        self.options.start_yearmonth = "2011-06"
        self.options.end_yearmonth = "2011-12"
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_extract_data(self):
        print("Testing histdatacom -X from api.")
        self.options.extract_csvs = True
        self.options.start_yearmonth = "2011-06"
        self.options.end_yearmonth = "2011-07"
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_import_to_influx(self):
        print("Testing histdatacom -I from api.")
        self.options.import_to_influxdb = True
        self.options.start_yearmonth = "2021-01"
        self.options.end_yearmonth = "2021-12"
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_api_return(self):
        print("Testing histdatacom api from api.")
        self.options.api_return_type = "datatable"
        self.options.start_yearmonth = "2011-05"
        self.options.end_yearmonth = "2012-01"
        self.result = histdatacom(self.options)
        return self.result

    def check_for_data_directory(self):
        data_path = Path(__file__).parent / self.options.data_directory

        if data_path.exists():
            return data_path
        raise FileExistsError(f"{data_path} does not exist. Something went wrong")

    def delete_data_directory(self):
        path = self.check_for_data_directory()
        rmtree(path)

    def test_cli_available_remote_data(self):
        print("Testing histdatacom -A from cli.")
        sh.histdatacom(A=True, by="start_asc", _fg=True)

    def test_cli_update_and_validate_remote_data(self):
        print("Testing histdatacom -U and -V from cli.")
        sh.histdatacom(
            U=True,
            p=" ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            f=" ".join(self.options.formats),
            t=" ".join(self.options.timeframes),
            _fg=True,
        )

    def test_cli_download_data(self):
        print("Testing histdatacom -D from cli.")
        sh.histdatacom(
            D=True,
            p=" ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            f=" ".join(self.options.formats),
            t=" ".join(self.options.timeframes),
            s="2011-06",
            e="2011-12",
            c=self.options.cpu_utilization,
            _fg=True,
        )

    def test_cli_extract_data(self):
        print("Testing histdatacom -X from cli.")
        sh.histdatacom(
            X=True,
            p=" ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            f=" ".join(self.options.formats),
            t=" ".join(self.options.timeframes),
            s="2011-06",
            e="2011-07",
            c=self.options.cpu_utilization,
            _fg=True,
        )

    def test_cli_import_to_influx(self):
        print("Testing histdatacom -I from cli.")
        sh.histdatacom(
            I=True,
            p=" ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            f=" ".join(self.options.formats),
            t=" ".join(self.options.timeframes),
            s="2011-05",
            e="2011-06",
            c=self.options.cpu_utilization,
            _fg=True,
        )

    def test_module_available_remote_data(self):
        print("Testing histdatacom -A as module.")
        sh.python("-m", "histdatacom", "-A", "--by", "start_asc", _fg=True)

    def test_module_update_and_validate_remote_data(self):
        print("Testing histdatacom -U and -V as module")
        sh.python(
            "-m",
            "histdatacom",
            "-U",
            "-p",
            " ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            "-f",
            " ".join(self.options.formats),
            "-t",
            " ".join(self.options.timeframes),
            _fg=True,
        )

    def test_module_download_data(self):
        print("Testing histdatacom -D as module.")
        sh.python(
            "-m",
            "histdatacom",
            "-D",
            "-p",
            " ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            "-f",
            " ".join(self.options.formats),
            "-t",
            " ".join(self.options.timeframes),
            "-s",
            "2011-06",
            "-e",
            "2011-12",
            "-c",
            self.options.cpu_utilization,
            _fg=True,
        )

    def test_module_extract_data(self):
        print("Testing histdatacom -X as module.")
        sh.python(
            "-m",
            "histdatacom",
            "-X",
            "-p",
            " ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            "-f",
            " ".join(self.options.formats),
            "-t",
            " ".join(self.options.timeframes),
            "-s",
            "2011-06",
            "-e",
            "2011-07",
            "-c",
            self.options.cpu_utilization,
            _fg=True,

        )

    def test_module_import_to_influx(self):
        print("Testing histdatacom -I as module.")
        sh.python(
            "-m",
            "histdatacom",
            "-I",
            "-p",
            " ".join(set(random.sample(list(Pairs.list_keys()), 1))),
            "-f",
            " ".join(self.options.formats),
            "-t",
            " ".join(self.options.timeframes),
            "-s",
            "2011-05",
            "-e",
            "2011-06",
            "-c",
            self.options.cpu_utilization,
            _fg=True,
        )

    @staticmethod
    def test_module():
        tester = Testhistdatacom()
        tester.test_module_available_remote_data()
        tester.test_module_update_and_validate_remote_data()
        tester.test_module_download_data()
        tester.test_module_extract_data()
        tester.test_module_import_to_influx()
        tester.delete_data_directory()

    @staticmethod
    def test_cli():
        tester = Testhistdatacom()
        tester.test_cli_available_remote_data()
        tester.test_cli_update_and_validate_remote_data()
        tester.test_cli_download_data()
        tester.test_cli_extract_data()
        tester.test_cli_import_to_influx()
        tester.delete_data_directory()
        del tester

    @staticmethod
    def test_py_api():
        tester = Testhistdatacom()
        print(tester.test_py_api_available_remote_data())
        del tester

        tester = Testhistdatacom()
        print(tester.test_py_api_update_and_validate_remote_data())
        del tester

        tester = Testhistdatacom()
        tester.test_py_api_download_data()
        del tester

        tester = Testhistdatacom()
        tester.test_py_api_extract_data()
        del tester

        tester = Testhistdatacom()
        tester.test_py_api_import_to_influx()
        del tester

        tester = Testhistdatacom()
        print(tester.test_py_api_api_return())
        del tester

        tester = Testhistdatacom()
        tester.delete_data_directory()
        del tester

    @staticmethod
    def main():
        Testhistdatacom.test_py_api()
        Testhistdatacom.test_cli()
        Testhistdatacom.test_module()

if __name__ == "__main__":
    Testhistdatacom.main()
