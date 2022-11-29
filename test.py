from pathlib import Path
from shutil import rmtree
import sh


import histdatacom
from histdatacom import Options
from histdatacom.fx_enums import Pairs
from histdatacom.fx_enums import Format
from histdatacom.fx_enums import Timeframe

class Testhistdatacom():
    def __init__(self):
        self.options = Options()
        self.options.available_remote_data: bool = False #
        self.options.update_remote_data: bool = False #
        self.options.by: str = "pair_asc"  # pylint: disable=invalid-name
        self.options.validate_urls: bool = False #
        self.options.download_data_archives: bool = False #
        self.options.extract_csvs: bool = False #
        self.options.import_to_influxdb: bool = False
        self.options.pairs: set = Pairs.list_keys() #
        self.options.formats: set = Format.list_values() #
        self.options.timeframes: set = Timeframe.list_keys() #
        self.options.start_yearmonth: str | None = "" #
        self.options.end_yearmonth: str | None = "" #
        self.options.data_directory: str = "data" 
        self.options.from_api: bool = False #
        self.options.api_return_type: str = "datatable"
        self.options.cpu_utilization: str = "medium"
        self.options.batch_size: str = "5000"
        self.options.delete_after_influx: bool = False
        self.options.zip_persist: bool = False
        self.result = None


    def test_py_api_available_remote_data(self):
        self.options.available_remote_data = True
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_update_and_validate_remote_data(self):
        self.options.update_remote_data = True
        self.options.pairs = {"usdmxn"}
        self.options.formats = {"ascii"}
        self.options.timeframes = {"tick-data-quotes"}
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_download_data(self):
        self.options.download_data_archives = True
        self.options.pairs = {"usdmxn"}
        self.options.formats = {"ascii"}
        self.options.timeframes = {"tick-data-quotes"}
        self.options.start_yearmonth = "2011-06"
        self.options.end_yearmonth = "2011-12"
        self.options.cpu_utilization = "high"
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_extract_data(self):
        self.options.extract_csvs = True
        self.options.pairs = {"usdmxn"}
        self.options.formats = {"ascii"}
        self.options.timeframes = {"tick-data-quotes"}
        self.options.start_yearmonth = "2011-06"
        self.options.end_yearmonth = "2011-07"
        self.options.cpu_utilization = "high"
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_import_to_influx(self):
        self.options.import_to_influxdb = True
        self.options.pairs = {"usdmxn"}
        self.options.formats = {"ascii"}
        self.options.timeframes = {"tick-data-quotes"}
        self.options.start_yearmonth = "2011-05"
        self.options.end_yearmonth = "2011-06"
        self.options.cpu_utilization = "high"
        self.result = histdatacom(self.options)
        return self.result

    def test_py_api_api_return(self):
        self.options.api_return_type = "datatable"
        self.options.pairs = {"usdmxn"}
        self.options.formats = {"ascii"}
        self.options.timeframes = {"tick-data-quotes"}
        self.options.start_yearmonth = "2011-05"
        self.options.end_yearmonth = "2012-01"
        self.options.cpu_utilization = "high"
        self.result = histdatacom(self.options)
        return self.result

    def check_for_data_directory(self):
        data_path = Path(__file__).parent / self.options.data_directory

        if data_path.exists():
            print(f"{data_path} exists.")
            return data_path
        raise FileExistsError(f"{data_path} does not exist. Something went wrong")

    def delete_data_directory(self):
        path = self.check_for_data_directory()
        rmtree(path)

    def test_cli_available_remote_data(self):
        sh.histdatacom(A=True, _fg=True)    

    def test_cli_update_and_validate_remote_data(self):
        sh.histdatacom(U=True,
                       p="usdmxn",
                       f="ascii",
                       t="tick-data-quotes",
                       _fg=True)

    def test_cli_download_data(self):
        sh.histdatacom(D=True,
                       p="usdmxn",
                       f="ascii",
                       t="tick-data-quotes",
                       s="2011-06",
                       e="2011-12",
                       c="high",
                       _fg=True)

    def test_cli_extract_data(self):
        sh.histdatacom(X=True,
                       p="usdmxn",
                       f="ascii",
                       t="tick-data-quotes",
                       s="2011-06",
                       e="2011-07",
                       c="high",
                       _fg=True)

    def test_cli_import_to_influx(self):
        sh.histdatacom(I=True,
                       p="usdmxn",
                       f="ascii",
                       t="tick-data-quotes",
                       s="2011-05",
                       e="2011-06",
                       c="high",
                       _fg=True)
    
    @staticmethod
    def test_cli():
        tester = Testhistdatacom()
        tester.test_cli_available_remote_data()
        tester.test_cli_update_and_validate_remote_data()
        tester.test_cli_download_data()
        tester.test_cli_extract_data()
        tester.test_cli_import_to_influx()
        print(tester.delete_data_directory())
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
        print(tester.test_py_api_download_data())
        del tester

        tester = Testhistdatacom()
        print(tester.test_py_api_extract_data())
        del tester

        tester = Testhistdatacom()
        print(tester.test_py_api_import_to_influx())
        del tester

        tester = Testhistdatacom()
        print(tester.test_py_api_api_return())
        del tester

        tester = Testhistdatacom()
        print(tester.delete_data_directory())
        del tester

    @staticmethod
    def main():
        Testhistdatacom.test_py_api()
        Testhistdatacom.test_cli()

if __name__ == "__main__":
    Testhistdatacom.main()
