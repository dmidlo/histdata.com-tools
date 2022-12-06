# type: ignore
# pylint: disable=not-callable
"""Generate Architecture Diagrams."""

import random
from pathlib import Path
from shutil import rmtree
from contextlib import suppress

import sh
from pycallgraph2 import PyCallGraph  # noqa:I900
from pycallgraph2.output import GraphvizOutput  # noqa:I900

import histdatacom
from histdatacom import Options, Pairs

project_dir = Path(".")
package_dir = project_dir / "src" / "histdatacom"
test_runner_path = project_dir / "test.py"
base_dir = project_dir / Path("tests", "architecture")
pstats_output_path = base_dir / "output.pstats"
pycallgraph_dot_path = base_dir / "pycallgraph.dot"
pycallgraph_svg_path = base_dir / "pycallgraph.svg"


class Pycallgraphhistdatacom:  # noqa:H601
    """Test class for histdatacom.

    $ pip install pycallgraph2
    """

    def __init__(self) -> None:
        """Initialize class."""
        self.number_of_pairs = random.randint(1, 4)  # noqa:S311

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
        self.method_result = None

    @staticmethod
    def check_py_api():  # noqa:D102,DC102
        tester = Pycallgraphhistdatacom()
        print(tester.check_py_api_available_remote_data())  # noqa:T201
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        print(  # noqa:T201,BLK100
            tester.check_py_api_update_and_validate_remote_data()
        )
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        tester.check_py_api_download_data()
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        tester.check_py_api_extract_data()
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        tester.check_py_api_import_to_influx()
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        print(tester.check_py_api_api_return())  # noqa:T201
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        tester.delete_data_directory()
        del tester  # noqa:WPS100

    @staticmethod
    def pycallgraph():  # noqa:D102,DC102
        graphviz = GraphvizOutput()
        graphviz.output_type = "dot"
        graphviz.output_file = pycallgraph_dot_path.resolve()

        with PyCallGraph(output=graphviz):
            Pycallgraphhistdatacom.check_py_api()

    @staticmethod
    def pycallgraph_dot_to_svg():  # noqa:D102,DC102

        with suppress(Exception):
            sh.dot(
                "-Tsvg",
                pycallgraph_dot_path.resolve(),
                "-o",
                pycallgraph_svg_path.resolve(),
            )
        pycallgraph_dot_path.unlink()

    @staticmethod
    def main():  # noqa:D102,DC102
        Pycallgraphhistdatacom.pycallgraph()
        Pycallgraphhistdatacom.pycallgraph_dot_to_svg()

    def check_py_api_available_remote_data(self):  # noqa:D102,DC102
        print("Checking histdatacom -A from api.")  # noqa:T201
        self.options.available_remote_data = True
        self.options.pairs = Pairs.list_keys()
        self.method_result = histdatacom(self.options)  # pylint: disable=E1102
        return self.method_result

    def check_py_api_update_and_validate_remote_data(self):  # noqa:D102,DC102
        print("Checking histdatacom -U from api.")  # noqa:T201
        self.options.update_remote_data = True
        self.method_result = histdatacom(self.options)
        return self.method_result

    def check_py_api_download_data(self):  # noqa:D102,DC102
        print("Checking histdatacom -D from api.")  # noqa:T201
        self.options.download_data_archives = True
        self.options.start_yearmonth = "2011-06"
        self.options.end_yearmonth = "2011-12"
        self.method_result = histdatacom(self.options)
        return self.method_result

    def check_py_api_extract_data(self):  # noqa:D102,DC102
        print("Checking histdatacom -X from api.")  # noqa:T201
        self.options.extract_csvs = True
        self.options.start_yearmonth = "2011-06"
        self.options.end_yearmonth = "2011-07"
        self.method_result = histdatacom(self.options)
        return self.method_result

    def check_py_api_import_to_influx(self):  # noqa:D102,DC102
        print("Checking histdatacom -I from api.")  # noqa:T201
        self.options.import_to_influxdb = True
        self.options.start_yearmonth = "2011-05"
        self.options.end_yearmonth = "2011-06"
        self.method_result = histdatacom(self.options)
        return self.method_result

    def check_py_api_api_return(self):  # noqa:D102,DC102
        print("Checking histdatacom api from api.")  # noqa:T201
        self.options.api_return_type = "datatable"
        self.options.start_yearmonth = "2011-05"
        self.options.end_yearmonth = "2012-01"
        self.method_result = histdatacom(self.options)
        return self.method_result

    def check_for_data_directory(self):  # noqa:D102,DC102
        data_path = project_dir / self.options.data_directory

        if data_path.exists():
            return data_path
        raise FileExistsError(f"{data_path} does not exist.")

    def delete_data_directory(self):  # noqa:D102,DC102
        path = self.check_for_data_directory()
        rmtree(path)


def generate_code2flow() -> None:
    """Generate code2flow.

    $ pip install code2flow
    $ code2flow src/
    """
    code2flow_svg_path = base_dir / "code2flow.svg"
    code2flow_gv_path = base_dir / "code2flow.gv"
    sh.code2flow("src/", "-o", code2flow_svg_path.resolve())
    code2flow_gv_path.unlink()


def generate_gprof2dot_png() -> None:
    """Generate gprof2dot.png.

    ## Call Graph Visualization using gprof2dot
    $ sudo port install graphviz
    $ pip install gprof2dot
    $ python -m cProfile -o output.pstats src/histdatacom/histdata_com.py\
         -X -p eurusd -f ascii -t tick-data-quotes -s 2021-01 -e now
    $ gprof2dot -f pstats output.pstats | dot -Tpng -o gprof2dot.png
    """
    gprof2dot_svg_path = base_dir / "gprof2dot.svg"

    sh.python(
        "-m",  # noqa:BLK100
        "cProfile",
        "-o",
        pstats_output_path.resolve(),
        test_runner_path.resolve(),
        _fg=True,
    )
    sh.dot(
        sh.gprof2dot("-f", "pstats", pstats_output_path.resolve()),
        "-Tsvg",
        "-o",
        gprof2dot_svg_path.resolve(),
    )
    pstats_output_path.unlink()


def generate_pyreverse_svgs() -> None:
    """Generate pyreverse svgs."""
    sh.pyreverse(
        "-f",
        "ALL",
        "-o",
        "svg",
        "--project",
        "pyreverse",
        "--output-directory",
        base_dir.resolve(),
        package_dir.resolve(),
    )


def main() -> None:
    """Generate Architecture Diagrams."""
    generate_gprof2dot_png()
    generate_code2flow()
    Pycallgraphhistdatacom.main()
    generate_pyreverse_svgs()


if __name__ == "__main__":
    main()
