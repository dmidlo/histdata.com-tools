# type: ignore
# pylint: disable=not-callable
"""Generate Architecture Diagrams."""

import tempfile
from pathlib import Path
from contextlib import suppress
from types import SimpleNamespace

import sh
from pycallgraph2 import PyCallGraph  # noqa:I900
from pycallgraph2.output import GraphvizOutput  # noqa:I900

import histdatacom
from histdatacom import Options
from histdatacom.api import Api
from histdatacom.histdata_ascii import CACHE_FILENAME

project_dir = Path(".")
package_dir = project_dir / "src" / "histdatacom"
histdata_ascii_fixtures = project_dir / "tests" / "fixtures" / "histdata_ascii"
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
        self.options = Options()
        self.options.by: str = "start_asc"  # pylint: disable=invalid-name
        self.options.pairs: set = {"eurusd"}
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
        print(tester.check_py_api_version())  # noqa:T201
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        tester.check_py_api_polars_ingest()
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        tester.check_py_api_cache_round_trip()
        del tester  # noqa:WPS100

        tester = Pycallgraphhistdatacom()
        print(tester.check_py_api_api_return_contract())  # noqa:T201
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

    def check_py_api_version(self):  # noqa:D102,DC102
        print("Checking histdatacom version from api.")  # noqa:T201
        self.options.version = True
        self.method_result = histdatacom(self.options)
        return self.method_result

    def check_py_api_polars_ingest(self):  # noqa:D102,DC102
        print("Checking local Polars ingest path.")  # noqa:T201
        self.method_result = Api._import_file_to_polars(
            SimpleNamespace(data_timeframe="T"),
            histdata_ascii_fixtures / "DAT_ASCII_EURUSD_T_201202.csv",
        )
        return self.method_result

    def check_py_api_cache_round_trip(self):  # noqa:D102,DC102
        print("Checking local Polars cache round trip.")  # noqa:T201
        frame = self.check_py_api_polars_ingest()
        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir, CACHE_FILENAME)
            Api._write_cache_data(frame, str(cache_path))
            self.method_result = Api.import_cache_data(str(cache_path))
        return self.method_result

    def check_py_api_api_return_contract(self):  # noqa:D102,DC102
        print("Checking histdatacom api return contract.")  # noqa:T201
        self.options.api_return_type = "polars"
        self.method_result = self.options.api_return_type
        return self.method_result


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
    $ python -m cProfile -o output.pstats -m pytest tests/unit/test_api.py
    $ gprof2dot -f pstats output.pstats | dot -Tpng -o gprof2dot.png
    """
    gprof2dot_svg_path = base_dir / "gprof2dot.svg"

    sh.python(
        "-m",  # noqa:BLK100
        "cProfile",
        "-o",
        pstats_output_path.resolve(),
        "-m",
        "pytest",
        "tests/unit/test_api.py",
        "tests/unit/test_histdata_ascii.py",
        "tests/unit/test_influx.py",
        _fg=True,
    )
    graph_source = str(
        sh.gprof2dot(
            "-f",
            "pstats",
            "--node-thres",
            "2.0",
            "--edge-thres",
            "2.0",
            pstats_output_path.resolve(),
        )
    )
    sh.dot(
        "-Tsvg",
        "-o",
        gprof2dot_svg_path.resolve(),
        _in=graph_source,
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
