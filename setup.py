"""Setup the setuptools version of histdatacom.

Raises:
    RuntimeError: When unable to find __version__ string

Returns:
    None
"""
import codecs
from pathlib import Path, PurePath

from setuptools import find_packages, setup


def read_init_file(rel_path: str) -> str:
    """Read the contents of a file.

    Args:
        rel_path (str): a relative file path

    Returns:
        str: contents of file
    """
    init_file_path = PurePath(__file__).parent / rel_path
    with codecs.open(str(init_file_path), "r") as file_path:
        return file_path.read()


def get_version(rel_path: str) -> str:
    """Get the version number from file by looking at  "__version__ = 'x.x.x'".

    Args:
        rel_path (str): a relative file path

    Raises:
        ErrCantFindVersionString: Unable to find version string

    Returns:
        str: string in file after line __version__ delimited by " or '
    """
    for line in read_init_file(rel_path).splitlines():
        if line.startswith("__version__"):
            deliminator = '"' if '"' in line else "'"
            return line.split(deliminator)[1]
    raise ErrCantFindVersionString(rel_path)


readme = Path("README.md")
with readme.open("r", encoding="utf-8") as readme_content:
    long_description = readme_content.read()

setup(
    # basic package data
    name="histdatacom",
    python_requires=">=3.10.0",
    version=get_version(str(PurePath("src", "histdatacom", "__init__.py"))),
    description=(
        "A Multi-threaded/Multi-Process command-line utility and "
        "python package that downloads currency exchange rates from "
        "Histdata.com. Imports to InfluxDB. Can be used in Jupyter "
        "Notebooks."
    ),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/dmidlo/histdata.com-tools",
    project_urls={
        "Bug Tracker": "https://github.com/dmidlo/histdata.com-tools/issues",
    },
    author="David Midlo",
    author_email="dmidlo@gmail.com",
    license="MIT License",
    # package structure
    packages=find_packages("src"),
    package_dir={"": "src"},
    # install the RSReader executable
    entry_points={
        "console_scripts": ["histdatacom = histdatacom.histdata_com:main"],
    },  # pylint: disable=line-too-long
    install_requires=[
        "influxdb_client",
        "rich",
        "requests",
        "bs4",
        "pyyaml",
        "rx",
        "argparse",
        "pytz",
        "ipywidgets",
        "pyarrow",
        "pandas",
        "certifi",
    ],
    extras_require={
        "dev": [
            "pytest",
            "mypy",
            "keyring",
            "sh",
            "flake8",
            "black",
            "flake8-black",
            "pylint",
            "bandit",
            "flake8-bandit",
            "flake8-comprehensions",
            "flake8-class-attributes-order",
            "flake8-bugbear",
            "pyflakes",
            "pre-commit",
            "types-setuptools",
            "pandas-stubs",
            "types-beautifulsoup4",
            "wemake-python-styleguide",
            "flake8-simplify",
            "flake8-pie",
            "flake8-use-pathlib",
            "flake8-use-fstring",
            "flake8-print",
            "flake8-no-implicit-concat",
            "pycodestyle",
            "flake8-pytest-style",
            "Flake8-AAA",
            "flake8-spellcheck",
            "flake8-docstring-checker",
            "flake8-docstrings",
            "flake8-coding",
            "flake8-length",
            "flake8-functions",
            "flake8-expression-complexity",
            "flake8-cognitive-complexity",
            "flake8-annotations-complexity",
            "cohesion",
            "Darglint",
            "tryceratops",
            "radon",
            "pyroma",
            "vulture",
            "flake8-type-checking",
            "isort",
            "sourcery-cli",
        ],
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Environment :: MacOS X",
        "Operating System :: MacOS",
        "Environment :: Win32 (MS Windows)",
        "Operating System :: Microsoft :: Windows",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: Jupyter",
        "Topic :: Terminals",
    ],
    keywords=[
        "finance",
        "data",
        "datascience",
        "HistData.com",
        "scraper",
        "influxdb",
        "currency exchange",
        "forex",
        "fx",
        "etl",
    ],
)


class ErrCantFindVersionString(RuntimeError):  # noqa: H601
    """RuntimeError when version string is not found.

    Args:
        path (str): tested path of __version__ string
    """

    def __init__(self, path: str) -> None:
        """Call when the version is not found.

        Args:
            path (str): tested path of __version__ string
        """
        super().__init__(f"Unable to find __version__ string in:   \n {path}")
