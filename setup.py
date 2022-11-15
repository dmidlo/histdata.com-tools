"""setup.py

Raises:
    RuntimeError: Unable to find version string

Returns:
    None: setuptools definition
"""
import codecs
import os.path
from setuptools import setup
from setuptools import find_packages

def read(rel_path: str) -> str:
    """reads the contents of a file

    Args:
        rel_path (str): a relative file path

    Returns:
        StreamReaderWriter: contents of file
    """
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as file_path:
        return file_path.read()

def get_version(rel_path: str) -> str:
    """gets the version number from file by looking at  "__version__ = 'x.x.x'"

    Args:
        rel_path (str): a relative file path

    Raises:
        RuntimeError: Unable to find version string

    Returns:
        str: string in file after line __version__ delimited by " or '
    """
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    raise RuntimeError("Unable to find version string.")

with open("README.md", "r", encoding="utf-8") as file:
    long_description = file.read()

setup(
    # basic package data
    name="histdatacom",
    version=get_version("src/histdatacom/__init__.py"),
    description="A Multi-threaded/Multi-Process command-line utility and \
        python package that downloads currency exchange rates from \
        Histdata.com. Imports to InfluxDB. Can be used in Jupyter Notebooks.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/dmidlo/histdata.com-tools',
    project_urls={
        "Bug Tracker": "https://github.com/dmidlo/histdata.com-tools/issues",
    },
    author="David Midlo",
    author_email="dmidlo@gmail.com",
    license="MIT License",

    # package structure
    packages=find_packages('src'),
    package_dir={'': 'src'},

    # install the RSReader executable
    entry_points={
        'console_scripts': [
            'histdatacom = histdatacom.histdata_com:main'
        ]
    },
    install_requires=[
        'influxdb_client',
        'rich',
        'requests',
        'bs4',
        'pyyaml',
        'rx',
        'argparse',
        'pytz',
        'ipywidgets',
        'pyarrow',
        'pandas'
    ],
    extras_require={
        'dev': [
            'pytest',
            'mypy',
            'types-setuptools',
            'pandas-stubs',
            'types-beautifulsoup4',
            'influxdb-pytest-plugin'
        ]
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Environment :: MacOS X',
        'Operating System :: MacOS',
        'Environment :: Win32 (MS Windows)',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3.10',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Framework :: Jupyter',
        'Topic :: Terminals',
    ]
)
