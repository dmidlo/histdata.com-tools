import codecs
import os.path
from setuptools import setup
from setuptools import find_packages

def read(rel_path):
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, rel_path), 'r') as fp:
        return fp.read()

def get_version(rel_path):
    for line in read(rel_path).splitlines():
        if line.startswith('__version__'):
            delim = '"' if '"' in line else "'"
            return line.split(delim)[1]
    else:
        raise RuntimeError("Unable to find version string.")

with open("README.md", "r", encoding="utf-8") as file:
    long_description = file.read()

setup(
    # basic package data
    name="histdatacom",
    version=get_version("src/histdatacom/__init__.py"),
    description="A Multi-threaded/Multi-Process command-line utility and python package that downloads currency exchange rates from Histdata.com. Imports to InfluxDB. Can be used in Jupyter Notebooks.",
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
        'pytz'
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Environment :: MacOS X',
        'Framework :: Jupyter',
        'Programming Language :: Python :: 3.10',
    ]
)

# os.environ.pop('SSL_CERT_FILE', None)
# os.environ.pop('REQUESTS_CA_BUNDLE', None)
