from setuptools import setup, find_packages
import os, sys

try:
    import certifi
except:
    print("Unexpected error:", sys.exc_info()[0])
    print("\n certifi not installed. Please run 'pip install certifi'")
    sys.exit()

os.environ["SSL_CERT_FILE"] = certifi.where()
os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

setup(
    #basic package data
    name = "histdatacom",
    version= "0.1",

    #package structure
    packages=find_packages('src'),
    package_dir={'':'src'},

    #install the RSReader executable
    entry_points = {
        'console_scripts' : [
            'histdatacom = histdatacom.histdata_com:main'
        ]
    },
    install_requires = [
        'influxdb_client',
        'rich',
        'requests',
        'bs4',
        'pyyaml',
        'rx',
        'argparse',
        'pytz',
        'datatable'
    ],
)

os.environ.pop('SSL_CERT_FILE', None)
os.environ.pop('REQUESTS_CA_BUNDLE', None)