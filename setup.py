import os
import sys
from setuptools import setup
from setuptools import find_packages

# try:
#     import certifi
# except ImportError:
#     print("Unexpected error:", sys.exc_info()[0])
#     print("\n certifi not installed. Please run 'pip install certifi'")
#     sys.exit()

# os.environ["SSL_CERT_FILE"] = certifi.where()
# os.environ["REQUESTS_CA_BUNDLE"] = certifi.where()

setup(
    # basic package data
    name="histdatacom",
    version="0.75",
    description="A Multi-threaded/Multi-Process command-line utility and python package that downloads currency exchange rates from Histdata.com. Imports to InfluxDB. Can be used in Jupyter Notebooks.",
    url='https://github.com/dmidlo/histdata.com-tools',
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
