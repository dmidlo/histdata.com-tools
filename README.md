# histdata.com-tools

A Multi-threaded/Multi-Process command-line utility and python package that downloads currency exchange rates from Histdata.com. Imports to InfluxDB. Can be used in Jupyter Notebooks.

- [histdata.com-tools](#histdatacom-tools)
- [Disclaimer](#disclaimer)
   - [Usage](#usage)
      - [Help](#help)
      - [Basic Use](#basic-use)
      - [Available Formats](#available-formats)
      - [Date Ranges](#date-ranges)
         - ['Start' & 'Now' Keywords](#start--now-keywords)
      - [Multiple Datasets](#multiple-datasets)
      - [Import to InfluxDB](#import-to-influxdb)
         - [influxdb.yaml](#influxdbyaml)
      - [Other Scripts, Modules, & Jupyter Support](#other-scripts-modules--jupyter-support)
- [Roadmap](#roadmap)
- [Setup](#setup)

## Disclaimer

*I am in no way affiliated with histdata.com or its maintainers. Please use this application in a way that respects the hard work and resources of histdata.com*

*If you choose to use this tool, it is **strongly** suggested that you head over to http://www.histdata.com/download-by-ftp/ and sign up to help support their traffic costs.*

*If you find this tool helpful and would like to support future development, I'm in need of caffeine, feel free to [buy me coffee!](https://www.buymeacoffee.com/dmidlo)*

## Usage
**Note #1**
The number one rule when using this tool is to be **MORE** specific with your input to limit the size of your request.

**Note #2**
*histdatacom is a very powerful tool and has the capability to fetch the entire repository housed on histdata.com. This is **NEVER** necessary. If you are using this tool to fetch data for your favorite trading application, do not download data in all available formats.*

*It is likely the default behavior will be modified from its current state to discourage unnecessarily large requests.*

**please submit feature requests and bug reports using this repository's issue tracker.*

##### Help
```sh
# Show the help and options
#
$ histdatacom -h

usage: histdatacom [-h] [-V] [-D] [-X] [-I] [-p PAIR [PAIR ...]] [-f FORMAT [FORMAT ...]] [-t TIMEFRAME [TIMEFRAME ...]] [-s START_YEARMONTH] [-e END_YEARMONTH]
                   [-d DATA_DIRECTORY]

options:
  -h, --help            show this help message and exit
  -V, --validate_urls   Check generated list of URLs as valid download locations
  -D, --download_data_archives
                        download specified pairs/formats/timeframe and create data files
  -X, --extract_csvs    histdata.com delivers zip files. use the -X flag to extract them to .csv.
  -I, --import_to_influxdb
                        import csv data to influxdb instance. Use influxdb.yaml to configure.
  -p PAIR [PAIR ...], --pairs PAIR [PAIR ...]
                        space separated currency pairs. e.g. -p eurusd usdjpy ...
  -f FORMAT [FORMAT ...], --formats FORMAT [FORMAT ...]
                        space separated formats. e.g. -f metatrader ascii ninjatrader metastock
  -t TIMEFRAME [TIMEFRAME ...], --timeframes TIMEFRAME [TIMEFRAME ...]
                        space separated Timeframes. e.g. -t tick-data-quotes 1-minute-bar-quotes ...
  -s START_YEARMONTH, --start_yearmonth START_YEARMONTH
                        set a start year and month for data. e.g. -s 2000-04 or -s 2015-00
  -e END_YEARMONTH, --end_yearmonth END_YEARMONTH
                        set a start year and month for data. e.g. -s 2020-00 or -s 2022-04
  -d DATA_DIRECTORY, --data-directory DATA_DIRECTORY
                        Not an Executable Search Path! This directory is used to perform work. default is "data" in the current directory
```

##### Basic Use

```sh
# Download and extract the current month's 
# available EURUSD data for metatrader 4/5
# into the default data directory ./data
#
$ histdatacom -p eurusd -f metatrader -s now
```

```sh
# include the -D flag to download 
# but NOT extract to csv.
# 
$ histdatacom -D -p usdcad -f metastock -s now
```

##### Available Formats

```sh
# The formats available are:
# metatrader, metastock, ninjatrader, excel, and ascii.
#
# histdata.com provides different resolutions of time
# depending on the format.
#
# The following format/timeframe combinations are available:
#
#    1-minute-bar-quotes -- all formats 
#    tick-data-quotes ----- ascii
#    tick-last-quotes ----- ninjatrader
#    tick-bid-quotes ------ ninjatrader
#    tick-bid-quotes ------ ninjatrader
```

```sh
# To download 1-minute-bar-quotes for both metastock and excel
#
$ histdatacom -p usdjpy -f metastock excel -s now 
```

##### Date Ranges

```sh
# date ranges are for year and month and can be specified
# in the following ways [ -._]:
#    2022-04
#   "2202 04"
#    2202.04
#    2202_04
```

```sh
# to fetch a single year's data, do not use a month
#    - note: unless you're fetching data for the current year,
#            tick data types will fetch 12 files for each month
#            of the year, 1-minute-bar-quotes will fetch a single
#            OHLC file with the whole year's data.
#            
$ histdatacom -p udxusd -f ascii -t tick-data-quotes -s 2011
```

```sh
# to fetch a single month's data, include a month, but do not
# use the -e, --end_yearmonth flag.
#
#   * if you're requesting 1-minute-bar-quotes for any
#     year except the current year, you will receive the
#     the whole year's data
#
#   * this example leaves out the -p --pair flag, and will
#     fetch data for all 66 available instruments
#
$ histdatacom -f metatrader -s 2012-07
```

###### 'Start' & 'Now' Keywords

```sh
# you may hav noticed that two special year-month keywords exist
#  'start' and 'now'
#
#  -'start' may only be used with the -s --start_yearmonth
#    flag and the -e --end_yearmonth flag must be specified
#    to indicate a range of data
#
$ histdatacom -p audusd -f metatrader -s start -e 2008-12
#
#  -'now' when used alone will return the current year-month
#        - when used with as '-s now' it will return the
#          most current month's data
#
$ histdatacom -p frxeur -f ninjatrader -s now
#
#   in the above example, no -t --timeframe flag was
#   specified. This will return all time resolutions
#   available for the specified format(s)
#
#   'now' when used with the -e --end_yearmonth flag
#    is intended to be the end of a range. Rather,
#    if the flags were to be -s 2019-04 -e now
#    the request would return data from April 2019 
#    to the present.
#
$ histdatacom -p xagusd -f ascii -1-minute-bar-quotes -s 2019-04 -e now
```

##### Multiple Datasets


```sh
# multiple datasets can be requested in one command
# 
# this example with use the -e --end_yearmonth flag
# to request a range of data for multiple instruments
#
#  - note: Large requests like these are to be avoided.
#          remember to sign up with histdata.com to help
#          them pay for network costs
#
$ histdatacom -p eurusd usdcad udxusd -f metatrader -s start -e 2017-04
```

##### Import to InfluxDB


```sh
# To import data to an influxdb instance, use the -I flag
# along with an influxdb.yaml file in the current working
# directory (where ever you are running the command from).
#
#  - ascii is the only format accepted for influxdb import.
#
#  - all histdata.com datetime data is in EST (Eastern Standard Time)
#    with no adjustments for daylight savings.
#
#  - Influxdb does not adjust for timezone and all datetime data
#    is recorded as UTC epoch timestamps (nano-seconds since 
#    midnight 00:00, January, 1st, 1970)
#
#  - this tool converts histdata.com ESTnoDST to UTC Epoch 
#    milli-second timestamps as part of the import-to-influx process
#
$ histdatacom -I -p eurusd -f ascii -t tick-data-quotes -s start -e now
```

###### influxdb.yaml

```yaml
# a sample influxdb.yaml file.
influxdb:
  org: influx_org
  bucket: data_bucket
  url: influx_server_api_url
  token: influx_user_token
```

##### Other Scripts, Modules, & Jupyter Support


```python
# Basic support for Jupyter notebooks and calling from another script/module
#  - there is no return value from calling histdatacom,
#    it functions only as far as the cli version does, that is,
#    that it will validate, download, extract, and/or import to influxdb.
#    After that, It would be up the developer to work with the files on disk
#    or to query influxdb.
#
#  - for progress bars in jupyter you will need to install the ipywidgets package
#     sh 
#        $ pip install ipywidgets
#
#  First import the required modules
import histdatacom
from histdatacom.cli import ArgsNamespace

# Create a new options object to pass parameters to histdatacom
options = ArgsNamespace

# Configure
options.extract_csvs = True
options.formats = {"ascii"}
options.timeframes = {"tick-data-quotes"}
options.pairs = {"audusd","udxusd","eurusd"}
options.start_yearmonth = "2022-03"
options.end_yearmonth = "2022-04"

# pass the options to histdatacom
histdatacom(options)  # (Jupyter)

#  at present, calling from another script or module is limited
#  to using the __name__=="__main__" idiom.
if __name__=="__main__": 
   histdatacom(options)
```


## Roadmap

- return datatable/pandas/dask dataframe when called from jupyter or another module

## Setup

1. Create a virtual environment
   - `python -m venv venv`

2. Activate the virtual environment
   - `source venv/bin/activate`
   - validate with `which python`
     - result should be `.../histdata_com_tools/venv/bin/python`

3. Install CA certificate (macOS)
   - `pip install certifi`

4. Apply Backport if not already patched upstream
   - This seems to already be upstream, but you can verify if concerned
   - `lib/python3.XXX/multiprocessing/managers.py > AutoProxy`
   - `https://github.com/python/cpython/commit/8aa45de6c6d84397b772bad7e032744010bbd456`
  
5. Build and install the app
   - `python setup.py build`
   - `python setup.py install`

6. Run `histdatacom` to view help message and flags
