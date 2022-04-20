# histdata.com-tools

Multi-threaded/Multi-Process Downloader for Currency Exchange Rates from Histdata.com

## Usage

```sh
$histdatacom -h #shows help message
```
```sh
histdatacom -
```
## In Progress

- Export data to InfluxDB
- Consider converting initial csv to alternative format for more performant processing (feather, parquet, jay, pickle, hdf5)

## Roadmap


## Setup

1. Create a virtual environment
   - `python -m venv venv`

2. Activate the virtual environment
   - `source venv/bin/activate`
   - validate with `which python`
     - result should be `.../histdata_com_tools/venv/bin/python`

3. Install CA certificate (macOS)
   - `pip install certifi`

4. Build and install the app
   - `python setup.py build`
   - `python setup.py install`

5. Apply Backport if not already patched upstream
   - This seems to already be upstream, but you can verify if concerned
   - `lib/python3.XXX/multiprocessing/managers.py > AutoProxy`
   - `https://github.com/python/cpython/commit/8aa45de6c6d84397b772bad7e032744010bbd456`
  
6. Run `histdatacom` to view help message and flags

7. Test a run with `histdatacom -p eurusd -P ascii -t 1-minute-bar-quotes`
    - Data is found in the `data/` directory unless otherwise specified with the -d flag

### CLI Help

```txt
histdatacom -h
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
                        space separated formats. e.g. -P metatrader ascii ninjatrader metastock
  -t TIMEFRAME [TIMEFRAME ...], --timeframes TIMEFRAME [TIMEFRAME ...]
                        space separated Timeframes. e.g. -t tick-data-quotes 1-minute-bar-quotes ...
  -s START_YEARMONTH, --start_yearmonth START_YEARMONTH
                        set a start year and month for data. e.g. -s 2000-04 or -s 2015-00
  -e END_YEARMONTH, --end_yearmonth END_YEARMONTH
                        set a start year and month for data. e.g. -s 2020-00 or -s 2022-04
  -d DATA_DIRECTORY, --data-directory DATA_DIRECTORY
                        Not an Executable Search Path! This directory is used to perform work. default is "data" in the current directory
```
