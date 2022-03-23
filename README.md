# histdata.com-tools
Multi-threaded/Multi-Process Downloader for Currency Exchange Rates from Histdata.com

## Features

- Mutlithreaded for web requests/downloads
- Multiprocess for zip file extraction
- Uses a queue and saves state to allow long running requests to be canceled and resumed at a later time.
## In Progress
- Export data to InfluxDB
## Setup
1. Create a virtual environment
   - `python -m venv venv`
2. Activate the virtual environment
   - `source venv/bin/activate`
   - validate with `which python`
    - result should be `.../histdata.com-tools/venv/bin/python`
3. Install dependencies
   - `pip install influxdb_client rich requests bs4 pytest`
4. Apply Backport if not already patched upstream
   - This seems to already be upstream, but you can verify if concerned
   - `lib/python3.XXX/multiprocessing/managers.py > AutoProxy`
   - `https://github.com/python/cpython/commit/8aa45de6c6d84397b772bad7e032744010bbd456`
5. Run `python histdata_com.py` to view help message and flags
6. Test a run with `python histdata_com.py -p eurusd -P ascii -t tick-data-quotes`
    - Data is found in the `.data/` directory unless otherwise specified with the -d flag
### CLI Help

```txt
usage: histdata_com.py [-h] [-p PAIR [PAIR ...]] [-P PLATFORM [PLATFORM ...]] [-t TIMEFRAME [TIMEFRAME ...]] [-i INDEX_URL] [-c HTML_CONTAINER_CLASSNAME]
                       [-d WORKING_DATA_DIRECTORY]
                       {run} ...

positional arguments:
  {run}
    run                 Use "run -d" to execute using the default settings

options:
  -h, --help            show this help message and exit
  -p PAIR [PAIR ...], --pairs PAIR [PAIR ...]
                        space separated currency pairs. e.g. -p eurusd usdjpy ...
  -P PLATFORM [PLATFORM ...], --platforms PLATFORM [PLATFORM ...]
                        space separated Platforms. e.g. -P metatrader ascii excel ninjatrader metastock
  -t TIMEFRAME [TIMEFRAME ...], --timeframes TIMEFRAME [TIMEFRAME ...]
                        space separated Timeframes. e.g. -t tick-data-quotes 1-minute-bar-quotes ...
  -i INDEX_URL, --index_url INDEX_URL
                        url to the web page that will be scraped for top-order links
  -c HTML_CONTAINER_CLASSNAME, --html_container_classname HTML_CONTAINER_CLASSNAME
                        search url content for parent element with this html/css class for links
  -d WORKING_DATA_DIRECTORY, --working-data-directory WORKING_DATA_DIRECTORY
                        Not an Executable Search Path! This directory is used to perform work. default is ".data" in the current direcotry
```