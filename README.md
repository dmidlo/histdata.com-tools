# histdata.com-tools

A Multi-threaded/Multi-Process command-line utility and python package that downloads currency exchange rates from Histdata.com. Imports to InfluxDB. Can be used in Jupyter Notebooks. Works on MacOS, Linux & Windows Systems.

---

- [histdata.com-tools](#histdatacom-tools)
- [Disclaimer](#disclaimer)
- [Setup](#setup)
  - [TLDR for all platforms](#tldr-for-all-platforms)
  - [MacOS and Linux](#macos-and-linux)
  - [Windows Powershell](#windows-powershell)
- [Usage](#usage)
  - [Show the Help and Options](#show-the-help-and-options)
  - [Basic Use](#basic-use)
  - [Available Formats](#available-formats)
  - [Date Ranges](#date-ranges)
    - ['Start' & 'Now' Keywords](#start-now-keywords)
  - [Multiple Datasets](#multiple-datasets)
  - [Import to InfluxDB](#import-to-influxdb)
    - [influxdb.yaml](#influxdbyaml)
  - [Other Scripts, Modules, & Jupyter Support](#other-scripts-modules-jupyter-support)
- [Roadmap](#roadmap)

---

## Disclaimer

*I am in no way affiliated with histdata.com or its maintainers. Please use this application in a way that respects the hard work and resources of histdata.com*

*If you choose to use this tool, it is **strongly** suggested that you head over to http://www.histdata.com/download-by-ftp/ and sign up to help support their traffic costs.*

*If you find this tool helpful and would like to support future development, I'm in need of caffeine, feel free to [buy me coffee!](https://www.buymeacoffee.com/dmidlo)*

### Setup

#### TLDR for all platforms

```sh
pip install histdatacom
```

---

##### MacOS and Linux

###### Create a new project directory and change to it

```bash
mkdir myproject && cd myproject && pwd
```

###### Create a Python Virtual Environment and activate it

```bash
python -m venv venv && source venv/bin/activate
```

###### Confirm Python Path and Verion

```bash
which python && python --version
```

###### Install the histdata.com-tools package from PyPi

```bash
pip install histdatacom
```

###### Run `histdatacom` to view help message and Options

```bash
histdatacom -h
```

#### Windows Powershell

---

###### Launch a Powershell Terminal.
   - Run as Administrator (right-click on shortcut and click Run as Admin...)

###### Make sure python3.10 is in your system's executable path.

```powershell
python --version
```

- should be already set if you clicked the checkbox when installing python 3.10
- If not, you can run the following.
  - you will need to relauch powershell as admin.

```powershell
[Environment]::SetEnvironmentVariable("Path", "$env:Path;C:\Program Files\Python310")
```

###### Change the Execution Policy to Unrestricted

```powershell
Set-ExecutionPolicy Unrestricted -Force
```

###### Create a new project directory and change to it

```powershell
New-Item -Path ".\" -Name "myproject" -ItemType "directory" -and Set-Location .\myproject\
```

###### Create a Python Virtual Environment and activate it

```powershell
python -m venv venv -and .\venv\Scripts\Activate.ps1
```

###### Confirm Python Path and Verion

```powershell
Get-Command python | select Source -and python --version
```

###### Install the histdata.com-tools package from github

```powershell
pip install histdatacom
```

###### Run `histdatacom` to view help message

```powershell
histdatacom -h
```

### Usage

**Note #1**
The number one rule when using this tool is to be **MORE** specific with your input to limit the size of your request.

**Note #2**
*histdatacom is a very powerful tool and has the capability to fetch the entire repository housed on histdata.com. This is **NEVER** necessary. If you are using this tool to fetch data for your favorite trading application, do not download data in all available formats.*

*It is likely the default behavior will be modified from its current state to discourage unnecessarily large requests.*

**please submit feature requests and bug reports using this repository's issue tracker.*

###### Show the help and options

```txt
histdatacom -h
```

```txt
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
                        Directory Used to save data. default is "data" in the current directory
```

##### Basic Use

###### Download and extract the current month's available EURUSD data for metatrader 4/5into the default data directory ./data

```sh
histdatacom -p eurusd -f metatrader -s now
```

###### include the `-D` flag to download but NOT extract to csv.

```sh
histdatacom -D -p usdcad -f metastock -s now
```

##### Available Formats

The formats available are:

||
|-----------|
|metatrader|
|metastock|
|ninjatrader|
|excel|
|ascii|

 histdata.com provides different resolutions of time
 depending on the format.

 The following format/timeframe combinations are available:

|||
|------------------|:-----------:|
|1-minute-bar-quotes|all formats|
|tick-data-quotes |ascii|
|tick-last-quotes|ninjatrader|
|tick-bid-quotes|ninjatrader|
|tick-ask-quotes|ninjatrader|


###### To download 1-minute-bar-quotes for both metastock and excel

```sh
histdatacom -p usdjpy -f metastock excel -s now 
```

##### Date Ranges

date ranges are for year and month and can be specified in the following ways:
 | [ -._] |
|-------|
|2022-04|
|"2202 04"|
|2202.04|
|2202_04|


###### to fetch a single year's data, leave out the month

- note: unless you're fetching data for the current year, tick data types will fetch 12 files for each month of the year, 1-minute-bar-quotes will fetch a single OHLC file with the whole year's data.

```txt
histdatacom -p udxusd -f ascii -t tick-data-quotes -s 2011
```

###### to fetch a single month's data, include a month, but do not use the `-e, --end_yearmonth` flag.

- if you're requesting 1-minute-bar-quotes for any
    year except the current year, you will receive the
    the whole year's data
- this example leaves out the `-p --pair` flag, and will
    fetch data for all 66 available instruments

```txt
histdatacom -f metatrader -s 2012-07
```

##### `Start` & `Now` Keywords

you may hav noticed that two special year-month keywords exist
 `start` and `now`

- `start` may only be used with the `-s --start_yearmonth`
   flag and the `-e --end_yearmonth` flag **must** be specified
   to indicate a range of data

```txt
histdatacom -p audusd -f metatrader -s start -e 2008-12
```

- `now` used alone will return the current year-month
- when used with as `-s now` it will return the most current month's data

```txt
histdatacom -p frxeur -f ninjatrader -s now
```

in the above example, no `-t --timeframe` flag was specified. This will return all time resolutions available for the specified format(s)

`now` when used with the `-e --end_yearmonth` flag is intended to be the end of a range. Rather, if the flags were to be `-s 2019-04 -e now` the request would return data from April 2019-04 to the present.

```txt
histdatacom -p xagusd -f ascii -1-minute-bar-quotes -s 2019-04 -e now
```

###### Multiple Datasets

###### multiple datasets can be requested in one command
this example with use the `-e --end_yearmonth` flag to request a range of data for multiple instruments.

- note: Large requests like these are to be avoided. remember to sign up with histdata.com to help them pay for network costs

```txt
histdatacom -p eurusd usdcad udxusd -f metatrader -s start -e 2017-04
```

##### Import to InfluxDB

To import data to an influxdb instance, use the `-I --import_to_influxdb` flag along with an `influxdb.yaml` file in the current working directory (where ever you are running the command from).

- ascii is the only format accepted for influxdb import.
- all histdata.com datetime data is in EST (Eastern Standard Time) with no adjustments for daylight savings.
- Influxdb does not adjust for timezone and all datetime data is recorded as UTC epoch timestamps (nano-seconds since midnight 00:00, January, 1st, 1970)
- this tool converts histdata.com ESTnoDST to UTC Epoch milli-second timestamps as part of the import-to-influx process

```txt
histdatacom -I -p eurusd -f ascii -t tick-data-quotes -s start -e now
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

Basic support for Jupyter notebooks and calling from another script/module

- there is no return value from calling histdatacom, it functions only as far as the cli version does, that is, that it will validate, download, extract, and/or import to influxdb.
- After that, It would be up the developer to work with the files on disk or to query influxdb.
- for progress bars in jupyter you will need to install the ipywidgets package with `pip install ipywidgets`

######  First import the required modules
```python
import histdatacom
from histdatacom.cli import ArgsNamespace
```

###### Create a new options object to pass parameters to histdatacom

```python
options = ArgsNamespace
```

###### Configure

```python
options.extract_csvs = True
options.formats = {"ascii"}
options.timeframes = {"tick-data-quotes"}
options.pairs = {"audusd","udxusd","eurusd"}
options.start_yearmonth = "2022-03"
options.end_yearmonth = "2022-04"
```

###### pass the options to histdatacom (Jupyter Notebooks)

```python
histdatacom(options)  # (Jupyter)
```

at present, calling from another script or module is limited to using the `__name__=="__main__"` idiom.

```python
if __name__=="__main__": 
   histdatacom(options)
```


## Roadmap

- return datatable/pandas/dask dataframe when called from jupyter or another module
