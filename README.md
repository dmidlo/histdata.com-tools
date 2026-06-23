# histdata.com-tools

A command-line utility and Python ETL package that downloads currency exchange
rates from Histdata.com. The Temporal sidecar is the default runtime; InfluxDB,
Jupyter, and alternate dataframe return formats are available through extras.
Works on MacOS, Linux & Windows Systems.
**Requires Python3.10+**

**NEW:** Expanded API support!!!

[![Downloads](https://pepy.tech/badge/histdatacom)](https://pepy.tech/project/histdatacom) ![PyPI - License](https://img.shields.io/pypi/l/histdatacom) ![PyPI](https://img.shields.io/pypi/v/histdatacom) ![PyPI - Status](https://img.shields.io/pypi/status/histdatacom)

---

- [histdata.com-tools](#histdatacom-tools)
- [Disclaimer](#disclaimer)
- [Usage](#usage)
  - [Show the Help and Options](#show-the-help-and-options)
  - [Basic Use](#basic-use)
  - [Available Formats](#available-formats)
    - [CSV Dialect and Format Specifications](#csv-dialect-and-format-specifications)
  - [Date Ranges](#date-ranges)
    - ['Start' & 'Now' Keywords](#start-now-keywords)
  - [Multiple Datasets](#multiple-datasets)
  - [CPU Utilization](#cpu-utilization)
  - [Import to InfluxDB](#import-to-influxdb)
    - [Docker-backed InfluxDB Smoke](#docker-backed-influxdb-smoke)
    - [influxdb.yaml](#influxdbyaml)
  - [Data Quality Assessments](#data-quality-assessments)
    - [Quality Targets and Check Groups](#quality-targets-and-check-groups)
    - [Clean and Failing Examples](#clean-and-failing-examples)
    - [Warning, Error, and Exit Policy](#warning-error-and-exit-policy)
  - [Temporal Sidecar Compatibility](#temporal-sidecar-compatibility)
    - [Runtime Model and Install Surface](#runtime-model-and-install-surface)
    - [Lifecycle and Diagnostics](#lifecycle-and-diagnostics)
    - [Sidecar Jobs and Automation](#sidecar-jobs-and-automation)
    - [Sidecar Troubleshooting and Contributor Docs](#sidecar-troubleshooting-and-contributor-docs)
  - [API - Other Scripts, Modules, & Jupyter Support](#api-other-scripts-modules-jupyter-support)
    - [CLI Automation](#cli-automation)
    - [Jupyter and External Scripts](#jupyter-and-external-scripts)
    - [Full Script Example](#full-script-example)
- [Setup](#setup)
  - [TLDR for all platforms](#tldr-for-all-platforms)
  - [Developer Setup](#developer-setup)
  - [Vanilla Python Setup](#vanilla-python-setup)
    - [Vanilla MacOS and Linux](#vanilla-macos-and-linux)
    - [Vanilla Windows Powershell](#vanilla-windows-powershell)
  - [Anaconda Setup](#anaconda-setup)
    - [Anaconda MacOS and Linux](#anaconda-macos-and-linux)
    - [Anaconda Windows using the Anaconda Prompt](#anaconda-windows-using-the-anaconda-prompt)
- [Roadmap](#roadmap)

---

## Disclaimer

**I am in no way affiliated with histdata.com or its maintainers. Please use this application in a way that respects the hard work and resources of histdata.com*

*If you choose to use this tool, it is **strongly** suggested that you head over to [http://www.histdata.com/download-by-ftp/](http://www.histdata.com/download-by-ftp/) and sign up to help support their traffic costs.*

*If you find this tool helpful and would like to support future development, I'm in need of caffeine, feel free to [buy me coffee!](https://www.buymeacoffee.com/dmidlo)*

---

## Usage

**Note #1**
The number one rule when using this tool is to be **MORE** specific with your input to limit the size of your request.

**Note #2**
*histdatacom is a very powerful tool and has the capability to fetch the entire repository housed on histdata.com. This is **NEVER** necessary. If you are using this tool to fetch data for your favorite trading application, do not download data in all available formats.*

*It is likely the default behavior will be modified from its current state to discourage unnecessarily large requests.*

**please submit feature requests and bug reports using this repository's [issue tracker](https://github.com/dmidlo/histdata.com-tools/issues).*

### Show the help and options

```txt
histdatacom -h
```

```txt
usage: histdatacom [-h] [-A] [-U] [--by BY] [--version] [-V] [-D] [-X] [-p PAIR [PAIR ...]] [-f FORMAT [FORMAT ...]] [-t TIMEFRAME [TIMEFRAME ...]] [-s START_YEARMONTH] [-e END_YEARMONTH] [-I] [-d] [-b BATCH_SIZE] [-c CPU_UTILIZATION]
                   [--data-directory DATA_DIRECTORY] [--sidecar] [--sidecar-start] [--no-sidecar-start] [--sidecar-submit-only] [--quality] [--quality-target PATH [PATH ...]] [--quality-checks GROUP [GROUP ...]]
                   [--quality-report PATH] [--quality-fail-on SEVERITY] [--quality-max-errors COUNT] [--quality-max-warnings COUNT]

options:
  -h, --help            show this help message and exit

Mode:
  -V, --validate_urls   Check generated list of URLs as valid download
                        locations
  -D, --download_data_archives
                        download specified pairs/formats/timeframe and create
                        data files
  -X, --extract_csvs    histdata.com delivers zip files. Use the -X flag to
                        extract them.

Config:
  -p, --pairs PAIR [PAIR ...]
                        space separated currency pairs. e.g. -p eurusd usdjpy
                        ...
  -f, --formats FORMAT [FORMAT ...]
                        space separated formats. -f metatrader ascii
                        ninjatrader metastock
  -t, --timeframes TIMEFRAME [TIMEFRAME ...]
                        space separated Timeframes. -t tick-data-quotes
                        1-minute-bar-quotes
  -s, --start_yearmonth START_YEARMONTH
                        set a start year and month for data. e.g. -s 2000-04
                        or -s 2015-00
  -e, --end_yearmonth END_YEARMONTH
                        set a start year and month for data. e.g. -e 2020-00
                        or -e 2022-04

Influxdb:
  -I, --import_to_influxdb
                        import data to influxdb instance. Use influxdb.yaml to
                        configure.
  -d, --delete_after_influx
                        delete data files after upload to influxdb
  -b, --batch_size BATCH_SIZE
                        (integer) influxdb write_api batch size. defaults to
                        5000

System:
  -c, --cpu_utilization CPU_UTILIZATION
                        "low", "medium", "high". High uses all available CPUs
                        OR integer percent 1-200
  --data-directory DATA_DIRECTORY
                        Directory Used to save data. default is "./data/"

Sidecar:
  --sidecar             submit this run to the local Temporal sidecar (default
                        runtime)
  --sidecar-start       start the sidecar server and worker fleet only when no
                        healthy sidecar is running
  --no-sidecar-start    submit to the sidecar only when a healthy sidecar is
                        already running
  --sidecar-submit-only
                        submit the sidecar job without waiting for its result

Data quality:
  --quality             run offline data-quality assessment against local
                        datasets without contacting HistData.com
  --quality-target, --quality-path PATH [PATH ...]
                        local file or directory to assess; supports
                        directories, HistData ZIP archives, CSV files, and
                        .data cache files
  --quality-checks GROUP [GROUP ...]
                        quality check groups to run; defaults to all.
                        Supported: all, inventory, ingestion, time, bars,
                        ticks, domain, modeling
  --quality-report PATH
                        write the full machine-readable JSON quality report to
                        PATH
  --quality-fail-on SEVERITY
                        exit non-zero when configured thresholds are exceeded
                        for error, warning, or never. Defaults to error
  --quality-max-errors COUNT
                        maximum error findings allowed before quality mode
                        exits non-zero; defaults to 0
  --quality-max-warnings COUNT
                        maximum warning findings allowed before quality mode
                        exits non-zero when --quality-fail-on warning is
                        selected; defaults to 0

Info:
  -A, --available_remote_data
                        list data retrievable from histdata.com
  -U, --update_remote_data
                        update list of data retrievable from histdata.com
  --by BY               With -A, -U, to sort --by [pair_asc, pair_dsc,
                        start_asc, start_dsc]
  --version             return current version of histdatacom.
```

---

### Basic Use

#### Download and extract the current month's available EURUSD data for metatrader 4/5into the default data directory ./data

```sh
histdatacom -p eurusd -f metatrader -s now
```

#### include the `-D` flag to download but NOT extract to csv

```sh
histdatacom -D -p usdcad -f metastock -s now
```

---

#### Available Formats

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

##### CSV Dialect and Format Specifications

- *For Detailed specifications for the CSVs that the histdata.com repo provides see [histdata.com_data_specs.md](https://github.com/dmidlo/histdata.com-tools/blob/main/histdata.com_data_specs.md)*

##### To download 1-minute-bar-quotes for both metastock and excel

```sh
histdatacom -p usdjpy -f metastock excel -s now
```

---

#### Date Ranges

date ranges are for year and month and can be specified in the following ways:
 | [ -._] |
|-------|
|2022-04|
|"2202 04"|
|2202.04|
|2202_04|

##### to fetch a single year's data, leave out the month

- note: unless you're fetching data for the current year, tick data types will fetch 12 files for each month of the year, 1-minute-bar-quotes will fetch a single OHLC file with the whole year's data.

```txt
histdatacom -p udxusd -f ascii -t tick-data-quotes -s 2011
```

##### to fetch a single month's data, include a month, but do not use the `-e, --end_yearmonth` flag

- if you're requesting 1-minute-bar-quotes for any
    year except the current year, you will receive the
    the whole year's data
- this example leaves out the `-p --pair` flag, and will
    fetch data for all 66 available instruments

```txt
histdatacom -f metatrader -s 2012-07
```

#### `Start` & `Now` Keywords

you may have noticed that two special year-month keywords exist
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

---

##### Multiple Datasets

##### multiple datasets can be requested in one command

this example with use the `-e --end_yearmonth` flag to request a range of data for multiple instruments.

- note: Large requests like these are to be avoided. remember to sign up with histdata.com to help them pay for network costs

```txt
histdatacom -p eurusd usdcad udxusd -f metatrader -s start -e 2017-04
```

---

##### CPU Utilization

One can set a cap on CPU Utilization with `-c --cpu_utilization`

- available levels are, `"low"`,`"medium"`,`"high"`
- **OR**
- integer percent 1-200
  eg. `-c 100` is equal to `-c high`

```sh
histdatacom -c medium -p udxusd -f metatrader -s 2015-04 -e 2016-04
```

---

### Import to InfluxDB

To import data to an influxdb instance, install the Influx extra and use the `-I --import_to_influxdb` flag along with an `influxdb.yaml` file in the current working directory (where ever you are running the command from).

```sh
pip install "histdatacom[influx]"
```

- ascii is the only format accepted for influxdb import.
- all histdata.com datetime data is in EST (Eastern Standard Time) with no adjustments for daylight savings.
- Influxdb does not adjust for timezone and all datetime data is recorded as UTC epoch timestamps (nano-seconds since midnight 00:00, January, 1st, 1970)
- this tool converts histdata.com ESTnoDST to UTC Epoch milli-second timestamps as part of the import-to-influx process

```txt
histdatacom -I -p eurusd -f ascii -t tick-data-quotes -s start -e now
```

#### Docker-backed InfluxDB Smoke

When Docker is available, contributors can run a disposable InfluxDB v2 smoke
without a user-managed `influxdb.yaml`:

```sh
python scripts/smoke_influx_docker.py
```

The smoke starts `influxdb:2.7-alpine`, writes representative HistData M1 and
tick line-protocol batches through the real `InfluxBatchWriter`, queries the
bucket, reports the field count, and removes the container. It is intentionally
not part of default pytest because it depends on Docker and a pullable InfluxDB
image.

#### influxdb.yaml

```yaml
# a sample influxdb.yaml file.
influxdb:
  org: influx_org
  bucket: data_bucket
  url: influx_server_api_url
  token: influx_user_token
```

##### Download influxdb.yaml to your project's directory

```shell
curl "https://raw.githubusercontent.com/dmidlo/histdata.com-tools/main/influxdb.sample.yaml" --output influxdb.yaml
```

---

### Data Quality Assessments

`histdatacom --quality` runs offline checks against datasets that are already on
disk. It does not contact HistData.com or InfluxDB; it submits a local Temporal
sidecar `DataQualityWorkflow` that runs CPU/file activities and writes detailed
JSON reports as disk artifacts. Use it after downloading or extracting data,
before trusting local ZIP, CSV, or cache artifacts for import, modeling, or
backtesting.

```sh
histdatacom --quality --quality-target data/ --quality-report reports/quality.json
```

The command prints a human summary and can also write a full JSON report. If no
`--quality-target` is passed, quality mode uses the configured data directory.
Targets can be plain HistData CSV files, HistData ZIP archives, directories
containing those files, or the canonical `.data` cache file.

Use `--repo-quality` when the same quality run should also update the local
repo helper file with bounded per-instrument quality summaries:

```sh
histdatacom --repo-quality --quality-target data/ --quality-report reports/quality.json
```

The `.repo` quality metadata stores summary counts, status, checked groups,
formats/timeframes/periods, and report artifact references. Detailed findings
stay in the JSON quality report on disk. Ordinary `-A` and `-U` repository
list/update commands do not run quality checks. To display stored quality
columns in repository output, use:

```sh
histdatacom -A --repo-quality-columns
```

#### Quality Targets and Check Groups

Quality groups are composable. `all` is the default and cannot be combined with
specific groups in the same command.

```sh
histdatacom --quality --quality-target data/ --quality-checks inventory ingestion
histdatacom --quality --quality-target data/DAT_ASCII_EURUSD_M1_201202.csv --quality-checks time bars
histdatacom --quality --quality-target data/DAT_ASCII_EURUSD_T_201202.zip --quality-checks ticks domain
```

Supported groups:

| Group | Scope |
| --- | --- |
| `inventory` | ZIP integrity, filename metadata, expected coverage manifest |
| `ingestion` | text readability, line endings, delimiter/header checks, schema and typed parsing, row-count anomalies |
| `time` | EST-no-DST to UTC normalization, month boundaries, ordering, duplicates, granularity, gaps, cross-file continuity |
| `bars` | M1 bid OHLC integrity, positive prices, precision, outliers, tick-to-M1 reconstruction |
| `ticks` | tick bid/ask ordering, spread, duplicate/stale/burst/one-sided quote behavior, spread regimes |
| `domain` | symbol metadata, quote conventions, calendar/session tags, cross-instrument consistency |
| `modeling` | advisory modeling-readiness checks for bid-only bars, leakage risk, spread-cost assumptions, target horizon feasibility |

HistData-specific assumptions are reported directly in findings:

- ASCII M1 rows are bid-based OHLCV bars.
- ASCII tick rows include bid and ask values.
- HistData timestamps are interpreted as fixed EST with no daylight-saving
  adjustment and normalized to UTC.
- M1 `volume` is not treated as automatically meaningful or required for
  market-quality decisions.

#### Clean and Failing Examples

A focused ingestion run against a clean M1 CSV reports a clean file and writes a
machine-readable report:

```sh
histdatacom --quality \
  --quality-target data/DAT_ASCII_EURUSD_M1_201202.csv \
  --quality-checks ingestion \
  --quality-report reports/quality-clean.json
```

```txt
Data quality assessment
checks: ingestion
status: clean
targets: 1 clean: 1 warning: 0 failed: 0
findings: 1 info: 1 warning: 0 error: 0
report: /path/to/reports/quality-clean.json

Clean files
- csv: /path/to/data/DAT_ASCII_EURUSD_M1_201202.csv (findings=1, warnings=0, errors=0)

Warning files
- none

Failed files
- none
```

The JSON report includes deterministic top-level summary fields:

```json
{
  "schema_version": "histdatacom.quality-report.v1",
  "summary": {
    "error_count": 0,
    "finding_count": 1,
    "info_count": 1,
    "max_severity": "info",
    "rule_count": 3,
    "status": "clean",
    "target_count": 1,
    "warning_count": 0
  }
}
```

A malformed M1 CSV fails ingestion and exits nonzero by default because
`--quality-fail-on error` with `--quality-max-errors 0` is the default policy:

```sh
histdatacom --quality \
  --quality-target data/bad/ \
  --quality-checks ingestion \
  --quality-report reports/quality-failing.json
```

```txt
Data quality assessment
checks: ingestion
status: failed
targets: 1 clean: 0 warning: 0 failed: 1
findings: 2 info: 1 warning: 0 error: 1
report: /path/to/reports/quality-failing.json

Clean files
- none

Warning files
- none

Failed files
- csv: /path/to/data/bad/DAT_ASCII_EURUSD_M1_201202_BAD.csv (findings=2, warnings=0, errors=1)
```

The detailed report carries row and field context for automation and manual
investigation:

```json
{
  "schema_version": "histdatacom.quality-report.v1",
  "summary": {
    "error_count": 1,
    "finding_count": 2,
    "max_severity": "error",
    "status": "failed",
    "target_count": 1,
    "warning_count": 0
  },
  "rule_results": [
    {
      "rule_id": "ingestion.ascii.schema",
      "findings": [
        {
          "code": "ASCII_ROW_FIELD_COUNT_INVALID",
          "severity": "error",
          "location": {
            "row_number": 2
          }
        }
      ]
    }
  ]
}
```

#### Warning, Error, and Exit Policy

Quality findings use three severities:

- `info`: informational summaries and profiles.
- `warning`: suspicious data, domain assumptions, or modeling-readiness risks
  that should be reviewed but do not block ingestion by default.
- `error`: hard defects such as corrupt ZIP archives, unreadable files, schema
  violations, parse failures, invalid OHLC relationships, or negative spreads.

Target status rolls up from findings: any error makes a target `failed`; warnings
without errors make it `warning`; otherwise it is `clean`.

The default process exit policy fails on any error:

```sh
histdatacom --quality --quality-target data/
```

To make warnings fail CI, opt in explicitly:

```sh
histdatacom --quality \
  --quality-target data/ \
  --quality-fail-on warning \
  --quality-max-warnings 0
```

To generate advisory reports without failing a job, disable quality exits:

```sh
histdatacom --quality \
  --quality-target data/ \
  --quality-fail-on never \
  --quality-report reports/quality.json
```

For CI/offline use, run against checked-in fixtures or downloaded artifacts in a
workspace cache. The command needs only local filesystem access; network access,
HistData.com availability, Temporal, and InfluxDB are not required.

---

### Temporal Sidecar Compatibility

The production default is now the local Temporal sidecar for CLI and API runs.
Default requests submit a `RunRequest` to the sidecar and start the bundled
local sidecar when no healthy sidecar is running. The `--sidecar` flag remains
accepted as an explicit compatibility alias for automation that already passed
it during the migration.

The foreground rollback runtime has been removed after its release-window
deprecation period. `--foreground` is no longer a valid CLI flag, and API code
that sets `options.use_sidecar = False` raises a clear `ValueError`. If the
sidecar cannot be started or contacted, CLI calls exit nonzero with a clear
error and API calls raise `SidecarUnavailableError`; the runtime never silently
falls back to a local non-sidecar execution path.

#### Runtime Model and Install Surface

The base install includes the Temporal Python SDK because the sidecar is the
default runtime:

```sh
pip install histdatacom
```

`histdatacom[temporal]` remains accepted as a backwards-compatible extra name
for automation that installed the sidecar dependency surface during migration,
but it does not change the default runtime contract: base installs include the
Temporal SDK needed by sidecar clients and workers.

The sidecar stores Temporal process state, SQLite history, logs, and runtime
manifests under a per-user, per-workspace runtime directory. Downloaded ZIP
files, extracted CSV/XLSX files, cache IPC files, and merged API artifacts stay
under the existing HistData data-directory policy.

Record status metadata is manifest-only for new writes. Normal CLI/API and
sidecar paths update `.histdatacom/manifest-status.sqlite3` under the relevant
data or sidecar status root and no longer create new hidden `.meta` files beside
records. Existing `.meta` files remain readable as migration inputs; successful
imports write the manifest row and remove the legacy file, while missing or
corrupt legacy files are reported without blocking manifest-backed operation.

Source distributions and universal fallback wheels include sidecar metadata and
CLI entry points. Platform wheels can bundle the Temporal server executable
under the sidecar package resources. On a bundled platform wheel,
`histdatacom-sidecar start` works without `--executable`; on metadata-only
artifacts and unsupported platforms, default sidecar startup requires an
operator-provided Temporal executable through the sidecar lifecycle command.
The bundled executable and the Python Temporal SDK are separate concerns: base
installs provide the SDK, while bundled platform wheels provide the local
Temporal server executable.

Default sidecar submissions are built from resolved runtime context and
`RunRequest` payloads. New orchestration work should use `RunRequest`,
sidecar workflows, and sidecar activities. Legacy helper surfaces now accept
explicit argument dictionaries rather than ambient parser state; parser globals
are not part of runtime selection.

#### Public Sidecar API Boundary

New GUI and automation integrations should submit work through the sidecar-era
public surface:

- `histdatacom.Options` passed to `histdatacom.main(options)` or
  `histdatacom(options)`
- `histdatacom.sidecar.contracts.RunRequest`
- `histdatacom-sidecar` lifecycle and jobs commands
- `histdatacom.sidecar.client` job-control helpers for submit, inspect, list,
  cancel, resume, progress, and artifact polling

Do not build new validate/download/extract/cache/import automation by importing
`Repo`, `Scraper`, `Api.validate_caches`, `Api.merge_caches`, or
`Influx.import_data` directly. Those direct side-effect methods remain as
compatibility helpers for existing callers and emit
`LegacyHelperSideEffectWarning` when used. Temporal activities continue to call
the lower-level `histdatacom.activity_stages` functions and related adapter
objects directly; those stage helpers are the supported worker boundary, not
the GUI or automation boundary.

#### Lifecycle and Diagnostics

Use the lifecycle CLI to inspect and manage the local sidecar:

```sh
histdatacom-sidecar doctor --json
histdatacom-sidecar status --json
histdatacom-sidecar start
histdatacom-sidecar start --executable /path/to/temporal
histdatacom-sidecar stop
```

`status` and `doctor` report component health for the server and each worker
lane: `orchestration`, `network`, `cpu-file`, and `influx`.

`histdatacom sidecar ...` is also routed through the top-level command. Use
`--workspace` or `HISTDATACOM_SIDECAR_WORKSPACE` for cron, service managers,
GUI launchers, and other contexts where the current working directory may not
be stable.

#### Sidecar Jobs and Automation

Submit a job through the default sidecar runtime:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Submit without waiting for completion:

```sh
histdatacom --sidecar-submit-only -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

The JSON control surface supports job inspection and future GUI polling:

```sh
histdatacom-sidecar jobs list --json
histdatacom-sidecar jobs progress histdatacom-<request-id> --json
histdatacom-sidecar jobs artifacts histdatacom-<request-id> --json
histdatacom-sidecar jobs cancel histdatacom-<request-id> --reason "operator stop"
```

- `histdatacom --version` stays local and does not require the sidecar.
- `-A`, `-U`, `-V`, `-D`, `-X`, and `-I` keep their existing option semantics before a sidecar request is submitted.
- `--foreground` has been removed and is rejected by the CLI.
- `--sidecar-start` starts the server and worker lane fleet only when no healthy sidecar is running.
- `--no-sidecar-start` requires an already-running healthy sidecar and fails
  clearly instead of starting one.
- `--sidecar-submit-only` submits a job and returns job metadata instead of waiting for cache artifacts or workflow results.
- Waited sidecar `-A` / `-U` repository requests keep the historical output contract: API calls return the available-data dictionary, and CLI calls render the repository table.
- API calls with `options.api_return_type` return the requested `polars`, `pandas`, or `arrow` object after a completed sidecar job by materializing cache artifacts on disk.
- If the sidecar is unavailable, CLI calls exit nonzero with a clear error and API calls raise `SidecarUnavailableError`.

Sidecar-backed API calls use the same public `Options` object and sidecar
defaults:

```python
options.sidecar_wait_result = True
options.api_return_type = "polars"
```

Set `options.sidecar_wait_result = False` to submit a job and receive sidecar
job metadata instead of a materialized API return object. Set
`options.sidecar_start = False` when a caller requires a pre-started sidecar.
`options.use_sidecar = False` is no longer supported.

#### Sidecar Troubleshooting and Contributor Docs

See [Temporal Sidecar Operations](docs/temporal-sidecar-operations.md) for the
runtime path layout, port policy, lifecycle commands, job controls,
cancellation/resume behavior, and troubleshooting guidance. See
[Temporal Workflow Topology](docs/temporal-workflow-topology.md) for workflow,
activity, task queue, and testing boundaries. See
[Temporal Sidecar Performance Baseline](docs/temporal-sidecar-performance.md)
for lane sizing and benchmark policy.

---

### API - Other Scripts, Modules, & Jupyter Support

histdatacom also has an API to allow developers and to integrate the package into their own projects.  It can be used in one of two ways; The first being a simple interface to automate CLI interaction. The second is as an interface to work with the data directly in a notebook environment like Jupyter Notebooks.

---

#### CLI Automation

##### First import the required modules

```python
import histdatacom
from histdatacom.options import Options
```

##### Create and Initialize a new options object to pass parameters to histdatacom

```python
options = Options()
```

##### Configure for CLI automation

To automate the CLI, simply include one of the boolean behavior flags: `options.validate_urls`, `options.download_data_archives`, `options.extract_csvs`, and `options.import_to_influxdb`

- Each behavior flag implies the use of the preceding flags.
  - histdatacom is an ETL pipeline (extract, transform, load) and each step depends on the preceding steps in the pipeline.
  - For the `CLI`, the order of operations are:
    - validate urls
    - download zip files from histdata.com
    - extract the csv from the zip archive
    - transform the ESTnoDST datetime to UTC Epoch `AND` upload to InfluxDB.

```python
# options.validate_urls = True
# options.download_data_archives = True  # implies validate
options.extract_csvs = True  # implies validate and download
# options.import_to_influxdb = True  # implies validate, download, and extract
options.formats = {"ascii"}
options.timeframes = {"tick-data-quotes"}
options.pairs = {"eurusd"}
options.start_yearmonth = "2021-04"
options.end_yearmonth = "now"
options.cpu_utilization = 100
```

- Automation requests submit through the sidecar by default and start a missing
  bundled sidecar when needed. Set `options.sidecar_wait_result = False` when
  the caller only needs job metadata, set `options.sidecar_start = False` when
  a caller requires a pre-started sidecar. `options.use_sidecar = False` is
  rejected because the foreground runtime has been removed.

- New automation should not call legacy helper classes directly for
  validate/download/extract/cache/import work. Direct side-effect helper
  methods warn because they bypass durable sidecar status, cancellation,
  retry/resume, and worker-lane routing.

- when a behavior flag is included, `histdatacom` assumes it is being used for `CLI` automation **exclusively** and does **not** provide a return value.

at present, calling from another script or module is limited to using the `__name__=="__main__"` idiom.

```python
if __name__=="__main__":
   histdatacom(options)
```

***Jupyter may be used normally***

```python
histdatacom(options)  # (Jupyter)
```

---

#### Jupyter and External Scripts

As opposed to the `CLI` interface, one may wish to load data from histdata.com and work with it interactively (e.g. in a Jupyter notebook), or as part of a larger pipeline.  To that end, histdatacom provides an option to specify a return type.

- return types can be:

  - a `polars` dataframe
  - a `pandas` dataframe
  - a `pyarrow` table

- `polars` is installed with `histdatacom`.
- *to use `pandas` or `arrow` return formats, install the optional extras*
  - `pip install "histdatacom[pandas]"`
  - `pip install "histdatacom[arrow]"`
- *to use InfluxDB imports or notebook tooling, install the corresponding extras*
  - `pip install "histdatacom[influx]"`
  - `pip install "histdatacom[jupyter]"`

- ***All datetime is returned as milliseconds since January 1, 1970 (midnight UTC/GMT)***

##### Import the required modules

```python
import histdatacom
from histdatacom.options import Options
```

##### Initialize a new options object to pass parameters to histdatacom

```python
options = Options()
```

##### Jupyter & External Script Options

```python
options.api_return_type = "polars"  # "polars", "pandas", or "arrow"
options.formats = {"ascii"}  # Must be {"ascii"}
options.timeframes = {"tick-data-quotes"}  # can be tick-data-quotes or 1-minute-bar-quotes
options.pairs = {"eurusd"}
options.start_yearmonth = "2021-04"
options.end_yearmonth = "now"
options.cpu_utilization = "high"
```

- This example uses just one pair/instrument/symbol `eurusd` and just one timeframe `tick-data-quotes`.  When the api is called with this 'one-one` specificity, the api will directly return the requested data.
- Regardless of the specified start_yearmonth and end_yearmonth, the resultant data will be sorted and merged into a single dataset.

##### Pass the options to histdatacom and assign the return to a variable

```python
data = histdatacom(options)  # (Jupyter)

print(data.shape)
print(type(data))
```

```text
(18648498, 4)
<class 'polars.dataframe.frame.DataFrame'>
```

- When specifying more than one pair/symbol/instrument or timeframe, the api will return an ***list of dictionaries*** with references to the timeframe, pair, records used to create the data, and the merged data itself.

```python
options.api_return_type = "pandas"
options.formats = {"ascii"}
options.timeframes = {"1-minute-bar-quotes"}
options.pairs = {"eurusd","usdcad"}
options.start_yearmonth = "2021-01"
options.end_yearmonth = "now"
options.cpu_utilization = "75"
```

```python
data = histdatacom(options)  # (Jupyter)

print(data)
print(type(data))
```

```txt
[
  {
    'timeframe': 'M1',
    'pair': 'EURUSD',
    'records': [<histdatacom.records.Record object ...>, ...],
    'data':
                    datetime     open     high      low    close  vol
      0       1609711200000  1.22396  1.22396  1.22373  1.22395    0
      1       1609711260000  1.22387  1.22420  1.22385  1.22395    0
      2       1609711320000  1.22396  1.22398  1.22382  1.22382    0
      3       1609711380000  1.22383  1.22396  1.22376  1.22378    0
      4       1609711440000  1.22378  1.22385  1.22296  1.22347    0
      ...               ...      ...      ...      ...      ...  ...
      484172  1650664440000  1.07976  1.08014  1.07976  1.08014    0
      484173  1650664500000  1.08013  1.08021  1.07997  1.08000    0
      484174  1650664560000  1.08000  1.08000  1.07956  1.07968    0
      484175  1650664620000  1.07980  1.07980  1.07958  1.07968    0
      484176  1650664680000  1.07980  1.07986  1.07963  1.07963    0

      [484177 rows x 6 columns]
  },
  {
    'timeframe': 'M1',
    'pair': 'USDCAD',
    'records': [<histdatacom.records.Record object ...>, ...],
    'data':
                    datetime     open     high      low    close  vol
      0       1609711200000  1.27136  1.27201  1.27136  1.27201    0
      1       1609711260000  1.27207  1.27241  1.27207  1.27220    0
      2       1609711320000  1.27211  1.27219  1.27211  1.27219    0
      3       1609711380000  1.27212  1.27261  1.27212  1.27261    0
      4       1609711440000  1.27268  1.27268  1.27261  1.27261    0
      ...               ...      ...      ...      ...      ...  ...
      483946  1650664440000  1.27121  1.27132  1.27114  1.27131    0
      483947  1650664500000  1.27129  1.27137  1.27102  1.27106    0
      483948  1650664560000  1.27107  1.27114  1.27098  1.27101    0
      483949  1650664620000  1.27105  1.27105  1.27091  1.27091    0
      483950  1650664680000  1.27091  1.27097  1.27073  1.27097    0

      [483951 rows x 6 columns]
  }
]

<class 'list'>
```

```python
print(data[0]['timeframe'], data[0]['pair'])
print(data[0]['data'])
print(type(data[0]['data']))
```

```txt
M1 EURUSD
               datetime     open     high      low    close  vol
0       20210103 170000  1.22396  1.22396  1.22373  1.22395    0
1       20210103 170100  1.22387  1.22420  1.22385  1.22395    0
2       20210103 170200  1.22396  1.22398  1.22382  1.22382    0
3       20210103 170300  1.22383  1.22396  1.22376  1.22378    0
4       20210103 170400  1.22378  1.22385  1.22296  1.22347    0
...                 ...      ...      ...      ...      ...  ...
484172  20220422 165400  1.07976  1.08014  1.07976  1.08014    0
484173  20220422 165500  1.08013  1.08021  1.07997  1.08000    0
484174  20220422 165600  1.08000  1.08000  1.07956  1.07968    0
484175  20220422 165700  1.07980  1.07980  1.07958  1.07968    0
484176  20220422 165800  1.07980  1.07986  1.07963  1.07963    0

[484177 rows x 6 columns]
<class 'pandas.core.frame.DataFrame'>
```

at present, calling from another script or module is limited to using the `__name__=="__main__"` idiom.

```python
if __name__=="__main__":
   histdatacom(options)
```

***Jupyter may be used normally***

```python
histdatacom(options)  # (Jupyter)
```

##### Full Script Example

```python
import histdatacom
from histdatacom.options import Options
from histdatacom.fx_enums import Pairs

def import_pair_to_influx(pair, start, end):
    data_options = Options()

    data_options.import_to_influxdb = True  # implies validate, download, and extract
    data_options.delete_after_influx = True
    data_options.batch_size = "2000"
    data_options.cpu_utilization = "high"

    data_options.pairs = {f"{pair}"}# histdata_and_oanda_intersect_symbs
    data_options.start_yearmonth = f"{start}"
    data_options.end_yearmonth = f"{end}"
    data_options.formats = {"ascii"}  # Must be {"ascii"}
    data_options.timeframes = {"tick-data-quotes"}  # can be tick-data-quotes or 1-minute-bar-quotes
    histdatacom(data_options)

def get_available_range_data(pairs):
    range_options = Options()
    range_options.pairs = pairs
    range_options.available_remote_data = True
    range_options.by = "start_dsc"
    range_data = histdatacom(range_options)  # (Jupyter)
    return range_data

def print_one_polars_frame(pair, start=None, end=None):
    options = Options()
    options.api_return_type = "polars"
    options.pairs = {f"{pair}"}
    options.start_yearmonth = "201501"
    options.formats = {"ascii"}
    options.timeframes = {"tick-data-quotes"}
    return histdatacom(options)

def main():
    histdata_symbs = Pairs.list_keys()

    # Oanda Symbols:
    oanda_symbs = {"audcad","audchf","audhkd","audjpy","audsgd","audusd","cadhkd","cadjpy","cadsgd",
    "chfhkd","chfjpy","euraud","eurcad","eurchf","eurgbp","eurhkd","eurjpy","eursgd","eurusd","gbpaud",
    "gbpcad","gbpchf","gbphkd","gbpjpy","gbpsgd","gbpusd","hkdjpy","sgdchf","sgdhkd","sgdjpy","usdcad",
    "usdchf","usdhkd","usdjpy","usdsgd","audnzd","cadchf","chfzar","eurczk","eurdkk","eurhuf","eurnok",
    "eurnzd","eurpln","eursek","eurtry","eurzar","gbpnzd","gbppln","gbpzar","nzdcad","nzdchf","nzdhkd",
    "nzdjpy","nzdsgd","nzdusd","tryjpy","usdcnh","usdczk","usddkk","usdhuf","usdmxn","usdnok","usdpln",
    "usdsar","usdsek","usdthb","usdtry","usdzar","zarjpy"}

    histdata_and_oanda_intersect_symbs = histdata_symbs & oanda_symbs

    pairs_data = get_available_range_data(histdata_and_oanda_intersect_symbs)
    for pair in pairs_data:
        start = pairs_data[pair]['start']
        end = pairs_data[pair]['end']

        import_pair_to_influx(pair, start, end)

if __name__ == '__main__':
    main()
```

---

## Setup

### TLDR for all platforms

---

#### Install histdatacom

```sh
pip install histdatacom
```

Polars is installed by default. To request optional API return formats:

```sh
pip install "histdatacom[pandas]"
pip install "histdatacom[arrow]"
```

InfluxDB import and notebook support are optional:

```sh
pip install "histdatacom[influx]"
pip install "histdatacom[jupyter]"
pip install "histdatacom[all]"
```

`histdatacom[temporal]` remains a compatibility alias for migration-era install
scripts, but the Temporal Python SDK is part of the base package dependency
set because sidecar execution is the default runtime.

to install latest development version

```sh
pip install git+https://github.com/dmidlo/histdata.com-tools.git
```

### Developer Setup

Use a project virtual environment for local development. Do not install
developer tooling into the user-local Python environment.

```sh
python -m venv venv
source venv/bin/activate
PYTHONNOUSERSITE=1 python -m pip install -e ".[dev]"
PYTHONNOUSERSITE=1 pre-commit install --install-hooks
```

On Windows, use the same project-local environment contract with PowerShell:

```powershell
py -m venv venv
.\venv\Scripts\Activate.ps1
$env:PYTHONNOUSERSITE = "1"
python -m pip install -e ".[dev]"
pre-commit install --install-hooks
```

The local Git hooks are designed to run from normal `git commit` and
`git push` commands after setup, even when the shell has not activated the
virtual environment. Hook wrappers resolve developer tools from
`HISTDATACOM_DEV_VENV`, the active `VIRTUAL_ENV`, `./venv`, or `./.venv` in
that order. Keep the project virtual environment in place after installing the
hooks; do not rely on user-local Python packages to satisfy `histdatacom`,
`coverage`, or other release gates.

The dependency surfaces are split by purpose:

- `.[test]` installs pytest, coverage, pandas, pyarrow, InfluxDB support, and
  test-only support around the base Temporal SDK dependency.
- `.[lint]` installs pre-commit and direct lint/type/doc hygiene tools.
- `.[release]` installs build and publish tooling.
- `.[dev]` is the aggregate local contributor environment with test, lint,
  release, and optional integration dependencies.

The `dev`, `lint`, `test`, and `release` extras pin direct developer tools
where reproducibility matters. Runtime dependencies keep compatibility lower
bounds rather than lock-file pins because `histdatacom` is a published PyPI
library. The active lint baseline is Black, Ruff, mypy, generic file checks,
Pyroma, ShellCheck, Commitizen, and the local CLI/coverage smoke hooks. The
previous flake8 plugin stack was intentionally replaced with Ruff so local
installs and hook behavior do not drift independently.

### Release Operator Path

Tagged releases and manual release runs build a metadata-only sdist/fallback
wheel plus bundled Temporal sidecar wheels for Linux x86_64, Linux arm64, macOS
Intel, macOS arm64, and Windows x86_64. The release workflow downloads Temporal
CLI `1.7.2` from the pinned upstream release, verifies SHA-256 checksums before
bundling, embeds local `temporal-cli-provenance.json` plus Temporal CLI
notice/license resources in bundled platform wheels, inspects every wheel with
the sidecar manifest checker, smoke-installs each platform wheel on a matching
GitHub-hosted runner, and attaches artifact provenance before upload or publish.

Use `release_target=build-only` for dry runs, `release_target=testpypi` for the
first publish rehearsal, and `release_target=pypi` only after setting
`testpypi_dry_run_confirmed=true`. The final `histdatacom-dist` artifact
contains only publishable sdists and wheels; JSON build and checksum reports are
uploaded separately as release reports.

If a bundled platform wheel fails after release, prefer yanking the affected
file on PyPI and cutting a replacement release. The sdist and universal fallback
wheel intentionally remain metadata-only rollback artifacts: users can still
install `histdatacom` and start the sidecar with
`histdatacom-sidecar start --executable /path/to/temporal` until a corrected
platform wheel is available.

### Coverage Policy

Coverage is enforced as a conservative total-project ratchet. The initial
threshold is set in `.coveragerc` from the current baseline so CI catches real
coverage regressions without blocking modernization work on unrelated low-legacy
modules. Future test work should raise `fail_under` when the baseline improves;
do not lower it unless a PR explains the production risk and links the follow-up
issue.

CI runs pytest through `pytest-cov`, enforces the `.coveragerc` threshold, and
uploads `coverage.xml` plus the `htmlcov/` report for every Python and OS matrix
leg. The first-pass gate is total-only. Per-package or domain thresholds belong
with the broader testing work tracked in issues #9 and #68.

The live Temporal sidecar smoke is not collected by default pytest because it
requires a real Temporal executable and starts local worker processes. Bundled
platform-wheel release smoke uses
`scripts/smoke_sidecar_install.py --hermetic-sidecar-smoke`, which submits a
local-only dataset-planning workflow with an explicit worker config and does
not contact HistData.com. Bundled platform-wheel release smoke also runs
`scripts/smoke_sidecar_install.py --default-routing-sidecar-smoke`, which
starts the sidecar with non-default worker routing and submits without an
explicit worker config so the installed package must resolve the running
frontend, namespace, and queues from persisted sidecar state. Run
`scripts/smoke_sidecar_install.py --quality-sidecar-smoke` to exercise the
installed `histdatacom --quality` console command against clean and dirty
local M1 fixtures through the packaged `DataQualityWorkflow` without contacting
HistData.com or InfluxDB. Run
`scripts/smoke_sidecar_install.py --live-sidecar-smoke` separately when an
operator intentionally wants external HistData.com URL-validation coverage.
These commands fail on shutdown leaks: stop exceptions, missing stop status,
persistent `stopping` status, or known remaining sidecar PIDs.

---

#### Vanilla MacOS and Linux

##### Create a new project directory and change to it

```bash
mkdir myproject && cd myproject && pwd
```

##### Create a Python Virtual Environment and activate it

```bash
python -m venv venv && source venv/bin/activate
```

##### Confirm Python Path and Version

```bash
which python && python --version
```

##### Install the histdata.com-tools package from PyPi

```bash
pip install histdatacom
```

##### Run `histdatacom` to view help message and Options

```bash
histdatacom -h
```

---

#### Vanilla Windows Powershell

##### Launch a Powershell Terminal

- Run as Administrator (right-click on shortcut and click Run as Admin...)

##### Make sure python3.10 is in your system's executable path

```powershell
python --version
```

- should be already set if you clicked the checkbox when installing python 3.10
- If not, you can run the following.
  - you will need to relaunch powershell as admin.

```powershell
[Environment]::SetEnvironmentVariable("Path", "$env:Path;C:\Program Files\Python310")
```

##### Change the Execution Policy to Unrestricted

```powershell
Set-ExecutionPolicy Unrestricted -Force
```

##### Create a new directory and change to it

```powershell
New-Item -Path ".\" -Name "myproject" -ItemType "directory"; Set-Location .\myproject\
```

##### Create a Virtual Environment and activate it

```powershell
python -m venv venv; .\venv\Scripts\Activate.ps1
```

##### Confirm Path and Version

```powershell
Get-Command python | select Source; python --version
```

##### Install histdata.com-tools package from PyPi

```powershell
pip install histdatacom
```

##### Run `histdatacom` to view help message

```powershell
histdatacom -h
```

---

#### Anaconda Setup

---

##### Anaconda MacOS and Linux

###### Create a Project Directory and Change to it

```shell
mkdir myproject && cd myproject && pwd
```

###### Create a `Python 3.10` Anaconda environment with `conda` and activate it

```shell
conda create -n py310 python=3.10 && conda activate py310
```

###### Check Python Path and Version

```shell
which python && python --version
```

###### Install histdatacom package from PyPi

```shell
pip install histdatacom
```

###### Run histdatacom package to view help message

```shell
histdatacom -h
```

---


##### Anaconda Windows using the Anaconda Prompt

###### Create a Directory and Change to it

```shell
mkdir myproject && cd myproject && echo %cd%
```

###### Create a `Python 3.10` Anaconda environment with `conda` and activate it

```shell
conda create -n py310 python=3.10 && conda activate py310
```

###### Check Python Path and Version

```shell
where python && python --version
```

###### Install histdatacom package from PyPi

```shell
pip install histdatacom
```

###### Run histdatacom package to view help message

```shell
histdatacom -h
```

---

## Roadmap

- [~~Add Support for Anaconda~~](https://github.com/dmidlo/histdata.com-tools/issues/28)
- [Implement MyPy static typing checking](https://github.com/dmidlo/histdata.com-tools/issues/16)
- [Implement UnitTesting with PyTest](https://github.com/dmidlo/histdata.com-tools/issues/9)
- [Create Binary Distributions](https://github.com/dmidlo/histdata.com-tools/issues/10)
  - See about packaging for different operating systems
    - deb/rpm packaging
    - NuGet/Chocolatey
    - MacPorts/Homebrew
- [docker image](https://github.com/dmidlo/histdata.com-tools/issues/11)
- [Create Down-sampling to Standard Candlestick Timeframes](https://github.com/dmidlo/histdata.com-tools/issues/18)
- [Fix terminate on ctrl-c multiprocessing KeyboardInterupt](https://github.com/dmidlo/histdata.com-tools/issues/15)
- [Look at replacing beautifulsoup with html parser](https://github.com/dmidlo/histdata.com-tools/issues/19)
- [Refactor to make use of globals more readable](https://github.com/dmidlo/histdata.com-tools/issues/14)
- [add -v -vv and -vvv flags](https://github.com/dmidlo/histdata.com-tools/issues/13)
- [Change Record statuses to Enum](https://github.com/dmidlo/histdata.com-tools/issues/20)
- [Add -S —set-status flag](https://github.com/dmidlo/histdata.com-tools/issues/21)
- [Create a central place for exceptions](https://github.com/dmidlo/histdata.com-tools/issues/22)
- Add the ability to import an order book to influxdb
- Add a --reset-cache flag to reset all or specified year-month range
