# histdata.com-tools

A command-line utility and Python ETL package for HistData.com currency exchange
rate archives. The local Temporal orchestration runtime is the default execution
engine for durable planning, downloads, extraction, cache builds, imports, job
telemetry, and live Rich progress, while normal PyPI artifacts stay lean by
provisioning the pinned Temporal executable through a verified first-run cache.

Data-quality checks cover ZIP/file inventory, CSV/XLSX ingestion, timestamp
continuity, OHLC bars, tick and spread behavior, symbol/domain calendars,
modeling readiness, and orchestration provenance with JSON reports and
CI-friendly exit policies.

InfluxDB imports, Jupyter tooling, and optional pandas/Arrow return formats are
available through extras.

Works on macOS, Linux, and Windows.
**Requires Python 3.10+**

[![Downloads](https://pepy.tech/badge/histdatacom)](https://pepy.tech/project/histdatacom) ![PyPI - License](https://img.shields.io/pypi/l/histdatacom) ![PyPI](https://img.shields.io/pypi/v/histdatacom) ![PyPI - Status](https://img.shields.io/pypi/status/histdatacom)

---

- [histdata.com-tools](#histdatacom-tools)
- [Disclaimer](#disclaimer)
- [Usage](#usage)
  - [Show the Help and Options](#show-the-help-and-options)
  - [Basic Use](#basic-use)
  - [Configuration Files](#configuration-files)
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
  - [Data Analytics](#data-analytics)
    - [Feed-Regime Detection](#feed-regime-detection)
  - [Orchestration Runtime](#orchestration-runtime)
    - [Runtime Model and Install Surface](#runtime-model-and-install-surface)
    - [Binary Provisioning and PyPI Packaging](#binary-provisioning-and-pypi-packaging)
    - [Public Orchestration API Boundary](#public-orchestration-api-boundary)
    - [Maintainer Runtime Diagnostics](#maintainer-runtime-diagnostics)
    - [Job Telemetry and Automation](#job-telemetry-and-automation)
    - [Runtime User and Maintainer Docs](#runtime-user-and-maintainer-docs)
  - [API - Other Scripts, Modules, & Jupyter Support](#api-other-scripts-modules-jupyter-support)
    - [Script and Application Automation](#script-and-application-automation)
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
usage: histdatacom [-h] [-A] [-U] [--by BY] [--version] [-V] [-D] [-X] [-C]
                   [--config PATH] [-p PAIR [PAIR ...]]
                   [--pair-groups GROUP [GROUP ...]] [-f FORMAT [FORMAT ...]]
                   [-t TIMEFRAME [TIMEFRAME ...]] [-s START_YEARMONTH]
                   [-e END_YEARMONTH] [-I] [-d] [-b BATCH_SIZE]
                   [-c CPU_UTILIZATION] [--data-directory DATA_DIRECTORY] [-v]
                   [--orchestration-start] [--no-orchestration-start]
                   [--submit-only] [--keep-runtime] [--no-keep-runtime]
                   [--quality] [--repo-quality] [--quality-preflight]
                   [--repo-quality-columns] [--quality-target PATH [PATH ...]]
                   [--quality-checks GROUP [GROUP ...]]
                   [--quality-report PATH] [--quality-preflight-report PATH]
                   [--quality-preflight-evidence PATH]
                   [--quality-preflight-evidence-max-age-seconds SECONDS]
                   [--quality-preflight-evidence-stale-ok]
                   [--quality-preflight-sample-size COUNT]
                   [--quality-profile PATH] [--quality-fail-on SEVERITY]
                   [--quality-max-errors COUNT] [--quality-max-warnings COUNT]

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
  -C, --build-cache, --cache-only, --build_cache
                        build canonical Polars .data caches and remove
                        transient ZIP/CSV sources after each cache is ready

Config:
  --config PATH         read recurrent-run defaults from a YAML file; explicit
                        CLI flags override configured values
  -p, --pairs PAIR [PAIR ...]
                        space separated currency pairs. e.g. -p eurusd usdjpy
                        ...
  --pair-groups, --instrument-groups, --symbol-groups GROUP [GROUP ...]
                        named instrument groups to union with --pairs. Common
                        groups: majors, minors, crosses, exotics, major-
                        triangles, metals, commodities, indices
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
                        set an end year and month for data. e.g. -e 2020-00 or
                        -e 2022-04

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
  -v, --verbose         increase logging verbosity; repeat as -vv for debug
                        and -vvv for trace

Orchestration:
  --orchestration-start
                        start the local orchestration runtime only when no
                        healthy runtime is running
  --no-orchestration-start
                        submit only when a healthy orchestration runtime is
                        already running
  --submit-only         submit the orchestration job without waiting for its
                        result
  --keep-runtime        leave a runtime started by this command running after
                        the job completes
  --no-keep-runtime     stop a runtime started by this command after waited
                        jobs complete

Data quality:
  --quality             run offline data-quality assessment against local
                        datasets without contacting HistData.com
  --repo-quality        run offline data-quality assessment and write bounded
                        quality summary metadata back to the local .repo file
  --quality-preflight   benchmark a deterministic sample of existing .data
                        caches before running a cache-scale quality battery
  --quality-target, --quality-path PATH [PATH ...]
                        local file or directory to assess; supports
                        directories, HistData ZIP archives, CSV files, XLSX
                        payloads, and .data cache files
  --quality-checks GROUP [GROUP ...]
                        quality check groups to run; defaults to all.
                        Supported: all, inventory, ingestion, time, bars,
                        ticks, domain, modeling, provenance
  --quality-report PATH
                        write the full machine-readable JSON quality report to
                        PATH
  --quality-preflight-report PATH
                        write the publish-safe JSON quality preflight report
                        to PATH
  --quality-preflight-evidence PATH
                        use a saved quality preflight JSON report as evidence
                        before a large cache-backed --quality run
  --quality-preflight-evidence-max-age-seconds SECONDS
                        maximum age for saved quality preflight evidence;
                        defaults to 86400
  --quality-preflight-evidence-stale-ok
                        allow matching quality preflight evidence even when
                        its generated_at_utc timestamp is stale
  --quality-preflight-sample-size COUNT
                        number of cache-size quantile targets to benchmark;
                        defaults to 4
  --quality-profile PATH
                        read a JSON quality profile with rule thresholds,
                        severities, and modeling assumptions
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
  --repo-quality-columns
                        include stored data-quality status columns in -A/-U
                        repository table output

Commands:
  analytics   Run offline data analytics operations
  cleanup     Remove transient source artifacts
  jobs        Inspect and control orchestrated work
  quality     Inspect local data quality evidence
  runtime     Inspect and manage the orchestration runtime

Run `histdatacom analytics --help` for analytics commands.
Run `histdatacom cleanup --help` for cleanup commands.
Run `histdatacom jobs --help` for job telemetry commands.
Run `histdatacom quality --help` for quality commands.
```

Maintainers: this help excerpt is generated from `ArgParser.format_help()` at a
fixed width. After changing public CLI flags, run:

```sh
python scripts/sync_readme_cli_help.py
python -m pytest tests/unit/test_readme_help_sync.py
```

For repeatable issue closure evidence, run the local readiness helper from
`dev` after implementation work is complete:

```sh
python scripts/closure_readiness.py \
  --issue 274 \
  --commit-readiness \
  --commit-message "feat(scope): describe the change" \
  --commit-path path/to/changed-file.py
python scripts/closure_readiness.py --issue 274 --push-readiness
python scripts/closure_readiness.py --issue 274 --issue-audit
python scripts/closure_readiness.py --issue 274 --workflow
python scripts/closure_readiness.py \
  --summarize-report .histdatacom/closure-readiness/closure-274.json
python scripts/closure_readiness.py --issue 274 --workflow --close-issue
python scripts/closure_readiness.py \
  --issue 274 \
  --execute-workflow \
  --pre-mutation-gates \
  --commit-message "feat(scope): describe the change" \
  --commit-path path/to/changed-file.py \
  --acceptance-test '*=tests/unit/test_changed_behavior.py'
```

The helper checks branch/upstream alignment, dirty and untracked files, linked
GitHub issue state, lingering pytest/pre-commit/Temporal/histdatacom tool
processes before and after gates, transient ZIP/CSV/XLS/XLSX source artifacts
under `data/`, README help synchronization, `git diff --check`, main help smoke
output, pytest, and pre-commit. Reports are publish-safe JSON and Markdown with
a GitHub-ready close comment block. `--commit-readiness` validates the current
change scope and candidate Commitizen message without running `git add`,
`git commit`, or `git push`; use repeated `--commit-path` flags to declare the
intended file scope and catch unrelated dirty files. `--push-readiness` reports
whether a clean `dev` branch with local commits ahead of `origin/dev` is ready
to push. Default issue-scoped reports are local outputs under
`.histdatacom/closure-readiness/`; the helper verifies those paths are
gitignored before writing them and blocks closure if that safety check drifts.
Explicit report paths still work, but the report marks whether they may dirty
the current worktree. `--workflow` performs the cheap precheck first, stops
before expensive gates when local state is blocked, writes safe default reports,
and enforces the `dev` branch workflow. Use `--close-issue` only when ready to
close; it remains an explicit opt-in action and reads back the final issue state
after closing. `--execute-workflow` is the explicit mutating mode: it validates
the declared paths and Commitizen message, runs targeted `git add`, commits,
checks push readiness, pushes to the expected upstream, runs closure gates,
closes the issue, and writes bounded execution evidence plus full ignored logs.
Add `--pre-mutation-gates` to run the same closure gate battery before the first
`git add`; the workflow blocks staging, commit, and push if those gates fail or
rewrite files, and records the result separately from the post-push closure
gates. Issue closure reports parse issue checklists or `Acceptance criteria`
bullets into acceptance coverage evidence. Attach criterion-specific or shared
evidence with `--acceptance-status`, `--acceptance-file`, `--acceptance-test`,
`--acceptance-report`, or `--acceptance-note` using `KEY=VALUE`; `KEY` can be
`ac-001`, a criterion number, slug, hash, or `*` for all criteria. Automatic
issue close refuses missing required criteria unless `--acceptance-missing-ok`
is supplied, and the override reason is recorded with
`--acceptance-override-reason`. Default behavior remains report-only unless this
flag is present. Add `--release-preflight` only during publishing work; normal
issue closure records the TestPyPI local simple-registry preflight as
not-applicable.

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

#### include the `-C` flag to build internal Polars caches and discard ZIP/CSV sources

```sh
histdatacom -C -p eurusd -f ascii -t tick-data-quotes -s 2024-01 -e 2024-03
```

Cache-only mode validates and downloads the selected HistData archives, builds
canonical `.data` cache files, and removes transient ZIP/CSV sources after each
cache is ready. It is intentionally limited to cache-capable ASCII `M1` and
tick quote datasets, and it does not merge the caches into memory.

#### clean up transient source artifacts without removing internal caches

```sh
histdatacom cleanup sources --data-directory data
histdatacom cleanup sources --data-directory data --apply
histdatacom cleanup status --data-directory data --pair-groups majors -f ascii -t T
```

Cleanup mode is a dry run unless `--apply` is present. It removes downloaded
ZIP, CSV, XLS, and XLSX source artifacts while preserving internal `.data`
caches. Use `cleanup status` to inspect cache counts, pending source cleanup,
disk pressure, runtime state, and offline workflow snapshots for a symbol or
instrument group without shelling out to `find`, `df`, `ps`, or raw Temporal
commands. Add `--json` for the stable scriptable payload.

---

### Configuration Files

Use `--config PATH` to keep recurrent command options in a YAML file. The file
may use a `histdatacom:` root section or a bare mapping. Keys match the public
CLI option names without leading dashes. Explicit CLI flags are parsed after the
file and override configured scalar and list values.

```yaml
histdatacom:
  download_data_archives: true
  extract_csvs: true
  pairs:
    - eurusd
    - gbpusd
  # Or use named groups instead of listing every symbol:
  # instrument_groups: [majors, metals]
  formats:
    - ascii
  timeframes:
    - 1-minute-bar-quotes
  start_yearmonth: 2022-01
  end_yearmonth: 2022-03
  data_directory: /data/histdata
  cpu_utilization: medium
  orchestration_start: true
  orchestration_wait_result: false
  verbosity: 1
```

Run it with:

```sh
histdatacom --config recurrent.yaml
```

Config files can also express offline data-quality runs:

```yaml
histdatacom:
  quality: true
  data_directory: data/
  quality_checks:
    - inventory
    - ingestion
  quality_report: reports/quality.json
  quality_fail_on: error
```

The routed commands use scoped sections in the same file:

```yaml
histdatacom:
  analytics:
    command: feed-regimes
    target: data/ASCII/T/eurusd
    bucket: month
    report: reports/eurusd-feed-regimes.json
    json: true
  jobs:
    command: list
    offline: true
    json: true
    limit: 20
  cleanup:
    command: status
    data_directory: data/
    pair_groups:
      - majors
    json: true
  runtime:
    command: status
    json: true
```

Run scoped commands with the same flag:

```sh
histdatacom --config recurrent.yaml analytics
histdatacom cleanup --config recurrent.yaml
histdatacom jobs --config recurrent.yaml
histdatacom runtime --config recurrent.yaml
```

Pair-list presets and shared instrument lists are tracked separately from this
full command snapshot surface.

For recurrent low-disk cache-building jobs, set `build_cache: true` instead of
`download_data_archives` / `extract_csvs`. The option accepts the same dataset
selectors as the CLI and leaves only the internal `.data` cache artifacts for
supported ASCII `M1` and tick quote datasets.

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

Use named instrument groups for common baskets:

```txt
histdatacom --pair-groups majors exotics -f ascii -t tick-data-quotes -s 2022
```

Use the major triangle basket when preparing data for cross-instrument quality
analytics:

```txt
histdatacom --pair-groups major-triangles -f ascii -t tick-data-quotes -s 2022
```

`major-triangles` covers the USD, EUR, JPY, GBP, CAD, CHF, AUD, and NZD
instruments needed by the data-quality triangular comparison rule: 28
downloadable instruments supporting 56 oriented relationships such as
`AUDCHF / CADCHF ~= AUDCAD`. It excludes exotics, metals, commodities, and
indices.

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
orchestration `DataQualityWorkflow` that runs CPU/file activities. Successful
default runs use a scratch report and delete it after validation; pass
`--quality-report PATH` when a durable detailed JSON report is needed. Use it
after downloading or extracting data, before trusting local ZIP, CSV, or cache
artifacts for import, modeling, or backtesting.

```sh
histdatacom --quality --quality-target data/ --quality-report reports/quality.json
```

The command prints a human summary, source-artifact cleanliness, and scratch
report cleanup status. If no `--quality-target` is passed, quality mode uses
the configured data directory. Targets can be plain HistData CSV files,
extracted Excel `.xlsx` payloads, HistData ZIP archives, directories containing
those files, or the canonical `.data` cache file.

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

#### Cache-Scale Quality Preflight

Use `--quality-preflight` before a large cache-backed quality battery. It scans
existing canonical `.data` caches, selects a deterministic cache-size quantile
sample, runs the selected quality checks against that bounded sample, measures
rows/sec and bytes/sec, and compares the extrapolated runtime with the Temporal
`data_quality` activity budget.

```sh
histdatacom --quality-preflight \
  --quality-target data \
  --pair-groups major-triangles \
  -f ascii -t tick-data-quotes \
  --quality-checks ticks \
  --quality-preflight-report reports/major-triangles-tick-preflight.json
```

The console output is human-readable. The optional
`--quality-preflight-report PATH` file is publish-safe JSON with target counts,
cache bytes, sampled paths, row counts, throughput, ETA range, sample quality
summary, generated timestamp, package version, preflight policy inputs,
no-target diagnostics, and a decision section that says whether the full battery
is safe, warned, failed, or has no matching targets. Safe and warned decisions
include the next `histdatacom --quality ...` command for the same target scope.
Use `--quality-preflight-sample-size COUNT` to tune the bounded sample.

When launching a large cache-backed `--quality` run, pass the saved report with
`--quality-preflight-evidence PATH`. If no matching evidence is available, the
CLI prints a warning and suggested preflight command before continuing without
prompting. Evidence must match the target root, filters, current package version,
Temporal `data_quality` budget, cache target count, and cache byte inventory.
Evidence also has to be fresh by default; use
`--quality-preflight-evidence-max-age-seconds SECONDS` to change the 86400-second
window, or pass `--quality-preflight-evidence-stale-ok` to explicitly bypass the
age check while still enforcing scope, version, policy, and cache-inventory
matches.

Inspect saved evidence directly when you need a non-interactive answer before a
large run:

```sh
histdatacom quality evidence \
  --evidence reports/major-triangles-tick-preflight.json \
  --target data \
  --pair-groups major-triangles \
  -f ascii -t tick-data-quotes \
  --quality-checks ticks
```

The command exits `0` only when the evidence is accepted for the current cache
scope. Use `--json` for automation. Rejections distinguish stale evidence,
package-version drift, Temporal policy drift, target/filter drift, and cache
inventory count, byte, or fingerprint changes. Add
`--quality-preflight-evidence-stale-ok` only when you intentionally want to
bypass the age window while still enforcing the other checks.

#### Full-Dataset Quality Campaigns

Full HistData.com quality campaigns should run in bounded
symbol/format/timeframe slices from an environment with a verified Temporal
executable: an explicit override, an offline/private bundled artifact, a
verified runtime cache entry, or a resolver-provisioned first-run download. Do
not run the full repository surface as one accumulating local scrape.

For each slice, run download/extract first, then run `--repo-quality` so `.repo`
keeps bounded findings and the detailed JSON report path. Normal campaign
execution keeps the generated cache artifacts. For low-disk cache-building
campaigns, use `--build-cache`; it builds canonical `.data` files and removes
the transient ZIP/CSV sources as each cache completes. Run cleanup only after
`--repo-quality` succeeds, and never remove `.repo` or published quality
reports.

For interrupted cache builds or older local source artifacts, use
`histdatacom cleanup sources` to inspect removable ZIP, CSV, XLS, and XLSX files,
then repeat with `--apply` when the report is expected. The cleanup command
preserves internal `.data` cache files. Use `histdatacom cleanup status` first
when an operator needs the cache count, pending cleanup count, disk pressure,
runtime state, and durable workflow status in one report.

```sh
histdatacom -D -X -p eurusd -f ascii -t M1 --data-directory /Volumes/histdata/data
histdatacom --repo-quality \
  --quality-target /Volumes/histdata/data/ASCII/M1/eurusd \
  --quality-report /Volumes/histdata/reports/eurusd-ascii-m1-quality.json \
  --data-directory /Volumes/histdata/data
histdatacom --build-cache -p eurusd -f ascii -t M1 --data-directory /Volumes/histdata/data
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
| `provenance` | optional orchestration manifest/status lineage checks for artifact paths, sizes, checksums, cache metadata, stale caches, and orphan files |

`provenance` checks are only applied when a local orchestration
`.histdatacom/manifest-status.sqlite3` store is available. Explicit
`--quality-checks provenance` runs without a store return a clean info finding
that records the missing store; ordinary file-only quality runs are not failed by
the absence of orchestration provenance data.

#### Quality Profiles

Use `--quality-profile PATH` to load a versioned JSON profile that tunes rule
thresholds, severities, precision profiles, gap/session tolerance, tick
microstructure profiles, cross-instrument tolerance, and modeling-readiness
assumptions. The report metadata includes the active `quality_profile` source,
name, configured rule IDs, and configured modeling-assumption keys.

Strict CI profiles can promote warnings to errors or tighten thresholds:

```json
{
  "schema_version": "histdatacom.quality-profile.v1",
  "name": "strict-ci",
  "rules": {
    "ingestion.ascii.row_count": {
      "min_row_count": 100,
      "tiny_severity": "error"
    },
    "time.ascii.gaps": {
      "tolerance": {
        "suspicious_gap_ms": 300000
      },
      "warning_severity": "error"
    }
  }
}
```

Exploratory research profiles can loosen market-anomaly thresholds and record
modeling assumptions without changing global defaults:

```json
{
  "schema_version": "histdatacom.quality-profile.v1",
  "name": "exploratory-research",
  "rules": {
    "bars.ascii.m1_outliers": {
      "thresholds_by_asset_class": {
        "fx": {
          "max_open_jump_ratio": 0.01
        }
      }
    },
    "ticks.ascii.microstructure": {
      "session_name": "rollover",
      "thresholds_by_symbol_session": {
        "EURUSD:rollover": {
          "one_sided_run_length": 4
        }
      }
    }
  },
  "modeling_assumptions": {
    "ask_side_execution_model": true,
    "current_bar_action_timing": "after_bar_close",
    "spread_cost_model": "fixed_session_profile",
    "target_horizon_minutes": 5
  }
}
```

```sh
histdatacom --quality \
  --quality-target data/ \
  --quality-profile profiles/strict-ci.json \
  --quality-fail-on warning \
  --quality-report reports/quality.json
```

Format support is explicit in every discovered target's `quality_support`
metadata. The current quality boundary is:

| Format | Timeframes | Quality support |
| --- | --- | --- |
| `ascii` | `M1`, `T` | Deep parser-level checks for ZIP, CSV, and canonical `.data` cache artifacts |
| `metatrader` | `M1` | Inventory-only: filename, ZIP integrity, and expected member checks |
| `ninjatrader` | `M1`, `T_LAST`, `T_BID`, `T_ASK` | Inventory-only: filename, ZIP integrity, and expected member checks |
| `metastock` | `M1` | Inventory-only: filename, ZIP integrity, and expected member checks |
| `excel` | `M1` | Inventory-only for ZIPs and extracted `.xlsx` workbook payloads |

Inventory-only targets emit a warning with code
`HISTDATA_FORMAT_INVENTORY_ONLY`; they are intentionally not reported as deeply
clean. Recognized formats used with unsupported timeframes emit
`HISTDATA_FORMAT_UNSUPPORTED`.

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

The report payload is a public automation contract. Compatibility expectations
and the golden-fixture update workflow are documented in
`docs/data-quality/report-compatibility.md`.

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

Reviewed source-data defects are documented under
`docs/data-quality/known-data-defects.md`. These records explain known vendor
anomalies for future batch interpretation, but they do not downgrade quality
severities or silence repo-quality failures.

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

### Data Analytics

Data analytics operations describe market-data behavior for downstream feature
engineering, dashboards, and modeling decisions. They are separate from
`histdatacom --quality`: analytics reports do not produce clean/warning/failed
statuses and do not downgrade repository quality metadata.

#### Feed-Regime Detection

`histdatacom analytics feed-regimes` profiles local ASCII tick artifacts by
month or year, then segments long histories into feed-behavior eras such as
sparse, transitional, and dense periods. The report includes tick density,
inter-arrival intervals, quote update cadence, zero-change runs, spread
statistics, quiet-gap counts, regime boundaries, and summary metadata.

```sh
histdatacom analytics feed-regimes \
  --target data/ASCII/T/eurusd \
  --bucket month \
  --report reports/eurusd-feed-regimes.json
```

Use `--json` to print the full machine-readable payload to stdout:

```sh
histdatacom analytics feed-regimes --target data/ --json
```

Use these outputs to choose modeling windows, session filters, feature
normalization strategies, or dashboard annotations. Treat surprising regimes as
research signals; run `histdatacom --quality` separately when you need
readability, timestamp consistency, ZIP integrity, or pass/fail validation.

---

### Orchestration Runtime

The production default is the local Temporal orchestration runtime for CLI and
API runs. Default requests submit a `RunRequest` to the runtime and start the
local service and worker fleet when no healthy runtime is running.

The foreground rollback runtime has been removed after its release-window
deprecation period. `--foreground` is no longer a valid CLI flag, and API code
that sets `options.use_orchestration = False` raises a clear `ValueError`. If the
runtime cannot be started or contacted, CLI calls exit nonzero with a clear
error and API calls raise `OrchestrationUnavailableError`; the runtime never
silently falls back to a local foreground execution path.

#### Runtime Model and Install Surface

The base install includes the Temporal Python SDK because orchestration is the
default runtime:

```sh
pip install histdatacom
```

`histdatacom[temporal]` is available for environments that want to make the
runtime dependency explicit, but it does not change the default runtime
contract: base installs include the Temporal SDK needed by clients and workers.

The runtime stores Temporal process state, SQLite history, logs, and runtime
manifests under a per-user, per-workspace runtime directory. Downloaded ZIP
files, extracted CSV/XLSX files, cache IPC files, and merged API artifacts stay
under the existing HistData data-directory policy.

Record status metadata is manifest-only for new writes. Normal CLI/API paths
update `.histdatacom/manifest-status.sqlite3` under the relevant data or
runtime status root and no longer create new hidden `.meta` files beside
records. Existing `.meta` files remain readable as migration inputs; successful
imports write the manifest row and remove the legacy file, while missing or
corrupt legacy files are reported without blocking manifest-backed operation.

Source distributions and universal wheels include orchestration metadata, CLI
entry points, runtime defaults, and third-party notices. The accepted V1.0
packaging design keeps normal PyPI and TestPyPI artifacts metadata-only and
provisions the pinned Temporal executable through a verified runtime cache on
first use. See [Temporal Binary Provisioning](docs/temporal-binary-provisioning.md)
for the production design. Release preflight hardening for that non-bundled path
is tracked by #251.

Metadata-only artifacts resolve the Temporal executable from an explicit
operator override, an offline/private bundle, a verified per-user cache entry, or
a pinned first-run download. Bundled executable wheels remain an offline/private
distribution path, not the normal PyPI release path. The executable and the
Python Temporal SDK are separate concerns: base installs provide the SDK, while
the runtime resolver owns executable availability.

Default orchestration submissions are built from resolved runtime context and
`RunRequest` payloads exposed by `histdatacom.orchestration`. New automation
work should use the orchestration facade instead of importing the private
runtime implementation package directly. Legacy helper surfaces now accept
explicit argument dictionaries rather than ambient parser state; parser globals
are not part of runtime selection.

#### Binary Provisioning and PyPI Packaging

The binary provisioning design is intentionally modeled like the HistData
repository file: a small package-owned index pins the allowed remote Temporal
artifacts by version, platform, URL, checksum, size, and provenance metadata.
Normal PyPI artifacts stay below upload limits because they ship the index and
not the binary.

The runtime resolver prefers explicit operator overrides, then verified
private/offline bundles, then a verified per-user cache, and finally a first-run
download when network provisioning is allowed. `HISTDATACOM_TEMPORAL_EXECUTABLE`
sets a process-wide explicit executable, `HISTDATACOM_TEMPORAL_CACHE_DIR` sets an
alternate cache root, and `HISTDATACOM_TEMPORAL_OFFLINE=1` disables first-run
network provisioning. Offline environments fail with instructions to pre-seed
the cache, install an offline/private bundle, or pass an explicit executable.

#### Public Orchestration API Boundary

New GUI and automation integrations should submit work through the public
orchestration surface:

- `histdatacom.Options` passed to `histdatacom.main(options)` or
  `histdatacom(options)`
- `histdatacom.orchestration.contracts.RunRequest`
- `histdatacom jobs ...` for job telemetry and control
- `histdatacom.orchestration.client` job-control helpers for submit, inspect,
  list, cancel, resume, progress, and artifact polling
- `histdatacom.orchestration.telemetry` helpers for job status, progress, logs,
  results, and artifacts

Do not build new validate/download/extract/cache/import automation by importing
`Repo`, `Scraper`, `Api.validate_caches`, `Api.merge_caches`, or
`Influx.import_data` directly. Those direct side-effect methods remain as
compatibility helpers for existing callers and emit
`LegacyHelperSideEffectWarning` when used. Temporal activities continue to call
the lower-level `histdatacom.activity_stages` functions and related adapter
objects directly; those stage helpers are the supported worker boundary, not
the GUI or automation boundary.

#### Maintainer Runtime Diagnostics

The normal user path does not require process lifecycle commands. Maintainers
can inspect and manage the local runtime through the lower-level lifecycle CLI:

```sh
histdatacom runtime doctor --json
histdatacom runtime status --json
histdatacom runtime start
histdatacom runtime start --executable /path/to/temporal
histdatacom runtime stop
```

`status` and `doctor` report component health for the server and each worker
lane: `orchestration`, `network`, `cpu-file`, and `influx`.

Use `--workspace` or `HISTDATACOM_RUNTIME_WORKSPACE` for cron, service
managers, GUI launchers, and other contexts where the current working directory
may not be stable.

#### Job Telemetry and Automation

Submit a job through the default orchestration runtime:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Interactive waited CLI runs render a live Rich progress view while the Temporal
job is running; piped output and API calls keep the machine-readable result
path.

Submit without waiting for completion:

```sh
histdatacom --submit-only -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

The JSON control surface supports job inspection and future GUI polling:

```sh
histdatacom jobs list --json
histdatacom jobs progress histdatacom-<request-id> --watch
histdatacom jobs progress histdatacom-<request-id> --json
histdatacom jobs artifacts histdatacom-<request-id> --json
histdatacom jobs cancel histdatacom-<request-id> --reason "operator stop"
```

Omit `--json` on `jobs progress` for the Rich terminal progress view; add
`--watch` to live-refresh it until the job reaches a terminal state.

- `histdatacom --version` stays local and does not require orchestration.
- `-A`, `-U`, `-V`, `-D`, `-X`, `-C`, and `-I` keep their existing option semantics before an orchestration request is submitted.
- `--foreground` has been removed and is rejected by the CLI.
- `--orchestration-start` starts the server and worker lane fleet only when no healthy runtime is running.
- `--no-orchestration-start` requires an already-running healthy runtime and fails
  clearly instead of starting one.
- `--submit-only` submits a job and returns job metadata instead of waiting for cache artifacts or workflow results.
- Waited orchestration `-A` / `-U` repository requests keep the output contract: API calls return the available-data dictionary, and CLI calls render the repository table.
- `--build-cache` / `options.build_cache` builds canonical `.data` cache files for cache-capable ASCII datasets, removes transient ZIP/CSV sources after each cache is ready, and does not merge caches into memory.
- API calls with `options.api_return_type` return the requested `polars`, `pandas`, or `arrow` object after a completed orchestration job by materializing cache artifacts on disk.
- If orchestration is unavailable, CLI calls exit nonzero with a clear error and API calls raise `OrchestrationUnavailableError`.
- `-v` emits high-level orchestration lifecycle logs; `-vv` adds worker,
  workflow, and activity detail; `-vvv` enables trace-level package logging and
  Temporal SDK/HTTP debug logging. Workflow and activity logs use Temporal's
  logger adapters so workflow replay does not duplicate normal workflow log
  lines. Log metadata is bounded to job/stage/status fields, and credential-like
  keys such as tokens, passwords, and secrets are redacted.

Orchestration-backed API calls use the same public `Options` object and runtime
defaults:

```python
options.orchestration_wait_result = True
options.api_return_type = "polars"
```

Set `options.orchestration_wait_result = False` to submit a job and receive
job metadata instead of a materialized API return object. Set
`options.orchestration_start = False` when a caller requires a pre-started
runtime. `options.use_orchestration = False` is not supported.

#### Runtime User and Maintainer Docs

See [Temporal Orchestration User Guide](docs/temporal-orchestration-operations.md)
for submit, observe, cancel, retry, resume, artifacts, and user troubleshooting
workflows. See
[Temporal Orchestration Runtime Runbook](docs/temporal-orchestration-runtime-runbook.md)
for maintainer lifecycle commands, runtime path layout, port policy, worker
lanes, SQLite persistence, maintenance, and low-level diagnostics. See
[Temporal Workflow Topology](docs/temporal-workflow-topology.md) for workflow,
activity, task queue, and testing boundaries. See
[Temporal Orchestration Performance Baseline](docs/temporal-orchestration-performance.md)
for lane sizing and benchmark policy.

---

### API - Other Scripts, Modules, & Jupyter Support

histdatacom exposes one Python API entry point for scripts, applications, and
notebooks:

```python
import histdatacom
from histdatacom.options import Options

options = Options()
result = histdatacom(options)
```

The same `Options` object supports two common API paths:

- submit CLI-shaped ETL work from a script or application, usually for
  validate/download/extract/import jobs that do not return a dataframe.
- request dataframe/table results for interactive work in Jupyter or for larger
  Python programs that need to consume the data directly.

API calls use the orchestration runtime by default. A missing runtime is started
when needed unless `options.orchestration_start = False` is set. The copyable
examples live under `samples/`; pytest executes those samples in hermetic mode
without contacting HistData.com or starting a Temporal runtime.

- `samples/api_quickstart.py`
- `samples/notebooks/api_quickstart.ipynb`

---

#### Script and Application Automation

##### First import the required modules

```python
import histdatacom
from histdatacom.options import Options
```

##### Create and Initialize a new options object to pass parameters to histdatacom

```python
options = Options()
```

##### Configure automation options

To submit the same ETL work a user would normally request from the CLI, set one
of the boolean behavior flags: `options.validate_urls`,
`options.download_data_archives`, `options.extract_csvs`,
`options.build_cache`, or
`options.import_to_influxdb`.

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
# options.build_cache = True  # implies validate/download; leaves only .data caches
# options.import_to_influxdb = True  # implies validate, download, and extract
options.formats = {"ascii"}
options.timeframes = {"tick-data-quotes"}
options.pairs = {"eurusd"}
options.start_yearmonth = "2021-04"
options.end_yearmonth = "2021-05"
options.cpu_utilization = "medium"
```

- Automation requests submit through orchestration by default and start a
  missing runtime when needed. Set
  `options.orchestration_wait_result = False` when the caller only needs job
  metadata, set `options.orchestration_start = False` when a caller requires a
  pre-started runtime. `options.use_orchestration = False` is
  rejected because the foreground runtime has been removed.

- New automation should not call legacy helper classes directly for
  validate/download/extract/cache/import work. Direct side-effect helper
  methods warn because they bypass durable orchestration status, cancellation,
  retry/resume, and worker-lane routing.

- When an ETL behavior flag is included without `api_return_type`, the call
  submits work and does not return dataframe data.

Use the normal Python `__name__ == "__main__"` guard for executable scripts:

```python
if __name__ == "__main__":
    histdatacom(options)
```

---

#### Jupyter and External Scripts

For notebooks and data-consuming Python programs, set
`options.api_return_type`. The completed orchestration job materializes cache
artifacts and returns a dataframe or table.

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
options.timeframes = {"1-minute-bar-quotes"}  # can be tick-data-quotes or 1-minute-bar-quotes
options.pairs = {"eurusd"}
# Or choose named baskets with options.pair_groups = {"majors", "major-triangles"}
options.start_yearmonth = "2021-04"
options.end_yearmonth = "2021-05"
options.cpu_utilization = "medium"
```

- This example uses just one pair/instrument/symbol `eurusd` and just one timeframe `1-minute-bar-quotes`. When the api is called with this 'one-one` specificity, the api will directly return the requested data.
- Regardless of the specified start_yearmonth and end_yearmonth, the resultant data will be sorted and merged into a single dataset.

##### Pass the options to histdatacom and assign the return to a variable

```python
data = histdatacom(options)  # (Jupyter)

print(type(data))
print(data.shape)
```

```text
<class 'polars.dataframe.frame.DataFrame'>
(rows depend on the requested period, 6)
```

- When specifying more than one pair/symbol/instrument or timeframe, the API
  returns a ***list of dictionaries*** with references to the timeframe, pair,
  records used to create the data, and the merged data itself.

```python
options.api_return_type = "pandas"
options.formats = {"ascii"}
options.timeframes = {"1-minute-bar-quotes"}
options.pairs = {"eurusd","usdcad"}
options.start_yearmonth = "2021-01"
options.end_yearmonth = "2021-02"
options.cpu_utilization = "medium"
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

The notebook/API path is covered by pytest and pre-commit through the hermetic
`samples/notebooks/api_quickstart.ipynb` execution test. The checked-in
`snippets.ipynb` file remains an exploratory example and is not executed by
default because it can request live HistData.com data.

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

`histdatacom[temporal]` remains available for explicit runtime installs, but
the Temporal Python SDK is part of the base package dependency set because
orchestration is the default runtime.

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

- `.[test]` installs pytest, coverage, pandas, pyarrow, InfluxDB support,
  notebook execution support, and test-only support around the base Temporal SDK
  dependency.
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

Tagged releases and manual release runs should build the normal metadata-only
sdist and universal wheel for PyPI/TestPyPI. The V1.0 provisioning design moves
Temporal executable availability into a verified first-run resolver backed by a
packaged artifact index and a per-user cache. Release preflight should prove the
normal wheel is under the upload-size gate and that a clean install can provision
or locate the pinned runtime through the resolver.

The existing bundled platform-wheel tooling remains useful for offline/private
artifacts and emergency operator recovery, but those artifacts are not the
default PyPI path. They should be uploaded to the normal PyPI project only after
the project-specific size limit is confirmed and the release operator explicitly
opts in.

Use `release_target=build-only` for dry runs, `release_target=testpypi` for the
first publish rehearsal, and `release_target=pypi` only after setting
`testpypi_dry_run_confirmed=true`. The final `histdatacom-dist` artifact
contains only publishable sdists and wheels; JSON build and checksum reports are
uploaded separately as release reports.

If runtime provisioning fails after release, prefer yanking the affected package
only when the Python artifact itself is wrong. Bad or unreachable Temporal
runtime artifacts should be handled by fixing the artifact index in a patch
release, while explicit executable overrides and pre-seeded caches remain
operator recovery paths.

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

The live Temporal runtime smoke is not collected by default pytest because it
requires a real Temporal executable and starts local worker processes. Bundled
platform-wheel release smoke uses
`scripts/smoke_runtime_install.py --hermetic-runtime-smoke`, which submits a
local-only dataset-planning workflow with an explicit worker config and does
not contact HistData.com. Bundled platform-wheel release smoke also runs
`scripts/smoke_runtime_install.py --default-routing-runtime-smoke`, which
starts the runtime with non-default worker routing and submits without an
explicit worker config so the installed package must resolve the running
frontend, namespace, and queues from persisted runtime state. Run
`scripts/smoke_runtime_install.py --quality-runtime-smoke` to exercise the
installed `histdatacom --quality` console command against clean and dirty
local M1 fixtures through the packaged `DataQualityWorkflow` without contacting
HistData.com or InfluxDB. Run
`scripts/smoke_runtime_install.py --live-runtime-smoke` separately when an
operator intentionally wants external HistData.com URL-validation coverage.
These commands fail on shutdown leaks: stop exceptions, missing stop status,
persistent `stopping` status, or known remaining runtime PIDs.

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
