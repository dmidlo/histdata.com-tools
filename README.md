# histdata.com-tools

A command-line utility and Python ETL package for HistData.com currency exchange
rate archives. The local Temporal orchestration runtime is the default execution
engine for durable planning, downloads, extraction, cache builds, imports, job
telemetry, and live Rich progress, while normal PyPI artifacts stay lean by
provisioning the pinned Temporal executable through a verified first-run cache.

Data-quality checks cover ASCII tick ZIP/file inventory, CSV ingestion,
timestamp continuity, tick and spread behavior, symbol/domain calendars,
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
  - [Provider-neutral Dataset Catalog](#provider-neutral-dataset-catalog)
  - [CPU Utilization](#cpu-utilization)
  - [Import to InfluxDB](#import-to-influxdb)
    - [Docker-backed InfluxDB Smoke](#docker-backed-influxdb-smoke)
    - [influxdb.yaml](#influxdbyaml)
  - [Data Quality Assessments](#data-quality-assessments)
    - [Quality Targets and Check Groups](#quality-targets-and-check-groups)
    - [Clean and Failing Examples](#clean-and-failing-examples)
    - [Warning, Error, and Exit Policy](#warning-error-and-exit-policy)
  - [Data Analytics](#data-analytics)
    - [Point-in-Time Market Context](#point-in-time-market-context)
    - [Feed-Regime Detection](#feed-regime-detection)
    - [Historical Feed-Observation Operators](#historical-feed-observation-operators)
    - [Reverse-Degradation Benchmark](#reverse-degradation-benchmark)
    - [Qualified Proposal-Engine Bank](#qualified-proposal-engine-bank)
    - [Classical Event-Clock Proposal Engines](#classical-event-clock-proposal-engines)
    - [Marked Hawkes Proposal Engines](#marked-hawkes-proposal-engines)
    - [Regime-Switching Hawkes Proposal Engines](#regime-switching-hawkes-proposal-engines)
    - [Recurrent Marked Temporal Point-Process Engine](#recurrent-marked-temporal-point-process-engine)
    - [Marked Add-Thin Sequence Engine](#marked-add-thin-sequence-engine)
    - [Empirical Reference-Motif Index](#empirical-reference-motif-index)
    - [Real Modern Reference-Motif Library](#real-modern-reference-motif-library)
    - [Reconstruction Scientific Target](#reconstruction-scientific-target)
    - [Reconstruction Math Verification](#reconstruction-math-verification)
    - [Point-in-Time Reconstruction Evidence](#point-in-time-reconstruction-evidence)
    - [Synchronized Cross-Series Constraints](#synchronized-cross-series-constraints)
    - [Empirical Motif Candidate Generation](#empirical-motif-candidate-generation)
    - [Historical Candidate Carving](#historical-candidate-carving)
    - [Cross-Currency Reconciliation](#cross-currency-reconciliation)
    - [Calibrated Reconstruction Ensembles](#calibrated-reconstruction-ensembles)
    - [Live Broker Delivery Capture](#live-broker-delivery-capture)
    - [Broker Delivery Fingerprints](#broker-delivery-fingerprints)
    - [Broker-Conditioned Reconstruction](#broker-conditioned-reconstruction)
    - [Atomic Reconstruction Persistence](#atomic-reconstruction-persistence)
    - [Reconstruction Activity Semantics](#reconstruction-activity-semantics)
    - [Derived Reconstruction Candlesticks](#derived-reconstruction-candlesticks)
    - [Reconstructed-History Strategy Sensitivity](#reconstructed-history-strategy-sensitivity)
    - [EURUSD Triangle Reconstruction Certification](#eurusd-triangle-reconstruction-certification)
    - [Temporal Reconstruction Orchestration](#temporal-reconstruction-orchestration)
    - [Reconstruction Evidence Diagnostics](#reconstruction-evidence-diagnostics)
    - [Public Reconstruction CLI and API](#public-reconstruction-cli-and-api)
  - [Orchestration Runtime](#orchestration-runtime)
    - [Runtime Model and Install Surface](#runtime-model-and-install-surface)
    - [Binary Provisioning and PyPI Packaging](#binary-provisioning-and-pypi-packaging)
    - [Public Orchestration API Boundary](#public-orchestration-api-boundary)
    - [Maintainer Runtime Diagnostics](#maintainer-runtime-diagnostics)
    - [Job Telemetry and Automation](#job-telemetry-and-automation)
    - [Cron Setup and Examples](#cron-setup-and-examples)
    - [Runtime User and Maintainer Docs](#runtime-user-and-maintainer-docs)
  - [API - Other Scripts, Modules, & Jupyter Support](#api-other-scripts-modules-jupyter-support)
    - [Script and Application Automation](#script-and-application-automation)
    - [Jupyter and External Scripts](#jupyter-and-external-scripts)
    - [Full Script Example](#full-script-example)
- [Setup](#setup)
  - [TLDR for all platforms](#tldr-for-all-platforms)
  - [Container Image](#container-image)
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

### Provider-neutral Dataset Catalog

Versioned provider adapters and a local dataset catalog keep historical
provider, logical dataset, immutable dataset version, observed/synthetic
origin, and delivery profile as separate identities. Mutable aliases resolve
once to an immutable version; replay receipts and pagination cursors retain
that version and query scope even after an alias moves.

```bash
histdatacom datasets --catalog catalog.json list
histdatacom datasets --catalog catalog.json resolve latest-qualified \
  --symbol EURUSD --period 202001 --receipt resolution.json
histdatacom datasets --catalog catalog.json verify latest-qualified
histdatacom datasets --catalog catalog.json replay resolution.json
```

See [Provider-neutral datasets and immutable catalog resolution](docs/provider-neutral-dataset-catalog.md)
for adapter authoring, manifest and configuration examples, licensing limits,
row/cursor identity, synthetic lineage, reconstruction preflight, and the
contract required by the future OANDA-compatible API in issue #77.

### Catalog-bound Reconstruction Experiments

Version 2.4 resolves a HistData catalog selector to immutable ASCII/T
partitions before reconstruction plan identity is computed. One frozen
experiment ID binds dataset version/revision, source hashes, roles, split and
leakage policy, evidence/preprocessing/feature/gate identities, authoritative
domain artifacts, and implementation versions. Every first-party stage carries
the catalog, resolution receipt, and experiment manifest; committed v2 products
retain the same identity at `source.experiment_id`.

```bash
histdatacom reconstruction --json experiment-list --root work/plan-artifacts
histdatacom reconstruction --json experiment-inspect \
  --manifest work/plan-artifacts/reconstruction-experiment-<sha256>.json
histdatacom reconstruction --json experiment-verify \
  --manifest work/plan-artifacts/reconstruction-experiment-<sha256>.json
```

Provider-neutral fields remain identity seams only. The executable milestone is
HistData.com ASCII/T; OANDA, alternate providers, and broker feeds remain later
milestones. See [Catalog-bound reconstruction experiments](docs/reconstruction-experiment-contracts.md).

### Show the help and options

```txt
histdatacom -h
```

```txt
usage: histdatacom [-h] [-A] [-U] [--by BY] [--version] [-V] [-D] [-X] [-C]
                   [--config PATH] [-p PAIR [PAIR ...]]
                   [--pair-groups GROUP [GROUP ...]] [-f FORMAT [FORMAT ...]]
                   [-t TIMEFRAME [TIMEFRAME ...]] [-s START_YEARMONTH]
                   [-e END_YEARMONTH] [-r EXPRESSION] [--random-seed INTEGER]
                   [-z IANA_ZONE] [-I] [-d] [-b BATCH_SIZE]
                   [-c CPU_UTILIZATION] [--data-directory DATA_DIRECTORY] [-v]
                   [--orchestration-start] [--no-orchestration-start]
                   [--submit-only] [--no-overlap]
                   [--schedule-key SCHEDULE_KEY] [--keep-runtime]
                   [--no-keep-runtime] [--request-json-out PATH]
                   [--request-bundle-out PATH] [--quality] [--repo-quality]
                   [--quality-preflight] [--repo-quality-columns]
                   [--quality-target PATH [PATH ...]]
                   [--quality-checks GROUP [GROUP ...]]
                   [--quality-report PATH] [--quality-preflight-report PATH]
                   [--quality-preflight-markdown]
                   [--quality-preflight-markdown-report PATH]
                   [--quality-preflight-profile-preview-output PATH]
                   [--quality-preflight-profile-preview-format {json,text,markdown}]
                   [--quality-preflight-validation-report PATH]
                   [--quality-preflight-run-validation]
                   [--quality-preflight-validation-evidence PATH]
                   [--quality-preflight-evidence PATH]
                   [--quality-preflight-evidence-max-age-seconds SECONDS]
                   [--quality-preflight-evidence-stale-ok]
                   [--quality-preflight-sample-size COUNT]
                   [--quality-profile PATH] [--quality-profile-preview]
                   [--quality-profile-preview-format {json,text,markdown}]
                   [--quality-profile-preview-output PATH]
                   [--quality-remediation-catalog-audit]
                   [--quality-fail-on SEVERITY] [--quality-max-errors COUNT]
                   [--quality-max-warnings COUNT]

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
                        space separated formats. -f ascii
  -t, --timeframes TIMEFRAME [TIMEFRAME ...]
                        space separated Timeframes. -t tick-data-quotes
  -s, --start_yearmonth START_YEARMONTH
                        set a start year and month for data. e.g. -s 2000-04
                        or -s 2015-00
  -e, --end_yearmonth END_YEARMONTH
                        set an end year and month for data. e.g. -e 2020-00 or
                        -e 2022-04
  -r, --random-window EXPRESSION
                        select deterministic duration/session tick windows;
                        random selection requires --random-seed, while session
                        expressions with both -s and -e return all matching
                        occurrences
  --random-seed INTEGER
                        seed required for reproducible random-window selection
  -z, --timezone, --output-timezone IANA_ZONE
                        append datetime_local to API results in an IANA
                        timezone; canonical cache and Influx timestamps remain
                        UTC

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
  --no-overlap          refuse submission when an active matching scheduled
                        job already exists in this runtime workspace
  --schedule-key SCHEDULE_KEY
                        stable logical key used by --no-overlap for scheduled
                        jobs
  --keep-runtime        leave a runtime started by this command running after
                        the job completes
  --no-keep-runtime     stop a runtime started by this command after waited
                        jobs complete
  --request-json-out PATH
                        write the resolved RunRequest JSON payload to PATH
                        without submitting work; use '-' for stdout
  --request-bundle-out PATH
                        write a scheduled-run bundle JSON payload to PATH
                        without submitting work; use '-' for stdout

Data quality:
  --quality             run offline data-quality assessment against local
                        datasets without contacting HistData.com
  --repo-quality        run offline data-quality assessment and write bounded
                        quality summary metadata back to the local .repo file
  --quality-preflight   benchmark a deterministic sample of existing .data
                        caches before running a cache-scale quality battery
  --quality-target, --quality-path PATH [PATH ...]
                        local file or directory to assess; supports
                        directories, HistData ZIP archives, CSV files, and
                        .data cache files
  --quality-checks GROUP [GROUP ...]
                        quality check groups to run; defaults to all.
                        Supported: all, inventory, ingestion, time, ticks,
                        domain, modeling, provenance, fingerprint
  --quality-report PATH
                        write the full machine-readable JSON quality report to
                        PATH
  --quality-preflight-report PATH
                        write the publish-safe JSON quality preflight report
                        to PATH
  --quality-preflight-markdown
                        print the publish-safe Markdown quality preflight
                        evidence report to stdout
  --quality-preflight-markdown-report PATH
                        write the publish-safe Markdown quality preflight
                        evidence report to PATH
  --quality-preflight-profile-preview-output PATH
                        write the resolved quality-profile preview to PATH and
                        reference it from quality preflight evidence
  --quality-preflight-profile-preview-format {json,text,markdown}
                        output format for --quality-preflight-profile-preview-
                        output; defaults to machine-readable json
  --quality-preflight-validation-report PATH
                        merge validation command status from a
                        closure/readiness JSON report into quality preflight
                        evidence; use 'latest' to discover the newest
                        compatible report under .histdatacom/closure-readiness
  --quality-preflight-run-validation
                        run bounded quality preflight validation commands
                        before rendering evidence
  --quality-preflight-validation-evidence PATH
                        write bounded machine-readable validation evidence to
                        PATH and reference it from quality preflight evidence
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
  --quality-profile-preview, --quality-profile-explain
                        print the resolved quality profile JSON without
                        running quality checks, writing reports, or submitting
                        work
  --quality-profile-preview-format, --quality-profile-explain-format {json,text,markdown}
                        output format for --quality-profile-preview; defaults
                        to machine-readable json
  --quality-profile-preview-output, --quality-profile-explain-output PATH
                        write the selected --quality-profile-preview rendering
                        to PATH; use '-' for stdout
  --quality-remediation-catalog-audit
                        enable remediation-catalog audit reporting in quality
                        reports, bounded payloads, and preflight evidence
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
  datasets    Resolve and verify versioned local datasets
  groups      List instrument groups and major triangles
  jobs        Inspect and control orchestrated work
  quality     Inspect local data quality evidence
  reconstruction  Plan, run, and inspect reconstruction
  runtime     Inspect and manage the orchestration runtime

Run `histdatacom analytics --help` for analytics commands.
Run `histdatacom cleanup --help` for cleanup commands.
Run `histdatacom datasets --help` for dataset commands.
Run `histdatacom groups --help` for group discovery commands.
Run `histdatacom jobs --help` for job telemetry commands.
Run `histdatacom quality --help` for quality commands.
Run `histdatacom reconstruction --help` for reconstruction.
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
  --commit-path path/to/changed-file.py \
  --acceptance-test '*=tests/unit/test_changed_behavior.py'
python scripts/closure_readiness.py \
  --issue 274 \
  --closure-verification \
  --infer-commit-paths \
  --commit-message "feat(scope): describe the change" \
  --acceptance-test '*=tests/unit/test_changed_behavior.py'
python scripts/closure_readiness.py --issue 274 --push-readiness
python scripts/closure_readiness.py --issue 274 --issue-audit
python scripts/closure_readiness.py --issue 274 --workflow
python scripts/closure_readiness.py \
  --issue 274 \
  --run-gates \
  --rerun-standalone-formatter-mutations
python scripts/closure_readiness.py \
  --summarize-report .histdatacom/closure-readiness/closure-274.json
python scripts/closure_readiness.py --open-issue-audit
python scripts/closure_readiness.py --open-issue-audit --json
python scripts/closure_readiness.py --issue 274 --workflow --close-issue
python scripts/closure_readiness.py \
  --issue 274 \
  --execute-workflow \
  --pre-mutation-gates \
  --rerun-formatter-mutations \
  --commit-message "feat(scope): describe the change" \
  --commit-path path/to/changed-file.py \
  --acceptance-test '*=tests/unit/test_changed_behavior.py'
```

The helper checks branch/upstream alignment, dirty and untracked files, linked
GitHub issue state, lingering pytest/pre-commit/Temporal/histdatacom tool
processes before and after gates, transient ZIP/CSV source artifacts
under `data/`, README help synchronization, `git diff --check`, main help smoke
output, pytest, and pre-commit. Reports are publish-safe JSON and Markdown with
a GitHub-ready close comment block. `--commit-readiness` validates the current
change scope and candidate Commitizen message without running `git add`,
`git commit`, or `git push`; use repeated `--commit-path` flags to declare the
intended file scope and catch unrelated dirty files. When paired with
`--issue`, the same report-only mode also accepts the `--acceptance-*` evidence
flags and prints commit readiness plus acceptance coverage in one human or JSON
payload. `--push-readiness` reports whether a clean `dev` branch with local
commits ahead of `origin/dev` is ready to push. Default issue-scoped reports are
local outputs under
`.histdatacom/closure-readiness/`; the helper verifies those paths are
gitignored before writing them and blocks closure if that safety check drifts.
`--closure-verification` is the one-shot non-mutating readiness mode for issue
work: it validates commit scope and message, acceptance coverage, focused pytest
commands supplied through `--acceptance-test`, full closure gates, optional
TestPyPI/simple-registry preflight, final git status, GitHub CLI/auth state,
issue readback, local workflow policy, source-artifact cleanliness, and owned
process health, then prints the exact `--execute-workflow` command when ready.
Use repeated `--commit-path` flags for an explicit scope or
`--infer-commit-paths` to record the current dirty worktree as the intended
scope while warning on broad or ambiguous inferred sets.
Explicit report paths still work, but the report marks whether they may dirty
the current worktree. `--workflow` performs the cheap precheck first, stops
before expensive gates when local state is blocked, writes safe default reports,
and enforces the `dev` branch workflow. Use `--close-issue` only when ready to
close; it remains an explicit opt-in action and reads back the final issue state
after closing. `--execute-workflow` is the explicit mutating mode: it validates
the declared paths and Commitizen message, runs targeted `git add`, commits,
checks push readiness, pushes to the expected upstream, runs closure gates,
closes the issue, and writes bounded execution evidence plus full ignored logs.
`--open-issue-audit` is the non-mutating whole-queue triage mode: it reads the
live open GitHub issue set, local branch/upstream/worktree state, bounded source
signals, and recent issue context, then classifies and ranks the next suggested
action in compact human output or stable JSON.
Add `--pre-mutation-gates` to run the same closure gate battery before the first
`git add`; the workflow blocks staging, commit, and push if those gates fail or
rewrite files, and records the result separately from the post-push closure
gates. Gate-induced file rewrites are reported with changed paths, responsible
gates, and whether the mutation appears to be formatter/tool output. The default
behavior stays conservative: formatter rewrites still block until the required
focused verification and full gate rerun are complete. For standalone
`--run-gates` reports, add `--rerun-standalone-formatter-mutations` only when
you want that one-time formatter/tool-only rerun performed automatically. Add
`--rerun-formatter-mutations` only when you want the workflow to perform that
one-time formatter/tool-only rerun automatically before staging. Successful
execution prints a compact closeout with final issue, branch, commit,
acceptance, report-path, runtime/process health, and the slowest workflow
phases. Use `--json` for the same compact closeout as a stable scriptable
payload; use `--full-json` only when stdout needs the full execution evidence
object. Long workflow runs also stream bounded phase progress to stderr so JSON
stdout stays parseable; add `--quiet-progress` when automation should suppress
live progress while retaining phase timing in the saved evidence report. Issue
closure reports
parse issue checklists or `Acceptance criteria`
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

#### Download and extract the current month's available EURUSD ASCII tick data into the default data directory ./data

```sh
histdatacom -p eurusd -f ascii -t tick-data-quotes -s now
```

#### include the `-D` flag to download but NOT extract to csv

```sh
histdatacom -D -p usdcad -f ascii -t tick-data-quotes -s now
```

#### include the `-C` flag to build internal Polars caches and discard ZIP/CSV sources

```sh
histdatacom -C -p eurusd -f ascii -t tick-data-quotes -s 2024-01 -e 2024-03
```

Cache-only mode validates and downloads the selected HistData archives, builds
canonical `.data` cache files, and removes transient ZIP/CSV sources after each
cache is ready. It is intentionally limited to cache-capable ASCII tick quote
datasets, and it does not merge the caches into memory.

The `.data` cache is also the canonical training substrate. Cache builds
materialize one flat row-aligned ASCII tick table with observed bid/ask data,
explicit row identity, timestamp features, data-quality issue columns,
classification codes, training controls, and nullable `synth_*` placeholders.
The durable training row key is `series_id`, `period`, and `row_id`;
`datetime`/`timestamp_utc_ms` remains an observed time-axis feature and is not
the only identity value. This lets later training stages mask or bucket
timestamps without losing deterministic row identity.

#### clean up transient source artifacts without removing internal caches

```sh
histdatacom cleanup sources --data-directory data
histdatacom cleanup sources --data-directory data --apply
histdatacom cleanup status --data-directory data --pair-groups majors -f ascii -t T
```

Cleanup mode is a dry run unless `--apply` is present. It removes downloaded
ZIP and CSV source artifacts while preserving internal `.data` caches. Use
`cleanup status` to inspect cache counts, pending source cleanup, disk pressure,
runtime state, and offline workflow snapshots for a symbol or instrument group
without shelling out to `find`, `df`, `ps`, or raw Temporal commands. Add
`--json` for the stable scriptable payload.

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
    - tick-data-quotes
  start_yearmonth: 2022-01
  end_yearmonth: 2022-03
  # Optional deterministic tick projection:
  # random_window: 2d
  # random_seed: 1729
  data_directory: /data/histdata
  request_bundle_out: requests/eurusd-cache-bundle.json
  request_json_out: requests/eurusd-cache.json
  cpu_utilization: medium
  orchestration_start: true
  orchestration_wait_result: false
  no_overlap: true
  schedule_key: eurusd-cache
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
    epoch_artifact: reports/eurusd-feed-epochs.v1.json
    min_evidence_periods: 12
    min_segment_periods: 3
    min_boundary_support: 0.75
    json: true
  jobs:
    command: submit
    request_bundle: requests/eurusd-cache-bundle.json
    submit_only: true
    json: true
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
supported ASCII tick quote datasets.

---

#### Available Formats

The raw HistData dimension currently supported by the application is:

| Format | Timeframe |
| --- | --- |
| `ascii` | `tick-data-quotes` |

Other HistData platform formats and raw bar timeframes were intentionally
removed. Downsampling and platform-specific formatting will be added back as
derived outputs after the ASCII tick substrate is stable.

##### CSV Dialect and Format Specifications

- *For Detailed specifications for the CSVs that the histdata.com repo provides see [histdata.com_data_specs.md](https://github.com/dmidlo/histdata.com-tools/blob/main/histdata.com_data_specs.md)*

##### To download ASCII tick-data-quotes

```sh
histdatacom -p usdjpy -f ascii -t tick-data-quotes -s now
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

##### Deterministic random and session windows

Use `-r/--random-window EXPRESSION` to project a bounded subset of ASCII tick
data. Random selection always requires an explicit non-negative
`--random-seed`; the expression, seed, requested symbols, repository inventory,
and optional `-s`/`-e` bounds reproduce the same half-open `[start, end)`
selection regardless of input order or worker parallelism.

```sh
# One reproducible 90-minute interval inside common EURUSD/GBPUSD support.
histdatacom -C -I -p eurusd gbpusd -f ascii -t tick-data-quotes \
  -r 90m --random-seed 1729

# Every London sampling window in January 2024; no seed is needed when a
# session expression has both bounds. Equal start/end months are valid here.
histdatacom -I -p eurusd -f ascii -t tick-data-quotes \
  -s 2024-01 -e 2024-01 -r ldn
```

Duration units are case-sensitive: `y` is a calendar year, `q` a calendar
quarter, `M` a calendar month, `w` a week, `d` a day, `h` an hour, and `m` a
minute. Year, quarter, and month selections are calendar-aligned; fixed
durations are UTC-minute-aligned. Supported forms include `1y`, `1q`, `1M`,
`2w`, `2d`, `6h`, `90m`, `ldn`, `ldn-ny`, `syd-syd`, `hk-3d`, `hk-3d-hk`,
`45m-auk`, `1h-auk-1h`, and `30m-ldn-1w-syd-1h`. Unsupported mixtures fail
closed.

Session codes use explicit 08:00–17:00 local-clock profiles and IANA timezone
rules:

| Code | Sampling location | IANA timezone |
|---|---|---|
| `fra` | Frankfurt/Paris | `Europe/Paris` |
| `ldn` | London | `Europe/London` |
| `ny` | New York | `America/New_York` |
| `chi` | Chicago | `America/Chicago` |
| `la` | San Francisco/Los Angeles | `America/Los_Angeles` |
| `auk` | Auckland/Wellington | `Pacific/Auckland` |
| `syd` | Sydney | `Australia/Sydney` |
| `tyo` | Tokyo | `Asia/Tokyo` |
| `hk` | Hong Kong/Singapore | `Asia/Hong_Kong` |

These profiles are reproducible sampling windows, not claims about centralized
FX exchange hours. DST follows each IANA zone. A single session selects its
open through close; ordered session pairs span from the first open through the
second close, wrapping to the next local day when needed. Equal session anchors
such as `syd-syd` end at the following session close. Hour/minute tokens outside
anchors add padding; day/week/month/quarter/year tokens between anchors add the
bridge duration.

HistData archives and canonical `.data` caches remain complete monthly evidence.
The planner resolves one compact, versioned selection against the common
repository coverage of all requested symbols and schedules only intersecting
months. It never truncates or rewrites ZIP, CSV, or cache artifacts. Exact
filtering happens only while materializing API results or streaming rows to
InfluxDB; empty or off-support selections fail instead of silently substituting
another interval.

##### to fetch a single year's data, leave out the month

- note: unless you're fetching data for the current year, a year-only tick request
  expands to the monthly tick archives available for that year.

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

Select one oriented triangle by naming all three symbols in the relationship:

```txt
histdatacom --pair-groups triangle-eurgbp-eurusd-gbpusd -f ascii -t tick-data-quotes -s 2022
```

Individual triangle group names use the pattern
`triangle-{direct}-{numerator}-{denominator}`. For example,
`triangle-eurgbp-eurusd-gbpusd` downloads `eurgbp`, `eurusd`, and `gbpusd`
for the rule `EURUSD / GBPUSD ~= EURGBP`.

Discover available groups without inspecting the source:

```txt
histdatacom groups list
histdatacom groups list --triangles
histdatacom groups show major-triangles
histdatacom groups show triangle-eurgbp-eurusd-gbpusd
histdatacom groups list --triangles --json
```

`histdatacom groups list` shows broad baskets such as `majors` and
`major-triangles`. Add `--triangles` to list each individual major triangle
group with its readable relationship rule.

##### to fetch a single month's data, include a month, but do not use the `-e, --end_yearmonth` flag

- if you're requesting tick-data-quotes for any
    year except the current year, you will receive the
    the whole year's data
- this example leaves out the `-p --pair` flag, and will
    fetch data for all 66 available instruments

```txt
histdatacom -f ascii -t tick-data-quotes -s 2012-07
```

#### `Start` & `Now` Keywords

you may have noticed that two special year-month keywords exist
 `start` and `now`

- `start` may only be used with the `-s --start_yearmonth`
   flag and the `-e --end_yearmonth` flag **must** be specified
   to indicate a range of data

```txt
histdatacom -p audusd -f ascii -t tick-data-quotes -s start -e 2008-12
```

- `now` used alone will return the current year-month
- when used with as `-s now` it will return the most current month's data

```txt
histdatacom -p frxeur -f ascii -t tick-data-quotes -s now
```

`now` when used with the `-e --end_yearmonth` flag is intended to be the end of a range. Rather, if the flags were to be `-s 2019-04 -e now` the request would return data from April 2019-04 to the present.

```txt
histdatacom -p xagusd -f ascii -t tick-data-quotes -s 2019-04 -e now
```

---

##### Multiple Datasets

##### multiple datasets can be requested in one command

this example with use the `-e --end_yearmonth` flag to request a range of data for multiple instruments.

- note: Large requests like these are to be avoided. remember to sign up with histdata.com to help them pay for network costs

```txt
histdatacom -p eurusd usdcad udxusd -f ascii -t tick-data-quotes -s start -e 2017-04
```

---

##### CPU Utilization

One can set a cap on CPU Utilization with `-c --cpu_utilization`

- available levels are, `"low"`,`"medium"`,`"high"`
- **OR**
- integer percent 1-200
  eg. `-c 100` is equal to `-c high`

```sh
histdatacom -c medium -p udxusd -f ascii -t tick-data-quotes -s 2015-04 -e 2016-04
```

---

### Import to InfluxDB

To import data to an influxdb instance, install the Influx extra and use the `-I --import_to_influxdb` flag along with an `influxdb.yaml` file in the current working directory (where ever you are running the command from).

```sh
pip install "histdatacom[influx]"
```

- ascii is the only format accepted for influxdb import.
- all histdata.com datetime data is in EST (Eastern Standard Time) with no adjustments for daylight savings.
- InfluxDB does not attach a display timezone; all datetime data is written as UTC
  epoch timestamps with millisecond precision.
- this tool converts histdata.com ESTnoDST to UTC Epoch milli-second timestamps as part of the import-to-influx process
- `-z/--timezone` never changes Influx timestamps. Select a display timezone in
  the Influx query or UI instead.
- enriched ASCII tick `.data` columns are projected onto the same tick point:
  observed quote fields, quality issue flags, quality/classification codes,
  training controls, and populated synthetic fields when present. Duplicate
  tick timestamps keep distinct Influx point identity with deterministic
  `row_id` tags; the `.data` row key remains the source of truth.

```txt
histdatacom -I -p eurusd -f ascii -t tick-data-quotes -s start -e now
```

#### Docker-backed InfluxDB Smoke

When Docker is available, contributors can run a disposable InfluxDB v2 smoke
without a user-managed `influxdb.yaml`:

```sh
python scripts/smoke_influx_docker.py
```

The smoke starts `influxdb:2.7-alpine`, writes representative HistData tick
line-protocol batches through the real `InfluxBatchWriter`, queries the
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
the configured data directory. Targets can be plain HistData CSV files, HistData
ZIP archives, directories containing those files, or the canonical `.data`
cache file.

Quality reports remain audit artifacts. Training-facing quality semantics are
row columns on enriched ASCII tick `.data` caches, including explicit
`dq_issue_*` indicators, quality counts, classification labels, training
usability/weight, and synthetic placeholders. Report and bounded summary
surfaces should derive from or stay consistent with those row-level columns, so
downstream training code does not need to parse report JSON or join separate
quality tables to assemble a training row.

When the engine intentionally skips a target-rule evaluation—for example, a
semantic scan of a ZIP whose matching extracted CSV is preferred—the report
adds optional `metadata.quality_engine` reconciliation metadata. Its
`histdatacom.quality-skip-events.v1` event list records a stable reason code,
rule ID, target kind, and publish-safe format/timeframe/symbol/period axis; it
never records local paths or row samples. Events and aggregate reason/rule
counts are deterministically bounded with explicit omission metadata. The same
contract is projected into bounded runtime payloads as `quality_engine`, and
the human summary reports planned, executed, and skipped evaluation totals.
The existing `skipped_duplicate_archive_rule_evaluation_count` remains for
compatible consumers.

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
  --quality-preflight-report reports/major-triangles-tick-preflight.json \
  --quality-preflight-markdown-report reports/major-triangles-tick-preflight.md \
  --quality-preflight-validation-evidence reports/major-triangles-validation.json \
  --quality-preflight-profile-preview-output reports/major-triangles-quality-profile.md \
  --quality-preflight-profile-preview-format markdown
```

The console output is human-readable. The optional
`--quality-preflight-report PATH` file is publish-safe JSON with target counts,
cache bytes, sampled paths, row counts, throughput, ETA range, sample quality
summary, generated timestamp, package version, preflight policy inputs,
no-target diagnostics, and a decision section that says whether the full battery
is safe, warned, failed, or has no matching targets. Safe and warned decisions
include the next `histdatacom --quality ...` command for the same target scope.
Every quality preflight also runs the fingerprint contract self-audit and
records its pass/fail status, bounded findings, representative report-surface
matrix, standalone verification command, and explicit
`fail-preflight-on-error` policy in the JSON evidence. The audit validates the
declared fingerprint schema/report registry and proves that a generated
representative report exposes each implemented fingerprint summary in full
report metadata, bounded payloads, and human CLI/report summaries unless the
surface is explicitly marked intentionally absent. If the contract audit fails,
preflight fails before recommending a full quality battery, even when the cache
sample and Temporal budget checks pass.
Use `--quality-preflight-markdown-report PATH` to write the matching
copy/paste-safe Markdown evidence report for GitHub issue updates, release
handoffs, or operator notes. That Markdown includes command/config summary,
package version, cache inventory, benchmark sample, ETA/rate, Temporal budget,
fingerprint contract audit summary, source-artifact cleanliness, POSIX disk
headroom, validation commands, and the explicit runtime-cleanup disposition for
the local preflight run. Pass
`--quality-preflight-profile-preview-output PATH` when the same evidence bundle
should include the resolved quality-profile preview used by the preflight. The
preview artifact can be JSON, text, or Markdown via
`--quality-preflight-profile-preview-format`, and the preflight evidence records
its publish-safe path, format, schema version, byte size, and SHA-256 hash. Use
`--quality-preflight-markdown` when stdout should be the Markdown report instead
of the compact console summary. Use `--quality-preflight-sample-size COUNT` to
tune the bounded sample.

Validation rows stay `not-run` by default so ordinary quality preflights do not
run repository gates. For release notes or GitHub issue evidence, pass
`--quality-preflight-validation-report PATH` to merge command status from a
closure/readiness JSON report. Use
`--quality-preflight-validation-report latest` to resolve the newest compatible
JSON report under `.histdatacom/closure-readiness` without running gates. The
closure report's release-independent `full-tests` result is recognized as
full-pytest evidence; importing it never starts tests or coverage. Pass
`--quality-preflight-run-validation` to run only the bounded local validation
bundle: focused quality-preflight tests, the README help-sync check, and
`git diff --check`. It does not run full pytest/coverage or pre-commit.

Pass `--quality-preflight-validation-evidence PATH` to write the validation
rows as a dedicated bounded JSON artifact and register it under
`evidence.artifacts.validation_evidence`. The artifact includes its schema and
generated timestamp plus each configured command's status, exit code, duration,
summary, and publish-safe output-artifact path when available. Its registry
entry records the publish-safe path, SHA-256, and byte size. Using the artifact
option alone is the dry inspection path: commands remain planned/`not-run` and
no repository gates execute. Combine it with an imported report to serialize
existing closure receipts, or with `--quality-preflight-run-validation` for the
bounded checks. Output summaries are truncated and publish-safe; full logs stay
in separately referenced artifacts.

This application evidence supplements normal issue/PR validation notes and can
be consumed by CI, but it does not replace CI or automate GitHub issues, pull
requests, comments, or labels. Full repository gates, publishing, TestPyPI,
PyPI, and GitHub issue closure remain explicit closure/release workflow
responsibilities.

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
`histdatacom cleanup sources` to inspect removable ZIP and CSV files, then
repeat with `--apply` when the report is expected. The cleanup command
preserves internal `.data` cache files. Use `histdatacom cleanup status` first
when an operator needs the cache count, pending cleanup count, disk pressure,
runtime state, and durable workflow status in one report.

```sh
histdatacom -D -X -p eurusd -f ascii -t tick-data-quotes --data-directory /Volumes/histdata/data
histdatacom --repo-quality \
  --quality-target /Volumes/histdata/data/ASCII/T/eurusd \
  --quality-report /Volumes/histdata/reports/eurusd-ascii-tick-quality.json \
  --data-directory /Volumes/histdata/data
histdatacom --build-cache -p eurusd -f ascii -t tick-data-quotes --data-directory /Volumes/histdata/data
```

#### Quality Targets and Check Groups

Quality groups are composable. `all` is the default and cannot be combined with
specific groups in the same command.

```sh
histdatacom --quality --quality-target data/ --quality-checks inventory ingestion
histdatacom --quality --quality-target data/DAT_ASCII_EURUSD_T_201202.csv --quality-checks time ticks
histdatacom --quality --quality-target data/DAT_ASCII_EURUSD_T_201202.zip --quality-checks ticks domain
```

Supported groups:

| Group | Scope |
| --- | --- |
| `inventory` | ZIP integrity, filename metadata, expected coverage manifest |
| `ingestion` | text readability, line endings, delimiter/header checks, schema and typed parsing, row-count anomalies |
| `time` | EST-no-DST to UTC normalization, month boundaries, ordering, duplicates, granularity, gaps, cross-file continuity |
| `ticks` | tick bid/ask ordering, spread, duplicate/stale/burst/one-sided quote behavior, spread regimes |
| `domain` | symbol metadata, quote conventions, calendar/session tags, cross-instrument consistency |
| `modeling` | advisory modeling-readiness checks for leakage risk, spread-cost assumptions, and target horizon feasibility |
| `provenance` | optional orchestration manifest/status lineage checks for artifact paths, sizes, checksums, cache metadata, stale caches, and orphan files |
| `fingerprint` | deterministic INFO-only target and run-scoped time-series fingerprints for coverage, topology, distributions, regimes, dynamics, dependence, stationarity, decomposition, and cross-series FX relationships |

`fingerprint.series` payloads include a `calendar_regimes` section for readable
ASCII tick targets. It counts session states, active/clock sessions,
overlaps, special windows, holiday/event tags, calendar tags, source
hour-of-day, and source day-of-week. The section embeds the calendar policy and
profile metadata used for classification, so incomplete/static calendar
profiles remain advisory and visible rather than becoming hidden failures. Tick
fingerprints also include bounded `conditional_distributions` for spread by
active session and special tag when spread data is available.

Calendar profiles can set `weekend_activity_policy` to `strict`, `advisory`, or
`allowed`, and `expected_session_closure_policy` to `expected` or `unexpected`.
Topology remediation keeps the stable `verify_weekend_session_policy` code but
adds bounded policy context with the profile name/source/version, EST-no-DST to
UTC basis, completeness, and the active treatment. Strict profiles produce an
inspection action, advisory profiles request assumption review, and allowed
weekend activity is rendered as a contextual note rather than a run-level next
action. Expected session closures remain contextual unless the profile
explicitly marks them `unexpected`.

```json
{
  "rules": {
    "domain.calendar_sessions": {
      "calendar_profile": {
        "weekend_activity_policy": "strict",
        "expected_session_closure_policy": "expected"
      }
    }
  }
}
```

Tick fingerprints include `microstructure_dynamics` for interarrival times,
spread changes, spread jumps, stale quote runs, bursts, and one-sided movement.
These sections record their calculation basis and topology limitations, so
non-monotonic timestamps, duplicates, gaps, or insufficient sequence rows remain
advisory metadata rather than hidden assumptions.
Readable tick fingerprints also include a `dependence` section with
observed-sequence autocorrelation summaries for spreads plus spread-change
series at profile-configured lags. Lags that are too long for the sampled
sequence, or series with zero variance, are reported as skipped lag metadata
instead of NaN values or quality failures.
They also include `stationarity_diagnostics` with advisory rolling mean/variance
drift, first/middle/last distribution-shift summaries, skipped rolling-window
reasons, sample counts, configured windows, rounding policy, zero-variance
markers, and deterministic transform recommendations such as `log_return`,
`differencing`, and `session_conditioning`. These diagnostics are descriptive
fingerprint facts only; nonstationarity does not fail a quality run.
Readable ASCII tick fingerprints also include a tick-only `decomposition`
section. It reuses the stationarity result, source-calendar hour/day/session
classification, and profile-configured rolling windows to emit deterministic
linear-trend, seasonal-bucket, residual, smoothing-window, and two-segment
structural-break proxies. Buckets and structural candidates are bounded by the
profile histogram limit; insufficient samples, skipped windows, zero variance,
and stationarity limitations remain explicit advisory metadata. These are
descriptive proxies, not fitted forecasting models, and no retired bar or M1
schema is emitted.

The decomposition section embeds a `period`-grain training projection with the
stable identity fields `series_id`, `period`, and `row_id`. API consumers can
call `decomposition_training_projection(...)` for its flat scalar values and
`project_decomposition_onto_training_frame(...)` to repeat those period facts
onto an already enriched ASCII tick frame without parsing a report or performing
a side join. Row count and identity columns are preserved.

Fingerprint runs also emit `cross_series_fingerprint` metadata using
`histdatacom.cross-series-fingerprint.v1`. It groups related ASCII tick series
by timeframe and period, then reports bounded symbol membership, full timestamp
grid overlap, missing counts, unequal coverage ranges and limiting legs,
pairwise return correlations, inverse and triangular consistency, and stale
forward-fill risk. Panel coverage also reports union/common periods, missing
period counts, unequal symbol ranges, and the legs that limit the common start
or end. The group topology rollup includes row/parsed counts,
duplicate and non-monotonic timestamps, suspicious and expected-session gaps,
weekend activity, and source/cache provenance. Legacy raw `.data` caches are
enriched in memory before this projection, and any row-level evidence uses
`series_id`, `period`, and `row_id`; timestamp alone is never treated as durable
identity. Full reports expose `metadata.cross_series_fingerprint`, bounded
runtime payloads expose `fingerprint_cross_series`, and the CLI renders a
concise `Cross-series fingerprint` section.

Before trusting cache-backed fingerprints after a cache migration, enrichment
change, manual cache copy, or unexpected source/cache timestamp change, run an
opt-in cache/source parity assessment. Parity is disabled by default, so normal
fingerprints retain their cache-first selection and cost. Enable it in a quality
profile and run the ordinary fingerprint check against the source CSV or ZIP:

```json
{
  "schema_version": "histdatacom.quality-profile.v1",
  "name": "fingerprint-cache-source-parity",
  "rules": {
    "fingerprint.series": {
      "cache_source_parity": {
        "enabled": true,
        "mismatch_limit": 16
      }
    }
  }
}
```

```sh
histdatacom --quality \
  --quality-target data/DAT_ASCII_EURUSD_T_201202.csv \
  --quality-checks fingerprint \
  --quality-profile fingerprint-cache-source-parity.json \
  --quality-report reports/eurusd-fingerprint-parity.json
```

The advisory `cache_source_parity` target section compares bounded coverage,
topology, calendar-regime, conditioned-spread, row-identity,
duplicate-timestamp, quality-report, and Influx-projection evidence. It keeps
raw source, raw legacy cache, enriched training cache, quality report, and
Influx projection as separate bases; legacy caches are enriched in memory and
are never rewritten. Stable mismatch codes and basis metadata roll up into
`metadata.time_series_fingerprint_cache_source_parity_summary`, bounded payload
key `fingerprint_parity`, and the CLI `Fingerprint cache/source parity` section.
Source paths and ZIP members remain publication-safe, mismatch fields and target
summaries are bounded, and no raw rows or full duplicate payloads are emitted.
Missing sources or caches are reported as `not_compared`; stale caches are
compared and marked advisory rather than accepted by the normal freshness path.

Every supported ASCII tick fingerprint also emits bounded
`synthetic_constraints` for the deterministic reference-set generator. The protocol separates
`defects_to_avoid`, `stylized_facts_to_preserve`, and
`source_artifacts_to_parameterize`; records stable comparison modes and
tolerances; and exposes advisory hints for session mix, spread regimes, gap
topology, expected closures, stationarity transforms, cache provenance, and
durable row identity. Its canonical generator input is the enriched `.data`
frame, not nested report JSON. Legacy raw caches are enriched in memory before
row issue columns are counted.

Python consumers can call `synthetic_constraints_from_training_frame(...)`
with an enriched Polars frame and its fingerprint, then call
`generate_synthetic_ticks_from_reference(...)`. The generator applies a seeded,
contiguous empirical block bootstrap to paired midpoint log returns and spreads,
filters rows marked by `dq_issue_*`, and enforces explicit inspection and output
row limits for very large periods. It refuses a missing fingerprint or
pre-populated synthetic columns unless replacement is explicitly requested.

Generated values augment the same rows only in `synth_bid`, `synth_ask`,
`synth_spread`, `synth_mid`, `synth_method_code`, `synth_confidence`, and
`synth_usable`. Observed `bid`/`ask`, timestamp features, duplicate-timestamp
rows, and durable `series_id`/`period`/`row_id` identity remain unchanged. Every
successful generation is automatically projected into a market-only candidate
cache, run through the ordinary `fingerprint.series` rule, and compared with
the reference through the synthetic constraint validator. Statistical mismatch
remains advisory rather than a hard data-quality gate.

Generate an enriched synthetic cache and optionally retain its ordinary
candidate fingerprint report:

```sh
histdatacom quality synthetic-generate \
  --reference-cache data/ASCII/T/EURUSD/2012/01/.data \
  --reference-report reports/reference-quality.json \
  --output-cache generated/ASCII/T/EURUSD/2012/01/.data \
  --candidate-report reports/generated-quality.json \
  --seed 17 \
  --json
```

The command will not overwrite observed columns or an existing output cache by
default. `--max-reference-rows` and `--max-generated-rows` bound work; rows
beyond the generation limit remain present with null synthetic values and
`synth_usable=false`. Volume synthesis, raw M1/OHLC generation, and derived
candlestick output remain outside this ASCII tick feature and follow later
issues #80 and #18.

Validate an exported candidate tick dataset by running the normal fingerprint
quality path for both reference and candidate, then comparing their saved
reports:

```sh
histdatacom quality synthetic-validate \
  --reference-report reports/reference-quality.json \
  --candidate-report reports/candidate-quality.json \
  --json
```

The advisory `histdatacom.synthetic-fingerprint-validation.v1` result reports
matched, mismatched, and missing target axes; candidate defect violations;
bounded stylized-fact mismatches; output/identity contract drift; and stable
mismatch codes. The validator itself does not mutate data or turn statistical
drift into a hard quality gate. Full reports expose
`metadata.time_series_fingerprint_synthetic_constraint_summary`, bounded
payloads expose `fingerprint_synthetic_constraints`, and the ordinary
fingerprint CLI summary renders `Synthetic fingerprint constraints`.

Classical baseline diagnostics are a separate, opt-in layer after the
fingerprint substrate. They are disabled by default and do not change quality
status. Enable the low-dependency baseline families through the ordinary
fingerprint profile:

```json
{
  "schema_version": "histdatacom.quality-profile.v1",
  "name": "classical-baselines",
  "rules": {
    "fingerprint.series": {
      "classical_baselines": {
        "enabled": true,
        "evaluation_fraction": 0.2,
        "minimum_training_rows": 20,
        "minimum_evaluation_rows": 5,
        "rolling_windows": [5, 20],
        "session_seasonal_enabled": true,
        "rounding_digits": 12
      }
    }
  }
}
```

The `histdatacom.classical-baselines.v1` section evaluates observed midpoint
values with a chronological holdout ordered by `series_id`, `period`, and
`row_id`. It never shuffles, never requires timestamp as durable identity, and
uses walk-forward forecasts where prior observed values become available only
after their row. Initial models are naive/random-walk, rolling mean, rolling
median, and session-conditioned seasonal-naive when the enriched session class
and calendar fingerprint support it. Metrics are emitted only after the
configured training and evaluation minima produce a valid split.

Stationarity status, rolling drift, distribution shift, skipped windows,
zero-variance markers, and the `log_return`, `differencing`, and
`session_conditioning` recommendations remain explicit evaluation guards.
Recommended transforms are reported but are not silently applied, and
nonstationarity never becomes a hard quality failure. The fitted curriculum now
includes exponential smoothing, explicit-order AR/ARMA/ARIMA,
SARIMA/ARIMAX/SARIMAX, structural state-space and Kalman diagnostics, symmetric
ARCH/GARCH, and family-neutral comparison. Automatic model selection and
forecasting leaderboards remain deliberately deferred.

Python consumers can call
`classical_baseline_diagnostics_from_training_frame(...)`, then
`project_classical_baseline_onto_training_frame(...)` to repeat the bounded
period-level readiness, split, best-model, and error scalars onto the same
enriched rows. The projection preserves observed bid/ask and duplicate
timestamp rows, requires only `series_id`/`period`/`row_id` identity, and flows
through the existing same-point Influx projection. Full reports expose
`metadata.time_series_fingerprint_classical_baseline_summary`, bounded payloads
use `fingerprint_classical_baselines`, and the fingerprint CLI renders
`Classical fingerprint baselines`.

The broader fitted-model curriculum begins with a separate opt-in input and
evaluation contract. It regularizes the enriched ASCII tick frame without
restoring raw M1 as an independent input or fitting ETS/ARIMA/state-space/GARCH
models prematurely:

```json
{
  "schema_version": "histdatacom.quality-profile.v1",
  "name": "classical-model-input",
  "rules": {
    "fingerprint.series": {
      "classical_model_input": {
        "enabled": true,
        "frequency_ms": 60000,
        "alignment_epoch_ms": 0,
        "closed_side": "left",
        "label_side": "left",
        "midpoint_aggregation": "last",
        "spread_aggregation": "last",
        "minimum_observations_per_bin": 1,
        "expected_closure_policy": "mark",
        "unexpected_missing_policy": "mark",
        "transform": "level",
        "differencing_order": 0,
        "seasonal_differencing_order": 0,
        "seasonal_period": 0,
        "horizons": [1],
        "fold_kind": "expanding",
        "minimum_training_observations": 20,
        "minimum_evaluation_observations": 5,
        "step_size": 5,
        "rolling_window": 0,
        "embargo_observations": 0,
        "rounding_digits": 12,
        "resources": {
          "max_source_rows": 1000000,
          "max_regularized_observations": 100000,
          "max_folds": 64,
          "max_horizons": 16,
          "max_candidate_orders": 32,
          "max_fit_attempts": 64,
          "max_wall_time_seconds": 300,
          "max_memory_bytes": 536870912,
          "max_retained_diagnostics": 64
        }
      }
    }
  }
}
```

`build_classical_model_input(...)` produces a bounded regular-grid derived
view and the `histdatacom.classical-model-input.v1` contract. UTC bins are
left-closed and left-labeled (`[start,end)`), aligned to an explicit epoch, and
never cross a source period. Midpoint and spread support explicit
first/last/mean/median aggregation. Derived midpoint open/high/low/close values
are descriptive fields in the regularized view; they do not make raw bar data
canonical again. Multiple ticks, duplicate timestamps, minimum bin support,
source-row bounds, truncation, and rounding all remain explicit.

Empty bins are never forward-filled. Calendar classification separates expected
weekend/session closures from unexpected missing observations; both remain null
on the canonical grid. The `omit` closure policy suppresses closure-target
evaluation folds without compressing elapsed grid time or forecast horizons. Level, log-level,
return, log-return, first differencing, and seasonal differencing are applied
only when configured. The contract records invalid domains, warm-up loss,
inverse-transform requirements, and the requirement to report forecast errors
on the original requested scale.

Expanding and rolling folds are chronological, never shuffled, and record
training/evaluation boundaries, configured horizons, step size, embargo, and
incomplete targets through `histdatacom.classical-model-fold.v1`. Shared
`histdatacom.classical-model-fit-result.v1` and
`histdatacom.classical-model-evaluation-result.v1` schemas define bounded
fitted/converged/limited/skipped/timeout/numerical/dependency/failure metadata
for later model families without including backend exception text, full
forecasts, residual histories, or fitted objects.

`project_classical_model_input_onto_training_frame(...)` augments the same
enriched tick rows with registered nullable `cm_input_*`, `cm_fold_*`, and
`cm_evaluation_*` scalar columns. Projection uses `series_id`, `period`, and
`row_id` as durable identity and repeats a completed-bin value only on rows at
or after its UTC close. Masked timestamps remain identifiable, duplicate
timestamps remain distinct, observed bid/ask and `synth_*` fields are preserved,
and post-observation evaluation values are marked diagnostic-only. The same
columns flow through the ordinary Polars cache and same-point Influx projection;
consumers do not need report parsing or model side-table joins.

No rich numerical dependency is added to the core package by this contract.
Statsmodels and ARCH providers belong in the optional `models` extra; the
low-dependency fingerprint and baseline paths remain usable when those
providers are absent. Full reports use
`time_series_fingerprint_classical_model_input_summary`, bounded payloads use
`fingerprint_classical_model_input`, and console output renders
`Classical model input contracts`.

The first fitted family is also opt-in and requires the `models` extra:

```sh
pip install "histdatacom[models]"
```

```json
{
  "schema_version": "histdatacom.quality-profile.v1",
  "name": "exponential-smoothing",
  "rules": {
    "fingerprint.series": {
      "classical_model_input": {
        "enabled": true,
        "frequency_ms": 60000,
        "transform": "level",
        "horizons": [1, 5, 20],
        "fold_kind": "expanding",
        "minimum_training_observations": 40,
        "minimum_evaluation_observations": 5,
        "step_size": 20
      },
      "exponential_smoothing": {
        "enabled": true,
        "projection_specification_id": "hw-add",
        "projection_horizon": 1,
        "baseline_rolling_windows": [5, 20],
        "specifications": [
          {
            "specification_id": "ses",
            "family": "ses",
            "error": "add"
          },
          {
            "specification_id": "holt-damped",
            "family": "holt",
            "error": "add",
            "trend": "add",
            "damped_trend": true
          },
          {
            "specification_id": "hw-add",
            "family": "holt_winters",
            "error": "add",
            "trend": "add",
            "seasonal": "add",
            "seasonal_periods": 24,
            "initialization_method": "estimated",
            "optimized": true,
            "max_iterations": 200
          },
          {
            "specification_id": "ets-aaa",
            "family": "ets",
            "error": "add",
            "trend": "add",
            "seasonal": "add",
            "seasonal_periods": 24
          }
        ]
      }
    }
  }
}
```

`histdatacom.exponential-smoothing.v1` consumes only the regular grid and
rolling-origin folds produced by the model-input contract. Explicit
specifications cover simple exponential smoothing, Holt trend, damped Holt,
additive or multiplicative Holt-Winters, and Statsmodels ETS error/trend/
seasonal combinations. Initialization, smoothing parameters, optimizer method,
iteration limit, and parameter bounds are configurable; multiplicative forms
reject non-positive transformed training segments. There is no automatic
configuration search or automatic winner.

Expected closures and unexpected missing bins stay distinct and null. A fit
uses only the trailing contiguous observed grid segment at an origin; neither
kind of gap is forward-filled or removed from elapsed horizon time. Configured
level, log-level, return, log-return, ordinary differencing, and seasonal
differencing policies come from the input contract. Forecasts and metrics are
inverted to the original scale, while warm-up, invalid-domain, insufficient
seasonal-cycle, skipped-target, convergence-warning, timeout, numerical, and
dependency limitations remain bounded advisory metadata.

Each explicit model is evaluated on the same configured folds and horizons as
the naive/random-walk, rolling mean, rolling median, and session-seasonal
references. Reports expose per-fold forecasts and errors, per-horizon aggregate
metrics, convergence and failure counts, deterministic model IDs, backend
version, fitted scalar parameters, and enforced fit-attempt/time/memory/
retention limits. Fitted objects, residual vectors, backend exception text, and
wall-clock measurements are never serialized.

`project_exponential_smoothing_onto_training_frame(...)` adds registered
nullable scalar `cm_ets_*` columns to the same enriched rows using
`series_id`/`period`/`row_id`. Forecasts appear only when the origin bin has
closed. Actuals and realized errors appear only at their later diagnostic
availability row and carry explicit diagnostic-only and training-eligibility
flags. The configured projection has bounded width, preserves observed and
`synth_*` namespaces, survives duplicate or masked timestamps, and flows through
the same Polars cache and Influx point. Full reports use
`time_series_fingerprint_exponential_smoothing_summary`, bounded payloads use
`fingerprint_exponential_smoothing`, and console output renders
`Exponential-smoothing models`.

The second fitted family adds explicit-order AR, ARMA, and ARIMA models:

```json
{
  "rules": {
    "fingerprint.series": {
      "classical_model_input": {
        "enabled": true,
        "frequency_ms": 60000,
        "transform": "level",
        "horizons": [1, 5],
        "minimum_training_observations": 40,
        "step_size": 20
      },
      "autoregressive": {
        "enabled": true,
        "projection_specification_ids": ["ar-2", "arma-1-1", "arima-1-1-1"],
        "projection_horizon": 1,
        "compare_exponential_smoothing": true,
        "specifications": [
          {"specification_id": "ar-2", "family": "ar", "p": 2, "trend": "c"},
          {"specification_id": "arma-1-1", "family": "arma", "p": 1, "q": 1, "trend": "c"},
          {"specification_id": "arima-1-1-1", "family": "arima", "p": 1, "d": 1, "q": 1}
        ]
      }
    }
  }
}
```

AR and ARMA are first-class configured families even though Statsmodels uses a
shared ARIMA backend. Orders, trend policy, initialization, estimation method,
stationarity/invertibility enforcement, fixed scalar parameters, and iteration
limits are explicit. Integrated model differencing (`d`) is distinct from the
input contract's configured transform/differencing; fingerprint stationarity
recommendations are advisory and are never applied automatically. Missing bins
reset fitting to the trailing contiguous observed segment and are never filled.

Every model is refit independently at each rolling origin, preventing residual,
state, fitted-value, or future-value leakage. Forecasts and errors are returned
on the original requested scale. Per-horizon MAE/RMSE/bias, forecast coverage,
convergence/failure rates, fitted-parameter stability, roots, conditioning, and
comparisons with lightweight and available exponential-smoothing references are
bounded report diagnostics. There is no automatic order search or winner.

`project_autoregressive_onto_training_frame(...)` adds fixed-width nullable
scalar columns under `cm_ar_*`, `cm_arma_*`, and `cm_arima_*`, joined only by
`series_id`/`period`/`row_id`. Forecast availability, realized diagnostic
availability, fit/reason codes, orders, roots, and training eligibility remain
point-in-time explicit. Full reports use
`time_series_fingerprint_autoregressive_summary`, bounded payloads use
`fingerprint_autoregressive`, and console output renders `Autoregressive
models`. Computational cost is proportional to configured specifications times
rolling origins; #421 candidate, fit-attempt, observation, time, memory, and
diagnostic limits bound that work.

The third fitted family adds explicit seasonal and exogenous autoregressive
models. It uses the same regular grid, transforms, chronological folds,
original-scale inversion, and resource policy as the earlier families:

```json
{
  "rules": {
    "fingerprint.series": {
      "classical_model_input": {
        "enabled": true,
        "frequency_ms": 60000,
        "horizons": [1, 5],
        "minimum_training_observations": 80,
        "step_size": 20
      },
      "seasonal_exogenous": {
        "enabled": true,
        "projection_specification_ids": ["sarima-hour", "arimax-clock", "sarimax-hour-clock"],
        "projection_horizon": 1,
        "regressor_profile": {
          "allow_partial_calendar": true,
          "require_complete_calendar_for": [],
          "max_regressors": 16
        },
        "specifications": [
          {
            "specification_id": "sarima-hour",
            "family": "sarima",
            "p": 1,
            "seasonal_p": 1,
            "seasonal_period": 60,
            "seasonal_cycle_ms": 3600000
          },
          {
            "specification_id": "arimax-clock",
            "family": "arimax",
            "p": 1,
            "regressor_names": ["source_hour_sin", "source_hour_cos"]
          },
          {
            "specification_id": "sarimax-hour-clock",
            "family": "sarimax",
            "p": 1,
            "seasonal_p": 1,
            "seasonal_period": 60,
            "seasonal_cycle_ms": 3600000,
            "regressor_names": ["source_hour_sin", "source_hour_cos"]
          }
        ]
      }
    }
  }
}
```

SARIMA, ARIMAX, and SARIMAX remain separate configured families even though
they share Statsmodels' state-space SARIMAX backend. Nonseasonal and seasonal
orders, the seasonal period and its elapsed-millisecond cycle, trend,
initialization, optimizer, stationarity/invertibility enforcement, fixed scalar
parameters, and iteration limits are explicit. A runtime cycle check prevents a
configuration written for one sampling frequency from being silently reused at
another frequency. There is no automatic order, regressor, or winner selection.

Exogenous columns come only from the deterministic calendar classifier. The
stable vocabulary covers source-hour and weekday cycles, market/session states,
session overlaps, rollover, Sunday open, Friday close, London fix, month/quarter/
year end, holiday/event presence, and explicitly registered `tag:*` values.
Column order, vocabulary, forecast-time availability, missingness, calendar
profile completeness, and provenance are recorded. Unknown regressors and
observed or future-derived market values are rejected. Holiday/event regressors
may be marked partial under the bundled advisory calendar, or required to have
a complete operator-supplied calendar profile.

Each model is refit independently at every #421 rolling origin. Future calendar
values are classified from future grid timestamps without reading future quote
values; missing bins and expected closures remain null and reset fitting to a
trailing contiguous segment. Reports contain bounded parameters, residual
summaries, roots, conditioning, convergence/failure rates, horizon metrics,
regime-conditioned errors, lightweight baseline references, and optional #422/
#423 references using descriptive shared-fold semantics only.

`project_seasonal_exogenous_onto_training_frame(...)` augments the same enriched
tick frame with 123 registered nullable scalar columns under `cm_sarima_*`,
`cm_arimax_*`, and `cm_sarimax_*`. These include order and regressor codes,
origin/target/fold/horizon identity, forecast availability, convergence and
reason codes, and separately gated realized diagnostics. They preserve observed
and `synth_*` namespaces and flow through the existing Polars cache and Influx
projection. Full reports use
`time_series_fingerprint_seasonal_exogenous_summary`, bounded payloads use
`fingerprint_seasonal_exogenous`, and console output renders `Seasonal and
exogenous models`.

The fourth fitted family adds explicit structural state-space models and
leakage-safe Kalman state diagnostics. It consumes the same #421 regular grid,
aggregation, transform, rolling-origin folds, horizons, and resource policy:

```json
{
  "rules": {
    "fingerprint.series": {
      "classical_model_input": {
        "enabled": true,
        "frequency_ms": 60000,
        "horizons": [1, 5],
        "minimum_training_observations": 80,
        "step_size": 20
      },
      "state_space": {
        "enabled": true,
        "projection_specification_id": "local-level",
        "projection_horizon": 1,
        "max_state_dimension": 64,
        "max_component_count": 8,
        "max_prediction_only_gap": 240,
        "max_retained_states": 16,
        "specifications": [
          {
            "specification_id": "local-level",
            "family": "local_level"
          },
          {
            "specification_id": "local-linear-trend",
            "family": "local_linear_trend",
            "stochastic_trend": true
          },
          {
            "specification_id": "structural-hourly",
            "family": "structural",
            "seasonal_period": 60,
            "seasonal_cycle_ms": 3600000
          }
        ]
      }
    }
  }
}
```

Local level, local linear trend, and configurable structural models remain
first-class specifications. Level/trend/irregular, seasonal, cycle, and
autoregressive components; stochastic flags; initialization; optimizer; fixed
parameters; and iteration limits are explicit. There is no automatic component
search or winner. Seasonal periods carry an elapsed-millisecond cycle so a
configuration cannot silently move to a different sampling frequency.

Missing regular-grid observations are passed to the Statsmodels
`UnobservedComponents` backend as missing observations. The Kalman system
performs one prediction-only transition per grid step: expected closures and
true missing observations remain distinct in the #421 input contract, neither
is forward-filled, and long prediction-only runs are bounded and reported.
Each model is independently refit at every forecast origin. Filtered states use
only that origin's training segment. Smoothed states are retrospective
diagnostics for the same bounded segment, are never used to forecast, and are
never training-eligible.

Reports include bounded likelihood/AIC/BIC, innovations, state uncertainty,
parameters, convergence/failure rates, prediction-only transition counts,
horizon metrics, lightweight baselines, and optional #422/#423/#424 references.
Comparisons are descriptive shared-fold references only. Full reports use
`time_series_fingerprint_state_space_summary`, bounded payloads use
`fingerprint_state_space`, and console output renders `State-space and Kalman
models`.

`project_state_space_onto_training_frame(...)` augments the same enriched tick
rows with 52 registered nullable scalar columns: 32 under `cm_state_space_*`
and 20 under `cm_kalman_*`. Forecast, actual/error, fit/reason, fold, horizon,
filtered-state, smoothed-state, uncertainty, availability, retrospective, and
training-eligibility fields join only by `series_id`/`period`/`row_id`. They
preserve observed and `synth_*` namespaces and serialize through the existing
Polars cache and Influx projection.

The fifth fitted family provides explicit symmetric ARCH(q) and GARCH(p,q)
conditional-variance models through the optional `arch` backend. It requires a
return-bearing #421 input contract (`transform: return` or `log_return`) and
keeps input definition, mean model, innovation and variance orders,
distribution, scaling, variance initialization, covariance type, parameter
bounds, and iteration limits explicit:

```json
{
  "rules": {
    "fingerprint.series": {
      "classical_model_input": {
        "enabled": true,
        "transform": "return",
        "horizons": [1, 5],
        "minimum_training_observations": 80
      },
      "volatility": {
        "enabled": true,
        "projection_specification_ids": ["arch-5", "garch-1-1"],
        "projection_horizon": 1,
        "realized_variance_proxy": "squared_return",
        "annualization_periods": 252,
        "specifications": [
          {
            "specification_id": "arch-5",
            "family": "arch",
            "input_definition": "raw_return",
            "mean_model": "zero",
            "distribution": "normal",
            "innovation_order": 5
          },
          {
            "specification_id": "garch-1-1",
            "family": "garch",
            "input_definition": "raw_return",
            "mean_model": "constant",
            "distribution": "students_t",
            "innovation_order": 1,
            "variance_order": 1
          }
        ]
      }
    }
  }
}
```

Raw returns, log returns, per-origin demeaned returns, and explicitly referenced
preceding mean-model residuals are separate contracts. Missing grid bins are
never filled: fitting uses only the trailing contiguous segment and records the
reset count. Fits guard finite inputs, minimum history, scaling, positive
variance, parameter bounds, persistence, unconditional variance, optimizer
status, and numerical failures with stable reason codes and without backend
exception text.

Forecast evaluation keeps return-mean, conditional-variance, and volatility
errors separate. Realized variance currently uses the explicit deterministic
`squared_return` proxy; rolling-variance and EWMA references use the same folds.
Multiple horizons, rolling stability, convergence/failure rates, bounded
standardized-residual diagnostics, and preceding-model references are reported,
but there is no automatic winner. GJR-GARCH and EGARCH have registry entries for
future extension and are not silently fitted by this family.

`project_volatility_onto_training_frame(...)` augments the same enriched tick
rows with 78 registered nullable scalar columns under `cm_arch_*` and
`cm_garch_*`. Forecasts are attached at origin availability; actual returns and
realized diagnostics are attached only after target availability. Durable
`series_id`/`period`/`row_id` joins preserve observed and `synth_*` fields and
round-trip through the existing Polars cache and Influx projection. Full reports
use `time_series_fingerprint_volatility_summary`, bounded payloads use
`fingerprint_volatility`, and console output renders `ARCH and GARCH volatility
models`.

The opt-in family-neutral comparison layer consumes those saved bounded
evaluation artifacts; it does not refit models. Enable it with
`fingerprint.series.classical_model_comparison.enabled: true` after enabling the
model-input contract and the families to compare. Each record carries the
dataset/fingerprint, regularization contract, fold set, target metric, scale,
horizon, period, specification, and explicit reference baseline. A comparison
is ineligible when any compatible-identity requirement differs or when bounded
fold evidence is incomplete. Mean/level, return-mean, conditional-variance, and
absolute-return-volatility metrics remain separate.

Skill is descriptive and reference-relative: ratio reduction for MAE/RMSE/bias
and baseline-minus-model for QLIKE. Negative skill is preserved. Missing or
near-zero references produce stable reason codes instead of silently changing
the baseline. Rolling error and parameter drift, convergence and failure rates,
regime context, resource-limit terminations, and representative reason counts
are bounded. Failed fits remain in accounting denominators. No `winner`,
`best_model`, production recommendation, automatic order search, or
hyperparameter search is emitted.

`project_classical_model_comparison_onto_training_frame(...)` adds 43 nullable
diagnostic scalars under `cm_comparison_*`, `cm_skill_*`, and `cm_stability_*`.
They join by `series_id`/`period`/`row_id`, remain null before target-time
availability, preserve duplicate timestamps and observed/`synth_*` columns, and
are explicitly retrospective and not training-eligible. Full reports use
`time_series_fingerprint_classical_model_comparison_summary`, bounded payloads
use `fingerprint_classical_model_comparison`, and console output renders
`Classical model comparison`.

Every series fingerprint also includes a bounded `fingerprint_audit` section.
It records expected, emitted, and intentionally skipped fingerprint sections,
stable skip/eligibility reason codes, calendar-profile completeness, tick-spread
conditioning eligibility, dynamics readiness, stationarity readiness, and
decomposition readiness. This is machine-readable contract metadata for report
consumers; the full fingerprint sections remain the source of the detailed
statistics.

Topology attention targets include bounded `inspection_context` when a mapped
remediation action has focused evidence. Invalid timestamps expose row positions
and parse-failure counts; non-monotonic timestamps expose offending transitions;
exact duplicate rows expose their timestamp values and occurrence counts;
suspicious gaps expose largest boundaries, durations, and expected-session
classification; and weekend activity exposes timestamp/session buckets. These
records do not include raw quote rows or absolute paths. Each context section
reports included, omitted, and truncated counts with full `limit_metadata`.
Expected session closures remain non-actionable context and only accompany a
suspicious-gap drill-down. Set
`fingerprint.series.topology_inspection_sample_limit` in a quality profile from
`0` through `5` to control the per-section sample count.

Quality JSON reports and CLI summaries also include bounded regime and
readiness summaries when fingerprint findings are present. Use
`time_series_fingerprint_regime_summary` to scan dominant session states, active
sessions, special/holiday/event tags, source hour/day coverage, calendar-profile
source/version/completeness/advisory state, and tick conditioned spread by active
session or special tag. Use `time_series_fingerprint_readiness_summary` to scan
whether return or microstructure dynamics are valid, limited, skipped, or
unavailable; which topology limitations affect sequence interpretation; and the
compact return, jump, flatline, spread, stale quote, burst, and one-sided
movement facts. The same readiness summary also includes bounded dependence
status, ACF basis, configured lag coverage, computed/skipped lag counts,
skipped-lag reason counts, and per-series sample counts. It also includes
stationarity status, calculation basis, sample counts, configured rolling
windows, computed/skipped window counts, skipped-window reasons, rounding
policy, zero-variance markers, and recommended transforms. The readiness surface
also carries decomposition basis, sample/window coverage,
stationarity dependency status, trend direction, structural-break candidate
counts, limitations, and its training-projection contract. Use
`time_series_fingerprint_readiness_risk` when you need a bounded, deterministic
triage list of targets and sections most likely to block downstream fingerprint
use. It ranks existing readiness, topology, dependence, regime, cache-source,
and report-surface evidence into stable reason codes such as
`invalid_timestamps_skipped`, `duplicate_timestamps`, `suspicious_gaps`,
`skipped_dependence_lags`, `skipped_rolling_windows`,
`insufficient_sample_count`, `zero_variance`, `unsupported_timeframe`, and
`not_emitted`. Use the raw
`time_series_fingerprint` payload when downstream tooling needs complete
fingerprint sections, full quantile maps, full conditioned distributions, or full
ACF lag maps.

Rank fingerprint readiness risks from saved quality reports:

```sh
histdatacom quality fingerprint-readiness --report reports/quality.json --json
```

The command reads report JSON only; it does not rescan market data. Use
`--target-limit`, `--section-limit`, and `--reason-limit` to control the bounded
machine JSON and matching human output.

Add `--next-work` when the question is which fingerprint product gap to address
next rather than which targets are risky:

```sh
histdatacom quality fingerprint-readiness \
  --report reports/quality.json \
  --next-work \
  --alternate-limit 2 \
  --json
```

The `histdatacom.fingerprint-next-work.v1` result combines already-saved
readiness-risk evidence with the current implemented/planned fingerprint
registry. It emits one recommendation, bounded alternates, publish-safe input
report identities, representative target axes, reason codes, known
prerequisites and downstream consumers, confidence/basis metadata, and
issue-ready acceptance-criteria suggestions. Existing section-readiness and
report-surface gaps rank ahead of later planned capabilities. Use the ordinary
readiness-risk output for target-level diagnosis; use `--next-work` for the
bounded cross-report product recommendation.

The recommendation basis also records whether the saved evidence confirms the
enriched single-row ASCII tick training substrate, legacy-cache projections,
durable row-identity columns, duplicate timestamps, unequal cross-series ranges,
and triangle comparisons. `ascii/T` remains the only base grain; legacy M1
targets are counted as ignored non-base evidence and cannot become a platform
or M1 implementation recommendation. The command never rescans market data,
changes quality status, or creates, edits, closes, or ranks GitHub issues. Issue
references are static capability metadata only. `--target-limit` also bounds
representative recommendation axes, while `--alternate-limit` bounds alternate
recommendations; both emit explicit truncation metadata.

Bounded report and fingerprint summary payloads include `limit_metadata` and
expanded `payload_limits` entries with requested, default, effective, minimum,
maximum, and unbounded limit fields. The legacy `limit` field remains present
and represents the effective limit applied to the emitted rows.

Discover the active fingerprint contract without scanning target data:

```sh
histdatacom quality fingerprint-schema --json
```

Use `histdatacom quality fingerprint-schema` for a concise human-readable
summary, or add `--quality-profile profiles/strict-ci.json` to reflect
profile-overridden fingerprint knobs such as quantiles, lags, rolling windows,
histogram bins, max rows, rounding, topology-inspection samples, and
distribution-attention thresholds. This
discovery command is for downstream parsers, validators, and schema review: it
lists schema versions, metadata keys, target capabilities, implemented/planned
sections, basis/status/reason vocabularies, and publish-safe example fragments.
It does not read local datasets or generate fingerprints; run
`histdatacom --quality --quality-checks fingerprint` when you need real target
fingerprint payloads.

Fingerprint discovery is backed by the shared data-quality fingerprint contract
registry, not by a separate hand-maintained copy in the discovery command. When
new fingerprint sections, schema versions, report metadata keys, bounded payload
keys, basis values, or status/reason vocabularies are added, update that registry
first; the CLI/API discovery payload and drift tests should then follow the same
contract surface.

Run the market-data-free contract self-audit when changing fingerprint schema,
registry, generated report, or example surfaces:

```sh
histdatacom quality fingerprint-schema --verify --json
```

`--verify` emits `histdatacom.time-series-fingerprint-contract-audit.v1` with
pass/fail status, deterministic checks, a bounded
`histdatacom.time-series-fingerprint-report-surface-evidence.v1` matrix, and
drift findings for missing schemas, orphan report surfaces, stale payload keys,
implemented/planned section mismatches, profile-default drift, vocabulary drift,
publish-safe example drift, and missing generated report surfaces. The
representative matrix proves that coverage, topology, topology attention,
distribution, distribution attention, regime, readiness, and readiness-risk
summaries are wired through full report metadata, bounded payload keys, and
CLI/report summary headings such as `Fingerprint regimes`. It does not read
local market data, run
quality rules, or automate GitHub/CI/release workflow. Cache-scale
`--quality-preflight` runs the same contract audit automatically and fails its
readiness decision when the audit reports contract errors.

The human `histdatacom quality fingerprint-schema --verify` output and
quality-preflight Markdown report include a bounded report-surface evidence
table, so operators can see the representative surface key, summary schema key,
full-report metadata state, bounded-payload state, CLI/report heading state, and
intentional CLI absence reason without inspecting nested JSON.

Run the bounded report-payload contract self-audit when changing report
summaries, bounded payloads, next actions, remediation coverage, remediation
catalog audits, or fingerprint summary surfaces:

```sh
histdatacom quality bounded-payload-contract --json
```

This emits `histdatacom.bounded-payload-contract-audit.v1`. The audit generates
a representative quality report through the application serializer, then checks
that bounded payload metadata exposes coherent requested/default/effective limit
semantics, counts, omitted counts, and truncation state. Cache-scale
`--quality-preflight` runs this bounded-payload audit automatically and fails its
readiness decision when generated report payload metadata drifts.

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

Profiles can also enable report-publication surfaces. To include a bounded
remediation-catalog audit in normal quality reports, bounded orchestration
payloads, and quality preflight sample evidence, opt in with:

```json
{
  "schema_version": "histdatacom.quality-profile.v1",
  "name": "reporting-with-catalog-audit",
  "reporting": {
    "remediation_catalog_audit": {
      "enabled": true
    }
  }
}
```

The embedded audit reuses the standalone remediation-catalog audit schema,
keeps known source-code coverage separate from observed report coverage, and
remains advisory; it does not change finding severities or quality exit policy.
The audit also records how each static finding was attributed to a rule:
`exact` for an explicit literal, constant, or class rule; `inferred` for a
single-rule helper chain, local rule object, module rule, or unambiguous
finding-code prefix; and `unresolved` when multiple rule callers remain
possible. Unresolved entries retain the source-family fallback and include a
stable `attribution_reason` such as `ambiguous_helper_rules`. Bounded source
family, helper, and finding-prefix counts make the remaining ambiguity
auditable without executing quality rules or reading market data.

Inspect that attribution directly through the standalone command:

```sh
histdatacom quality remediation-catalog --json
```

The concise renderer includes exact, inferred, and unresolved occurrence
counts plus attribution status and reason on ranked gaps. These fields improve
catalog planning evidence only; they do not add remediation mappings or change
gap severity. Ranked gaps also include `actionability` and
`actionability_reason`. Actionable defects sort ahead of policy/profile
decisions, unsupported formats or capabilities, expected context, attribution
or diagnostic blockers, and unsafe-to-automate cases. The summary preserves the
ordinary mapped/unmapped counts and adds boundary-aware actionable, intentional,
attribution-blocked, and diagnostic-blocked counts. Unknown warning/error gaps
remain actionable by default, so boundary classification cannot silently hide a
new defect.

Every audit also derives a bounded `remediation_plan` from the complete ranked
gap set. Plan items are re-ranked by deterministic fixability rather than raw
severity alone and include the original catalog-gap rank, actionability,
severity counts, exact-or-family selector proposal, draft hint-code slug,
suggested action kind, fixability score/level/confidence with a reason trail,
fields still requiring maintainer judgment, and bounded source/report evidence.
Observed report-only gaps use their reported rule/finding identity and remain
first-class plan candidates. Attribution and diagnostic blockers are explicitly
marked `blocked`; policy, support, expected-context, unsafe, and informational
boundaries remain visible with low fixability instead of being presented as
automatic catalog edits. The `histdatacom.quality-remediation-plan.v1` artifact
is advisory: it never edits the remediation catalog, creates GitHub work, or
changes finding severity and exit policy.

To translate findings in a saved quality report into concrete user-data repair
steps, use the separate non-mutating repair-plan command:

```sh
histdatacom quality repair-plan \
  --report reports/quality.json
```

Add `--json` for the bounded `histdatacom.quality-repair-plan.v1` artifact.
`--item-limit` controls included findings and `--evidence-limit` controls the
publish-safe diagnostic values retained per item; both surfaces include total,
included, omitted, and truncation metadata. The initial operation vocabulary
covers invalid archive/member renames, missing or unexpected member rebuilds,
extra-member inspection, CRC/corrupt archive replacement, and read-access
restoration. Exact report evidence produces an exact proposal, incomplete
evidence produces `needs_context`, and unmapped or out-of-scope findings remain
explicitly `unsupported`.

The repair plan is advisory and manual-only. It does not expose an `--apply`
mode and never renames files, rewrites ZIPs, removes members, changes
permissions, downloads replacements, or changes report severity and exit
policy. This is distinct from remediation-catalog `remediation_plan` output:
the catalog plan helps maintainers add missing hint mappings, while
`quality repair-plan` helps users interpret already observed findings and
mapped hints without changing their data.

The same reporting surface can be enabled without a profile file by passing
`--quality-remediation-catalog-audit` with `--quality`, `--repo-quality`, or
`--quality-preflight`. When the flag is combined with `--quality-profile`, the
profile still supplies thresholds, severities, and modeling assumptions; the
CLI flag only sets `reporting.remediation_catalog_audit.enabled` to `true`.

Preview the fully resolved profile before a run with
`--quality-profile-preview`. JSON remains the default output for automation:

```sh
histdatacom --quality \
  --quality-profile profiles/strict-ci.json \
  --quality-remediation-catalog-audit \
  --quality-profile-preview
```

For operator review, choose a bounded readable renderer:

```sh
histdatacom --quality \
  --quality-profile profiles/strict-ci.json \
  --quality-remediation-catalog-audit \
  --quality-profile-preview \
  --quality-profile-preview-format text
```

Use `--quality-profile-preview-format markdown` when the explanation should be
pasted into an issue, PR, or runbook. Keep the default stdout behavior for
quick inspection, or write the selected rendering to a standalone artifact path:

```sh
histdatacom --quality \
  --quality-profile profiles/strict-ci.json \
  --quality-profile-preview \
  --quality-profile-preview-format markdown \
  --quality-profile-preview-output reports/quality-profile-preview.md
```

For preflight evidence, prefer the preflight-attached form so the JSON and
Markdown preflight reports record the preview artifact metadata:

```sh
histdatacom --quality-preflight \
  --quality-target data \
  --quality-profile profiles/strict-ci.json \
  --quality-preflight-report reports/preflight.json \
  --quality-preflight-profile-preview-output reports/quality-profile-preview.md \
  --quality-preflight-profile-preview-format markdown
```

Preflight-attached artifacts are recorded under `evidence.artifacts` with a
publish-safe path, format, schema version, SHA-256 digest, and byte size. The
profile preview remains mirrored at `evidence.quality_profile_preview` for
compatibility with existing reports and runbooks.

Preview artifact parent directories are created automatically. Use
`--quality-profile-preview-output -` only when stdout is the intended artifact
stream.

The preview exits before target discovery, quality checks, report writes, repo
metadata writes, or orchestration submit. The JSON payload remains
deterministic and includes the active profile source, source path, configured
rule IDs, configured modeling assumptions, reporting keys, and the resolved
`reporting.remediation_catalog_audit.enabled` value after CLI overrides. It
also includes a `profile_explanation` section with input channels such as
built-in defaults, named profiles, YAML config, profile files, API options, and
CLI overrides; per-value source rows; and a bounded effective diff from the
built-in default profile. Resolution preserves those facts before the profile
is normalized, so an override row records its previous source and value instead
of reconstructing them from the final JSON. The `text` and `markdown` renderers
are presentation layers over that same explanation data.

Python callers that need the same first-class contract can use
`resolve_quality_profile()`, `load_quality_profile_file_resolution()`, and
`apply_quality_profile_overrides()` from `histdatacom.data_quality`. The
returned `QualityProfileResolution.profile` remains the normal validated
`QualityProfile`; `value_sources`, `input_channels`, and `to_payload()` expose
the deterministic provenance contract. Existing `quality_profile_from_*()` and
`load_quality_profile_file()` callers continue to receive `QualityProfile`
directly.

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
| `ascii` | `T` | Deep parser-level checks for ZIP, CSV, and canonical `.data` cache artifacts |

Retired formats and timeframes emit `HISTDATA_FORMAT_UNSUPPORTED` when they are
encountered as direct CSV inputs and fail ZIP inventory naming checks when they
arrive as unsupported archive/member names.

HistData-specific assumptions are reported directly in findings:

- ASCII tick rows include bid and ask values.
- HistData timestamps are interpreted as fixed EST with no daylight-saving
  adjustment and normalized to UTC.
- Tick `volume` is not treated as automatically meaningful or required for
  market-quality decisions.

#### Clean and Failing Examples

A focused ingestion run against a clean tick CSV reports a clean file and writes a
machine-readable report:

```sh
histdatacom --quality \
  --quality-target data/DAT_ASCII_EURUSD_T_201202.csv \
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
- csv: /path/to/data/DAT_ASCII_EURUSD_T_201202.csv (findings=1, warnings=0, errors=0)

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

A malformed tick CSV fails ingestion and exits nonzero by default because
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
- csv: /path/to/data/bad/DAT_ASCII_EURUSD_T_201202_BAD.csv (findings=2, warnings=0, errors=1)
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
  violations, parse failures, invalid timestamps, or negative spreads.

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

#### Point-in-Time Market Context

The `histdatacom.market_context` domain stores approved macro, central-bank,
news, and shock evidence as immutable versioned timelines rather than repeated
tick columns. Every event vintage retains source/version and retrieval
metadata, content hashes, licensing and redistribution constraints, affected
currencies/symbols, confidence, limitations, normalized source time, explicit
pre/post windows, and revision lineage.

Ex-ante queries require an as-of time and cannot expose schedules, actuals, or
revisions before the exact vintage was available. Ex-post queries retain all
vintages. Bounded window joins return compact context/calendar sidecars over
`ReconstructionWindowV1`; they never persist the full analytical enrichment
frame. Missing, incomplete, and out-of-coverage context remain explicit rather
than becoming invented event labels.

Calendar sidecars reuse the existing session, holiday, rollover, fix, and
month/quarter/year-end classifier. The shared source-adapter seam retains
provenance and licensing but does not authorize or scrape a paid news corpus.
The production corpus uses documented official ONS, ECB, Bank of England, and
Federal Reserve sources plus a small cited operator shock catalog:

```bash
histdatacom analytics market-context-corpus \
  --artifact-dir .histdatacom/market-context \
  --start-date 2002-03-01 \
  --end-date 2026-06-30
```

The command writes immutable content-addressed raw snapshots, a directly
loadable timeline, and a self-contained corpus with source hashes, licenses,
coverage/missingness, duplicate counts, runtime, and peak memory. Installed
helpers replay the raw snapshots, refuse unsupported reconstruction context,
return the carving query contract, and project bounded benchmark event state.

See [`docs/market-context-contracts.md`](docs/market-context-contracts.md) for
the source selection, licenses, artifacts, replay, coverage/preflight,
timezone and revision rules, information-audit integration, streaming limits,
and trust gates.

#### CFTC positioning state

CFTC Commitments of Traders is a separate persistent weekly positioning
sidecar, not a `MarketContextEventV1` window and not a repeated tick column.
The installed campaign retains Legacy and TFF, futures-only and combined,
EUR/GBP/EURGBP contract identities, official release/correction evidence,
compressed-history consistency, immutable refresh diffs, and fail-closed
ex-ante semantics:

```bash
histdatacom analytics cftc-positioning-corpus \
  --artifact-dir data/.histdatacom/analytics/cftc-positioning \
  --start-date 2002-03-01 \
  --end-date 2026-06-30
```

Window queries expose bounded latest-known snapshots, age, mapping status, and
point-in-time-safe net/open-interest/change/rolling features. Current PRE rows
cannot masquerade as original vintages; nominal publication estimates fail
strict ex-ante use. Companion receipts bind the query into the information
audit, benchmark, motif selection, planning, and carving without changing
their immutable v1 schemas.

See [`docs/cftc-positioning-contracts.md`](docs/cftc-positioning-contracts.md)
for source selection and acknowledgement, field/family/scope mappings, quote
direction, publication/restatement rules, artifact replay/diff behavior,
coverage, resource limits, consumer seams, and explicit nonclaims.

#### Feed-Regime Detection

`histdatacom analytics feed-regimes` projects canonical ASCII tick fingerprints
into a versioned feed-epoch definition. Epochs represent evidence-backed
changes in the technological observation process, not calendar eras or market
regimes. Boundaries include uncertainty intervals and deterministic stability
evidence under sampling, missing-period, and feature-removal perturbations.

```sh
histdatacom analytics feed-regimes \
  --target data/ASCII/T/eurusd \
  --bucket month \
  --report reports/eurusd-feed-regimes.json \
  --epoch-artifact reports/eurusd-feed-epochs.v1.json
```

Use `--json` to print the full machine-readable payload to stdout:

```sh
histdatacom analytics feed-regimes --target data/ --json
```

Only stability-passing definitions are valid downstream observation-model
inputs. Periods inside a boundary uncertainty interval are assigned to an
explicit transition instead of being forced into either neighboring epoch. The
artifact records every fingerprint, source, feature-provenance, conditioning,
quality, and config hash needed for replay.

Feed-epoch fitting is a bounded control-plane operation. Streaming
reconstruction references the definition ID and carries only compact epoch or
transition assignments; it does not persist fingerprint panels or the wide
analytical frame per tick. See
[`docs/feed-epoch-contracts.md`](docs/feed-epoch-contracts.md) for the schema,
trust gate, resource limits, and streaming integration.

For the real three-symbol technology-epoch fit, v2 scans monthly Arrow caches
column-wise, uses explicit calendar/open/active-time denominators, and applies
robust multivariate PELT plus family-specific holdouts:

```sh
histdatacom analytics feed-epochs-v2 \
  --target data/ASCII/T/eurusd data/ASCII/T/gbpusd data/ASCII/T/eurgbp \
  --artifact-dir data/.histdatacom/feed-epochs-v2
```

The command writes separate compact definition, bounded evidence, and runtime
artifacts. It does not create an augmented cache or claim that a detected
boundary is a market regime, recovered quote, vendor cause, or broker profile.

#### Historical Feed-Observation Operators

`ObservationOperatorV1` turns a bounded market-event surface into a sparse,
quantized delivery-observation surface using stability-passing feed epochs.
The operator supports conditioned thinning, unchanged-quote filtering,
timestamp and price quantization, batching, duplicates, burst/rate caps,
outages, and reconnect behavior through versioned parameters with explicit
support and uncertainty.

The bare fitting boundary consumes canonical feed-epoch projections or paired
controlled-calibration evidence. Canonical sparse history does not identify a
true dense-event denominator, so unsupported thinning parameters remain
visibly unsupported and use a neutral identity behavior rather than being
presented as direct observations. Sparse conditioned strata follow a fixed
state/session/event-to-global fallback hierarchy or fail closed.

`ObservationCalibrationCampaignV2` adds the real-evidence trust boundary. It
fits relative active-time retention and supported delivery mechanisms by
symbol, technology epoch, update type, and session, then applies the operator
to dense reference caches in chronological calibration, validation, and final
holdout blocks. Every parameter carries support, uncertainty, source hashes,
and an identifiability or refusal reason. Calendar closure, archive gaps,
unchanged filtering, batching, quantization, duplicates, outages, and reconnect
behavior remain distinct diagnostics.

```sh
histdatacom analytics observation-calibrate-v2 \
  --definition data/.histdatacom/feed-epochs-v2/feed-epochs-v2-definition.json \
  --evidence data/.histdatacom/feed-epochs-v2/feed-epochs-v2-evidence.json \
  --artifact-dir data/.histdatacom/observation-calibration-v2
```

The campaign cannot become application-ready when retention is merely identity
because the dense denominator is unknown, when a default required mechanism is
unsupported, or when a final holdout fails. Requesting an optional unsupported
mechanism also fails closed. Dense and degraded window rows stay process-local;
the persisted campaign contains aggregate evidence and the compact replayable
operator only.

Observation rendering does not mutate `SyntheticEventV1`. Inputs retain their
market-event IDs and produce separate operator-lineaged delivery observations.
Forward application preserves protected historical anchors exactly; the
separate `degrade()` interface lets #436 degrade modern holdouts while
protecting only explicitly selected controls.

Application uses `ReconstructionWindowV1`, aligned timestamp/batch quanta,
declared halo metadata, required bounded carry state after the source window,
deterministic hash decisions, and input/output amplification limits. The
compact operator JSON is durable and hash-replayable; fit panels and window
output observations remain bounded intermediates rather than augmented
permanent cache columns. See
[`docs/observation-operator-contracts.md`](docs/observation-operator-contracts.md)
for the contracts, trust gates, fallback semantics, and streaming boundary.

#### Reverse-Degradation Benchmark

`ReverseDegradationBenchmarkV1` is the generator-neutral validation harness for
reconstruction work. It streams dense modern reference events through a
versioned historical observation operator, evaluates transparent no-fill,
interpolation, resampling, and existing empirical-overlay controls alongside
candidate generators, and retains only bounded online aggregates.

The immutable benchmark manifest subdivides the existing withheld validation
boundary into ordered validation and final-holdout periods without changing the
upstream information-mode v1 schema. A valid experiment covers multiple feed
epochs and degradation severities, reports symbol/epoch/session/event/sparsity
slices, records uncertainty and ensemble support, and carries cross-series,
strategy, convergence, failure, memory, scratch, and output-cost hooks.

Hard historical-constraint or protected-anchor violations always block
promotion regardless of soft statistical fit. Scorecards compare every method
with no fill but explicitly set `automatic_winner` to false and never emit a
winner candidate. Dense, degraded, reconstructed, and rejected intermediates
remain process-local; only the compact manifest and scorecard are intended to
persist. See
[`docs/reverse-degradation-benchmark-contracts.md`](docs/reverse-degradation-benchmark-contracts.md)
for the complete interfaces, metric semantics, resource bounds, and trust
gates.

The real-data promotion policy is separately frozen and packaged before any
promotable candidate campaign. It distinguishes hard campaign/candidate gates
from visible advisory evidence, fails closed when hard observations are
missing, and never selects an automatic winner. See
[`docs/reverse-degradation-benchmark-corpus.md`](docs/reverse-degradation-benchmark-corpus.md)
for the predeclared thresholds, evidence ordering, provisional motif boundary,
and scientific nonclaims.

The installed `histdatacom analytics reverse-degradation-benchmark-corpus`
command now builds the real EURUSD/GBPUSD/EURGBP Arrow partitions, replays
source and selected-window hashes, executes all declared degradation families,
runs dense/no-fill/interpolation/motif/negative controls, and writes a compact
content-addressed manifest, motif index, leakage audit, resource audit, and
scorecard. Required fitted-operator or replay failures abort the campaign;
dense and holdout event rows remain process-local.

#### Qualified Proposal-Engine Bank

The v2.5 proposal layer registers empirical motif, four event clocks, three
marked Hawkes variants, two regime-Hawkes variants, RMTPP, Add-Thin, and the
constrained Schrödinger bridge as one first-party model bank. Engine identity,
campaign role, and product eligibility are independent. “Challenger” in older
module/document names is a benchmark role, not an optional post-certification
architecture or a permanent product classification.

The public v2 plan explicitly orders engines, binds config/dataset/context/
evidence artifacts, and names the reconstruction selection. Powered
qualification can only reduce legacy eligibility. July's 18-window pilot
correctly returned no decision; the expanded campaign freezes 96 synchronized
windows across separate calibration, validation, and final-holdout periods.
Of the three powered marked-Hawkes variants, diagonal self-excitation and full
self/cross excitation pass all ten hard gates and are eligible for
reconstruction and ensemble use; zero excitation fails time-uniformity. The
final v2 HistData product selection is deliberately narrower and names only
diagonal self-excitation. The other registered engines retain their failed,
underpowered, refused, research-only, or eligible-but-unselected decisions and
cannot silently enter the product. There is no motif fallback and no automatic
winner. Legacy plans without a powered dossier retain their explicit
single-qualified-engine compatibility behavior. See
[`docs/proposal-engine-portfolios.md`](docs/proposal-engine-portfolios.md).

#### Classical Event-Clock Proposal Engines

`histdatacom.synthetic.event_clock` registers non-homogeneous Poisson,
gamma-mixed Cox, exponential ACD(1,1), and two-state hidden Markov
duration/mark engines in the generator-neutral benchmark.
All four use calibration-only versioned fits, deterministic synchronized
generation, explicit epoch/session support and refusal, bounded prior-only
history, hard fit/generation resource limits, and family-specific diagnostics.

Event-clock proposals project into the same structural candidate surface and
historical-carving engine as empirical motifs. The v2 portfolio retains their
audits and evidence but refuses them from product selection; no scorecard
selects a winner automatically. The retained real campaign fitted
all four families with zero generation failures or anchor violations, but none
passed the frozen candidate gates. See
[`docs/classical-event-clock-challengers.md`](docs/classical-event-clock-challengers.md)
for likelihoods, conditioning/backoff semantics, deterministic lineage,
resource bounds, carving integration, retained evidence, and primary sources.

#### Marked Hawkes Proposal Engines

`histdatacom.synthetic.marked_hawkes` adds separate zero-excitation,
self-excitation, and full self/cross-excitation ablations without changing the
fixed #450 event-clock registry. Calibration-only exponential-kernel fits use
explicit epoch/session support, exact bounded-window likelihoods, versioned
approximate uncertainty, source/destination quote-transition marks, and
fail-closed spectral-radius checks.

Generation uses one deterministic bounded Ogata timeline for the synchronized
symbol group. Observed anchors and generated events update intensity strictly
after their timestamps; prior carry is explicit and bounded; proposals remain
inside immutable anchor intervals. Proposal, output, amplification, history,
parameter, memory, and runtime limits refuse the complete attempt without
partial rows. Hawkes batches satisfy the same generator-neutral carving
protocol as empirical and event-clock candidates.

The real reverse-degradation campaign evaluates all three Hawkes ablations
beside the qualified empirical reference and all four classical engines,
but continues to declare `automatic_winner: false`. See
[`docs/marked-hawkes-challenger.md`](docs/marked-hawkes-challenger.md) for fit,
stability, mark, synchronized-generation, lineage, resource, and benchmark
semantics.

#### Regime-Switching Hawkes Proposal Engines

`histdatacom.synthetic.regime_hawkes` registers two two-state MMHP-delta
ablations beyond the static Hawkes comparison: state-specific baseline/mark
behavior with shared excitation, and state-specific baseline/excitation/mark
behavior. The state is shared across the synchronized triangle, canonicalized
as `calm`/`active` by expected activity, and never treated as an observed
economic truth.

Fixed-bin scaled forward-backward inference and bounded, likelihood-monotone
generalized EM keep filtered and smoothed probabilities separate. Low
occupancy, collapsed activity, unsupported transitions, label switching,
instability, structural tampering, or any fit/generation resource violation
fails closed without usable partial state. Events within a bin affect
excitation only from the next bin.

Technological feed epochs remain a separate context axis. Stable epochs bind
their v2 epoch identity; transition windows bind boundary support and
uncertainty periods. The real campaign reloads the corpus-bound feed-epoch
artifact and rejects context mismatch. Candidate batches retain exact
context/anchor digests and use the shared historical-carving protocol.

The retained comparison contains the empirical reference, four event clocks,
three static Hawkes models, and both regime engines, with no automatic winner
or unqualified product selection. See
[`docs/regime-switching-hawkes-challenger.md`](docs/regime-switching-hawkes-challenger.md)
for the approximation, information boundaries, diagnostics, lineage,
resource limits, carving seam, nonclaims, and primary references.

#### Recurrent Marked Temporal Point-Process Engine

`histdatacom.synthetic.neural_tpp` registers one dependency-free CPU RMTPP
after the empirical, classical event-clock, static Hawkes, and regime-Hawkes
comparisons. It uses an explicit start token, deterministic full-batch BPTT,
whole-window train/tune splits, row-free protected-split leakage evidence, an
immutable checkpoint, an exact closed-form intensity compensator, and exact
inverse-CDF event-time sampling over 12 joint symbol/quote-transition marks.

One synchronized recurrent state consumes observed anchors only when their
time is reached. Generated events remain strictly inside immutable
destination-symbol anchor pairs, retain exact state/intensity/mark lineage,
and enter the same generator-neutral historical-carving path. Independent
fit, gradient-work, checkpoint, history, step, amplification, memory, output,
and wall-time limits fail closed without partial parameters or rows.

The retained campaign contains 11 proposal candidates and 15 total reports,
while continuing to declare `automatic_winner: false`; RMTPP remains
benchmark-eligible and is refused from the current product. See
[`docs/neural-tpp-challenger.md`](docs/neural-tpp-challenger.md) for the model,
dataset/leakage boundary, exact likelihood and sampler, checkpoint/replay
contracts, carving seam, nonclaims, and primary references.

#### Marked Add-Thin Sequence Engine

`histdatacom.synthetic.add_thin` registers one dependency-free CPU
point-process diffusion engine. It uses the Add-Thin forward law and exact
B/C/D/E reverse coefficients with a deliberately bounded, non-neural
piecewise-constant time-bin × joint-mark posterior approximation. The fixed
12 marks are a declared project extension; the reference paper models arrival
times and leaves marks to future work.

Whole Asia/London/New York windows split before fit. Validation and final
holdout rows are reduced to row-free leakage evidence, while deterministic
training/tuning corruptions select one additive-smoothing checkpoint.
Generation begins from bounded homogeneous-Poisson noise, keeps observed
anchors outside the denoising state, emits only core-owned points strictly
inside destination-symbol anchor pairs, and records every B/C/D/E, thinning,
collision, resource, and lineage decision.

The retained campaign contains 12 proposal candidates and 16 total reports. The
real closure comparison records no Add-Thin fit/generation failure, refusal,
leakage, or anchor violation, but the challenger fails multiple promotion
gates. It declares `automatic_winner: false` and remains refused from product
selection. See
[`docs/add-thin-challenger.md`](docs/add-thin-challenger.md) for the equations,
approximation boundary, marked extension, strict contracts, resource limits,
carving seam, retained evidence, nonclaims, and primary references.

#### Empirical Reference-Motif Index

`ReferenceMotifIndexV1` projects bounded windows from the augmented ASCII tick
surface into compact event-time offsets, bid/ask deltas, quote-transition
marks, conditioning coordinates, transformation limits, and complete source
lineage. It consumes the enriched evidence without copying the full 521-column
row into every motif event.

Only eligible training windows may enter the artifact. Chronological
calibration, validation, and final-holdout windows are excluded, while
cross-split source overlap and normalized near-duplicate shapes fail closed.
Index selection is stable under input reordering, retrieval follows an explicit
exact-to-global support hierarchy, and matches expose distance, cell support,
fallback level, and deterministic fragment-ID tie-breaking.

Ex-ante queries require an as-of timestamp and hide motifs whose observations
or artifacts were not yet available. Selected motifs bind directly into the
existing reconstruction information audit as training-split empirical-motif
inputs. Index persistence is atomic and content-addressed through an
`ArtifactRef`; augmented panels remain intermediates. See
[`docs/reference-motif-index-contracts.md`](docs/reference-motif-index-contracts.md)
for split, leakage, compact-layout, retrieval, resource, and trust semantics.

#### Real Modern Reference-Motif Library

The installed `histdatacom analytics modern-reference-motif-library` command
builds the first production index from 24 hash-verified monthly EURUSD,
GBPUSD, and EURGBP Arrow caches in stable `technology_epoch_04`. Its fixed
chronological profile keeps 201901--202301 for training and blocks 202307,
202401, and 202510 as calibration, validation, and final holdout.

The builder prefilters normalized cross-split near duplicates, reruns the
fail-closed leakage audit, retains a deterministic compact 256-fragment train
index, aggregates support/backoff coverage, exercises explicit unsupported
refusal, and runs the unchanged #463 real benchmark twice. The installed
readers verify the content-addressed index, manifest, leakage, coverage,
qualification, and resource artifacts. Dense source and holdout rows never
enter those files. See
[`docs/modern-reference-motif-library.md`](docs/modern-reference-motif-library.md)
for the source profile, feature schema, corrected event-clock/transition
semantics, qualification gates, CLI, and nonclaims.

#### Reconstruction Scientific Target

The v2.5 pipeline has one content-addressed scientific ledger for its estimand,
assumptions, context-missingness taxonomy, generated-row constraints, and
validity boundary. It formalizes the output as a plausible conditional
counterfactual ensemble after carving and reconciliation—not recovered ticks,
observed history, or broker history. Ex-post products remain explicitly
`invalid-for-backtest` as newly observed point-in-time evidence.

The ledger is bound through the experiment, plan/runtime graph, product quality
evidence, published dataset version, and certification policy. Source and
validation stages independently classify the completeness and information mode
of every bounded market-context and CFTC query. Retained v2.4 identities remain
readable as `legacy-unbound`, but must be replanned before current execution.

```sh
histdatacom reconstruction --json science
```

Only the qualified HistData.com ASCII/T EURGBP/EURUSD/GBPUSD intersection is in
scope. Provider-neutral contracts preserve later adapter seams; OANDA,
alternate providers, live feeds, and broker conditioning remain later
milestones. See
[`docs/reconstruction-scientific-ledger.md`](docs/reconstruction-scientific-ledger.md).

Marked-Hawkes reconstruction now propagates the qualified observation-operator
interval through distinct high-retention/low-infill, central, and
low-retention/high-infill scenarios. Scenario identity is separate from the
existing ensemble member and path seed. Planning and runtime both admit
against the low-retention endpoint, publish negative-binomial count moments
and quantiles, and refuse an unsafe scenario before generation. Validation and
an untouched final holdout report event-count, cadence, path, spread, triangle,
and strategy effects plus operator-versus-path variance decomposition. All
three scenario products are retained by the current release policy; v2.4
point-estimate artifacts keep their original identity and are never relabeled
as v2.5 scenario evidence. See
[`docs/observation-process-uncertainty.md`](docs/observation-process-uncertainty.md).

Uncertain feed-epoch boundaries are also explicit. New marked-Hawkes products
cross left-persistence, elapsed-time-linear, and early-right-adoption boundary
scenarios with the three observation-operator endpoint scenarios. Planning
uses the worst qualified crossed-cell cardinality bound; runtime and product
evidence expose the transition policy, scenario, and boundary IDs. Ex-ante
transition use is refused without a separately bound point-in-time-valid prior.
See
[`docs/feed-epoch-transition-uncertainty.md`](docs/feed-epoch-transition-uncertainty.md).

#### Reconstruction Math Verification

`current_reconstruction_math_verification_report()` runs the installed v1.0.0
scientific-math harness. Its 23 deterministic checks independently verify the
negative-binomial failures parameterization, strict Hawkes stability,
time-rescaling compensators and inverses, energy and variogram estimators,
dimensionless projection burden, exact FX triangle bid/ask sides, bounded quote
age, and no-future-use semantics. The content-addressed report contains neither
events nor samples and rejects changed formulas, nested checks, and derived
summaries during replay.

The v2.5 certification policy requires the exact passing report as
`reconstruction-math-verification-report`; a campaign extracts
`/summary/passed` only after verifying the report schema, subject identity, and
file hash. This verifies formula implementation, not campaign fitness or
historical truth. See
[`docs/reconstruction-math-verification.md`](docs/reconstruction-math-verification.md).

#### Point-in-Time Reconstruction Evidence

The reconstruction plan now hash-binds a versioned evidence policy and the
HistData source handler compiles bounded row, interval, window, partition, and
series evidence sidecars before proposal work begins. Exact row findings keep
their immutable source-row identity; aggregate quality and fingerprint state
remain sidecars and cannot be flattened onto ticks. Ex-ante execution withholds
future rows, values, and finding counts.

Resolved gap, spread, and source-quality constraints affect proposal
conditioning and historical carving, while reconciliation, delivery, and
validation retain the projection and use-decision lineage through the
committed delivery-quality manifest. The contracts are provider-neutral, but
the public planner admits only HistData.com ASCII/T; OANDA,
alternate-provider, and broker evidence adapters remain later-milestone work.
See
[`docs/reconstruction-evidence-contracts.md`](docs/reconstruction-evidence-contracts.md)
for grain, availability, fallback, boundedness, and audit semantics.

#### Synchronized Cross-Series Constraints

The plan now also hash-binds a provider-neutral cross-series constraint policy.
For the current milestone, source enrichment compiles only the complete
HistData.com EURGBP/EURUSD/GBPUSD ASCII/T core window. Each bounded bundle
strongly identifies dataset, series, partition, event identity, duplicate
timestamps, coverage, availability, alignment support, #331 fingerprint
content, residual severity, and readiness.

Exact event-sequence alignment is preferred. Bounded nearest-prior evidence is
diagnostic only: it never forward-fills, never turns timestamps into row
identity, and never changes observed quotes. Proposal consumes one explicitly
supported synchronization instant and records its constraint-window ID;
carving, reconciliation, delivery, and validation preserve bundle, window, and
use-decision IDs through the committed delivery-quality manifest.
Contradictory or incomplete groups remain available for anomaly labeling but
fail closed for proposal and every later production stage.

The contracts establish the provider-neutral seam required by future dataset
adapters, but OANDA, other historical providers, live feeds, and
broker-specific adaptation remain later-milestone work and are rejected by the
current planner. See
[`docs/cross-series-constraint-contracts.md`](docs/cross-series-constraint-contracts.md)
for alignment, point-in-time, readiness, lineage, and runtime semantics.

#### Empirical Motif Candidate Generation

`generate_empirical_motif_candidates()` proposes zero, one, or many narrow
`SyntheticEventV1` rows between immutable historical anchors. Cardinality and
cadence come from the conditioned delivery regime; selected empirical paths
are transformed only inside their declared time/price support and detrended
onto an anchor-to-anchor bridge so fragment seams cannot accumulate jumps.

Seeds and event identity depend on semantic run, member, anchor, motif, and
configuration inputs—not retries, workers, windows, or storage estimates.
Each event maps to a recoverable transform containing its source motif,
support/backoff, scale, seed, condition query, and source artifact lineage.
Sparse evidence, closed sessions, zero-width intervals, unsafe quotes, and
resource overruns produce explicit empty/refused decisions.

Candidate rows remain bounded, process-local streaming intermediates. Batch
metadata states that hard carving, broker conditioning, and final persistence
have not run. The included benchmark adapter lets the existing
reverse-degradation harness compare this generator with all controls without
selecting an automatic winner. See
[`docs/empirical-motif-generation-contracts.md`](docs/empirical-motif-generation-contracts.md)
for determinism, seam, lineage, resource, and stage-boundary details.

#### Historical Candidate Carving

`carve_empirical_motif_candidates()` is the first stage allowed to accept
candidate-only motif rows. A versioned constraint set applies immutable-anchor,
resource, fingerprint-validation, context-support, quarantine, and session
closure rules before conditioned motif eligibility, intensity thinning, or
spread projection. Missing support refuses rather than inventing liquidity.

News, rollover, crisis, and other explicit state tags can change acceptance
rates, eligible motifs, and spread envelopes. Incompatible motifs may use a
same-position candidate from an explicitly supplied substitution batch;
otherwise they are rejected. Deterministic scores exclude retry, worker, and
window identity, so adjacent window outputs union to the single-window result.

Accepted rows carry the final constraint-set ID and compact lineage back to the
candidate event, batch, and motif transform. Projected lineage retains original
quotes and candidate/output content hashes. Rejected rows are discarded; only
reconciling reason counts and bounded examples remain. See
[`docs/historical-carving-contracts.md`](docs/historical-carving-contracts.md)
for precedence, evidence binding, refusal, identity, and streaming semantics.

#### Cross-Currency Reconciliation

`plan_cross_currency_windows()` intersects explicit per-symbol coverage and
plans only complete synchronized windows. Missing legs, unequal leading or
trailing periods, and spans without common support remain recorded exclusions;
they are never filled or silently shortened.

`reconcile_cross_currency_window()` applies versioned triangle and inverse
relationships at exact nanosecond event times. It never forward-fills another
instrument. Duplicate timestamps pair by deterministic event ordinal, while
asynchronous support and stale-join risk remain measured. Only synthetic
quotes may be projected; immutable observations are content-hashed and must
remain unchanged. The first certified relationship is `EURUSD / GBPUSD ~=
EURGBP`.

Residuals, support, projections, and infeasibility are stratified by session,
event, and feed epoch. Every passing generation group still requires the same
content-bound validation after broker conditioning. A partition manifest can
commit only when that final validation covers the complete all-symbol
synchronization unit and exact output content. The existing #331 diagnostic
also consumes reconciled streams directly without a permanent cache
roundtrip. See
[`docs/cross-currency-reconciliation-contracts.md`](docs/cross-currency-reconciliation-contracts.md)
for projection, refusal, validation, compatibility, and atomic-commit details.

#### Calibrated Reconstruction Ensembles

`plan_reconstruction_ensemble()` derives stable member IDs and semantic seeds
from exact source/configuration hashes, not workers, retries, row order, or
retention rank. Reverse-degradation windows then measure member intervals by
feed epoch, session, event state, symbol, horizon, and sparsity. Validation
cells fit bounded adjustments; final-holdout cells alone report achieved
coverage, failures, refusals, and substantive diversity.

Logical-content hashes exclude member/seed/lineage identity, so identical
market paths are diagnosed as collapsed and ID-only or metric-free differences
cannot count as useful diversity. The primary member is explicitly a compact
validation-medoid representative, not historical truth, an automatic winner,
or a default generator.

Storage estimates cover all-member computation and scratch while durable
output is limited to a configured retained subset. Omitted members can be
regenerated only from the frozen plan after every source and configuration
SHA-256 matches. Reports contain bounded summaries rather than event rows.
Motif-match similarity remains uncalibrated transformation evidence; generated
tick confidence stays null. See
[`docs/reconstruction-ensemble-calibration-contracts.md`](docs/reconstruction-ensemble-calibration-contracts.md)
for calibration, confidence, diversity, retention, and replay semantics.

#### Live Broker Delivery Capture

> **Later milestone:** no live broker or OANDA feed is selected for the current
> HistData-only reconstruction path. The contracts below preserve a future
> capture seam; they do not qualify a v2.5 execution dataset.

`histdatacom.broker_capture` records a broker feed as versioned measurement
evidence rather than guessing modern delivery style from historical vendor
data. Adapter messages retain optional broker/exchange timestamps with explicit
precision, exact price lexemes, batch/message identity, honest size/activity
semantics, quote and lifecycle events, and raw-message hashes where permitted.
The collector adds adjacent UTC wall and monotonic receive clocks plus explicit
clock-correction events.

Canonical JSONL partitions are appended, fsynced, rotated, hashed, and exposed
only after atomic compact-manifest publication. Quota, immutable retention, and
high-watermark backpressure refuse predictably. Partial/orphan artifacts remain
detectable but undiscoverable as completed data, while verified replay checks
sidecars, bytes, hashes, rows, counts, and sequence before sending events
through the same consumer interface used during live collection.

The core adapter protocol never inspects private broker configuration and the
public contracts reject credential-shaped metadata. A real adapter still
requires an explicit broker/protocol/licensing decision; no redesign is needed.
See [`docs/broker-capture-contracts.md`](docs/broker-capture-contracts.md) for
clock, security, storage, replay, fixture, and fingerprint eligibility gates.

#### Broker Delivery Fingerprints

> **Later milestone:** broker/OANDA feed adaptation is not part of the current
> HistData-only executable path. These contracts preserve the future seam, but
> no broker dataset or profile is admitted by v2.5 compatibility.

Qualified broker captures are converted into compact immutable delivery
profiles with `fit_broker_delivery_fingerprint()`. Fitting verifies capture
health and hashes in a first streaming pass, then performs bounded deterministic
aggregation in a second pass and rechecks the logical content hash. It does not
persist augmented capture rows or materialize tick-sized intermediates.

Profiles describe cadence, quote intensity, spread and spread changes,
duplicate/stale/burst behavior, source timestamp and price precision, batching,
outage/reconnect/clock behavior, and conditional behavior by symbol, session,
overlap, special window, holiday, market event, and lifecycle state. Every cell
records observed support plus an explicit supported, ordered-backoff, or
unsupported decision. Every metric has support, bounded samples, uncertainty,
extrema, quantiles, units, and limitations.

`compare_broker_delivery_fingerprints()` produces bounded, stratified drift
evidence without a global similarity score or automatic winner. Successors are
new effective-dated artifacts that reference—but never mutate—the old profile,
so prior synthetic lineage remains reproducible. See
[`docs/broker-delivery-fingerprint-contracts.md`](docs/broker-delivery-fingerprint-contracts.md)
for eligibility, streaming, condition, drift, persistence, and #445 handoff
semantics.

#### Broker-Conditioned Reconstruction

> **Later milestone:** this research implementation remains non-executable in
> the current public planner until a qualified broker feed exists. The v2.5
> path accepts only HistData.com ASCII/T data and `modern_reference` delivery.

`condition_broker_proposal()` applies a versioned, bounded broker-delivery
strength to cadence, burst/quiet/outage, spread, and precision coordinates before
motif retrieval. Exact profile cells and recorded backoff are honored; missing,
unsupported, ineffective, or mismatched-drift selections refuse without issuing
a conditioned query.

After historical carving and cross-currency reconciliation,
`render_broker_delivery()` applies deterministic precision, rounding, batching,
stale-quote, exact-duplicate, timestamp, and spread presentation to synthetic
rows only. Observed anchors remain unchanged. The entire group is withheld until
local constraints, post-broker cross-currency validation, and the #331
cross-instrument quality path pass. Compact manifests retain content hashes,
event lineage, profile/effective-period/drift evidence, action counts, config,
and optional paired benchmark comparison IDs; augmented tick intermediates are
not made durable.

See
[`docs/broker-delivery-transfer-contracts.md`](docs/broker-delivery-transfer-contracts.md)
for proposal, renderer, refusal, validation, benchmark, and streaming/persistence
semantics.

#### Atomic Reconstruction Persistence

`publish_reconstruction_group()` turns one fully validated broker-rendered
symbol group into the final narrow archive. It requires an independent exact
set of immutable observed anchors plus a primary/retained-member storage
preflight. Only the 26-column `SyntheticEventV1` schema is written; the
521-column analytical frame and rejected candidates remain scratch data.

Zstandard Parquet files are partitioned by schema, run, broker fingerprint,
ensemble member, symbol group, symbol, and UTC event date. Files and compact
source/constraint/quality/replay/retention manifests are validated below a
hidden transaction directory, then the complete synchronized unit is promoted
with one atomic same-filesystem rename. Discovery sees only committed
directories. Repeating an identical publication is idempotent, while anchor
drift, truncation, checksum/schema/count mismatches, or different physical
writer settings fail closed.

Arrow batches and lazy Polars scans support symbol/time file pruning, column
projection, and event-time predicate pushdown. The optional `query` extra adds
DuckDB for direct Parquet inspection:

```sh
pip install "histdatacom[arrow,query]"
```

See
[`docs/reconstruction-persistence-contracts.md`](docs/reconstruction-persistence-contracts.md)
for layout, atomic commit, replay, preflight, query, cleanup, and #447 Temporal
handoff semantics.

#### Reconstruction Activity Semantics

Final reconstructed rows are quote deliveries, not centralized FX trades.
`summarize_reconstruction_activity_streams()` and
`summarize_committed_reconstruction_activity()` derive deterministic activity
metadata without widening the immutable 26-column `SyntheticEventV1` schema or
persisting the 521-column analytical frame. The committed reader projects only
the 19 event, price, origin, confidence, and lineage columns needed by the
online accumulator and processes configurable bounded Arrow batches.

Every symbol can emit separate observed-only, synthetic-only, and merged
slices. Each slice records quote-event/update counts, exposure duration, tick
intensity, interarrival cadence, price-change and stale-quote transitions,
spread-based liquidity proxies, optional event-confidence support, exact units,
aggregation rules, bounded provenance, and a content hash. Volume handling is
explicitly `unavailable`, `omitted`, source/broker supplied, or a synthetic
activity proxy; the contracts always set `centralized_traded_volume_claim` to
false and refuse source-size states when the final event schema has no such
fields.

Activity evidence is bound to an information manifest and either ex-post or
ex-ante mode. Ex-ante summaries require an as-of timestamp, while ex-post
summaries reject one. Validation reuses the existing reverse-degradation
scorecard's event-count, intensity, interarrival, burst/quiet, and spread
metrics plus calibration support; it never selects an automatic winner.
Explicit sum, boundary-carry, recomputation, and support-weighted-mean rules
form the derived-bar handoff for #18, with volume remaining unavailable unless
separately sourced.

See
[`docs/reconstruction-activity-semantics.md`](docs/reconstruction-activity-semantics.md)
for metric definitions, information and volume policy, provenance, streaming,
benchmark, and derived-bar contracts.

#### Derived Reconstruction Candlesticks

`publish_derived_bars()` creates an optional export product from a verified
committed reconstruction manifest. It never reads raw HistData M1 rows or the
521-column analytical frame. The immutable 26-column event product remains the
source of truth, while derived bars use a separate versioned 64-column schema
and compact manifest. Each row carries the derivation policy ID and rounding
precision; the manifest records any requested time-window bounds.

Version one supports UTC Unix-epoch-aligned `1m`, `5m`, `15m`, `30m`, `1h`,
`4h`, and `1d` half-open bins. Bid, ask, midpoint, and spread OHLC values follow
canonical `(event_time_ns, event_sequence, event_id)` order. Observed-only,
synthetic-only diagnostic, and merged-product scopes are explicit. Empty bins
and market closures emit no rows, query-cut edge bins are flagged partial, and
no price, liquidity, or volume is forward-filled.

The streaming accumulator carries the previous quote into the next non-empty
bar for price-change/stale transition accounting, then projects #80 counts,
duration, intensity, stale rate, mean spread, and confidence support. Volume is
always null with state `unavailable`. Each row retains event bounds, origin
support, bounded generator/broker/constraint lineage, and an event-content
hash.

```python
from histdatacom.synthetic import (
    ActivitySliceScope,
    DerivedBarPolicyV1,
    publish_derived_bars,
    scan_derived_bars_polars,
)

published = publish_derived_bars(
    "data/exports",
    "data/reconstruction-products/.../commits/.../manifest.json",
    policy=DerivedBarPolicyV1(
        intervals=("1m", "5m", "1h"),
        scopes=(ActivitySliceScope.MERGED,),
    ),
)
bars = scan_derived_bars_polars(
    published.manifest_path,
    columns=("symbol", "bar_start_ns", "mid_close", "event_count"),
)
```

Monthly Parquet partitions are written below hidden scratch, replay-verified,
and promoted with one same-filesystem rename. Column/time/symbol/scope/interval
projection and pruning are available through Arrow batches and lazy Polars
scans. Raw M1 download/import remains rejected.

See [`docs/derived-bar-contracts.md`](docs/derived-bar-contracts.md) for the
complete interval, OHLC, activity, lineage, partial/empty-bin, storage,
verification, and downstream reconciliation contract.

#### Reconstructed-History Strategy Sensitivity

`evaluate_strategy_sensitivity()` applies one content-addressed strategy,
execution, cost, latency, horizon, and resource policy to multiple exact
time-aligned source cases. Supported cases include untouched observed history,
degraded modern holdouts, reconstructed ensemble members,
broker-conditioned/unconditioned streams, and verified derived bars. Case
identity includes the source artifact, symbol, half-open window, information
audit, ensemble member, broker profile, and—when applicable—bar scope and
interval.

The evaluator is streaming and bounded. It retains only strategy state,
pending signals, and online aggregates; quotes, individual outcomes, the
521-column analytical frame, and strategy columns are not persisted. Results
are stratified by feed epoch, session, event state, sparsity, broker profile,
ensemble member, and horizon. Reports include failure, no-trade,
missing-support, and refusal rates, member/window dispersion, and explicit
reverse-degradation evidence showing whether reconstructed execution response
moves toward the dense reference relative to the degraded input.

`ReferenceMomentumStrategyV1` is a transparent lagged-midpoint fixture for
alignment and accounting tests. It is not a recommended strategy. Version one
uses normalized exposure, crosses bid/ask, and makes latency, quote-wait,
slippage, and per-side fixed costs explicit. Reports are compact derived
metadata and always set profit claims, investment recommendations, event-schema
augmentation, and automatic winner selection to false.

Ex-post cases require an explicit `invalid-for-backtest` reason. Mixed ex-ante
and ex-post plans require a plan-level reason as well; the label permits only a
descriptive historical counterfactual and never converts it into point-in-time
strategy evidence.

See
[`docs/strategy-sensitivity-contracts.md`](docs/strategy-sensitivity-contracts.md)
for input identities, information-mode gates, source adapters, accounting,
stratification, restoration, terminal states, and resource bounds.

#### EURUSD Triangle Reconstruction Certification

`modern_reference_triangle_certification_policy()` predeclares the current
v2.5.0 scientific, operational, reporting, repository, and release contract for
the EURGBP/EURUSD/GBPUSD product over common support beginning at `200203`. It
fixes `modern_reference` delivery with the `unconditioned_reference` claim and
explicitly excludes broker adaptation. The common end month, source-readiness
contracts, scientific thresholds, and peak-memory/scratch/runtime/storage and
candidate-amplification budgets participate in the deterministic policy
identity. The older broker-bound `eurusd_triangle_certification_policy()` and
V1 dossiers remain readable for evidence replay but are not the #449 release
path; the current factory additionally binds #491's powered portfolio and
#508's validation-only marked-Hawkes product selection and #509's
holdout-calibrated observation-process uncertainty report alongside #498's
gap-free support map, complete product/dataset publication,
mounted-storage qualification, and recovery evidence.

The information-safety gate also binds the exact passing reconstruction
math-verification report, so release-critical parameterizations cannot drift
independently of certification evidence.

Certification consumes compact, verified report artifacts bound to that exact
policy identity, so evidence cannot be reused after scope or threshold drift.
Each scalar observation names the exact artifact identities that support it,
and every requirement declares the exact artifact kinds it needs. Missing
artifacts remain `missing`; measured threshold violations are `failed`; neither
can become a pass through a summary boolean. V2 rejects every broker-named
artifact instead of silently turning synthetic reference output into a broker
claim.

`evaluate_modern_reference_reconstruction_certification()` covers all fifteen
#491 gate groups plus the individual portfolio, diagnostics, source-readiness,
and operations seams and
returns one bounded `ReconstructionCertificationDossierV2`. The dossier can be
`incomplete`, `failed`, `ready-for-promotion`, or `certified`.
`ready-for-promotion` is narrowly reserved for the state where every check has
passed except the single coverage observation. Coverage is still run exactly
once, only during the explicit `dev`-to-`main` promotion. The TestPyPI local
simple-registry preflight and all non-coverage evidence must already pass.

`histdatacom reconstruction certify --spec CAMPAIGN.json --output-directory
DIR` executes the public campaign. It verifies every declared JSON artifact's
SHA-256, schema, and subject identity, then extracts each observation through a
declared JSON pointer. Observation values cannot be written inline in the
campaign spec, and promotion-only coverage is refused on an ordinary `dev`
campaign. Publication atomically writes canonical machine JSON, deterministic
Markdown, the frozen campaign manifest, methodology evidence, and a bounded
campaign receipt. The dossier contains no tick rows or analytical-frame
columns and never claims historical truth, selects an automatic winner, makes
an investment recommendation, or authorizes release before every gate passes.

See
[`docs/reconstruction-certification-contracts.md`](docs/reconstruction-certification-contracts.md)
for the gate mapping, artifact binding, state machine, publication semantics,
and required real-data execution sequence.

#### Temporal Reconstruction Orchestration

`ReconstructionRunWorkflow` plans deterministic memory-weighted waves of
all-symbol `ReconstructionWindowWorkflow` children. Each window executes the
source/enrichment, proposal, carving, cross-series, delivery-projection,
validation, and atomic-commit boundaries sequentially through activity-side
stage handlers. Workflow history carries only bounded commands, counters,
checkpoints, and strong artifact references.

Stage receipts and manifest-store compare-and-swap snapshots make worker loss,
activity retry, duplicate completion, and process restart resumable without
duplicating committed rows. Cancellation removes only disposable window
scratch. The final report independently verifies every committed publication
and reconciles storage counts and scope with workflow checkpoints.

Default workers install the seven versioned first-party handlers. The reference
path uses explicit modern-reference identity delivery and generic v2
persistence, so no application registration or fake broker fingerprint is
needed. Validation keeps a byte-identical staged-manifest mirror and a separate
transaction descriptor, allowing a retry after the atomic rename but before
the commit receipt to recover the already committed publication safely.

See
[`docs/reconstruction-temporal-orchestration.md`](docs/reconstruction-temporal-orchestration.md)
for adapter registration, queue/resource policy, backpressure, recovery,
cancellation, report reconciliation, and fault-injection guarantees.

#### Reconstruction Evidence Diagnostics

The first-party diagnostic publisher converts strong retained HistData
experiment and qualification evidence into twelve bounded chart-data families
with stable, scale-coherent views where one mixed axis would mislead.
Missing product, carving, bar, or strategy evidence remains explicitly
unavailable; underpowered qualification remains underpowered. Figures never
select an engine or replace the machine-readable gates.

```sh
histdatacom reconstruction --json diagnostic-build \
  --spec work/diagnostic-spec.json \
  --output-directory work/diagnostics
histdatacom reconstruction --json diagnostic-list \
  --manifest work/diagnostics/reconstruction-diagnostic-publication-<sha256>.json
```

JSON publication works in the base install. The `histdatacom[viz]` extra adds
deterministic, receipted SVG and PNG rendering. Current execution is restricted
to HistData.com ASCII/T evidence; provider-neutral contracts preserve the
future seam, while OANDA and broker data remain later milestones. See
[`docs/reconstruction-diagnostics.md`](docs/reconstruction-diagnostics.md).

#### Public Reconstruction CLI and API

`histdatacom reconstruction` and `ReconstructionClient` expose the same typed
plan, operator-request, preflight, submission, receipt, status, cancel, resume,
output-list, bounded-preview, and integrity-replay contracts. The installed
command requires an explicit ex-post or ex-ante information mode plus the
machine-readable acknowledgement that reconstructed output is plausible
counterfactual evidence—not recovered historical truth.

Full-range planning also has a bounded plan-set surface. `plan-set` begins with
bounded month groups and deterministically bisects any group whose execution,
retention, or artifact-size preflight refuses it; `preflight-set` freshly
verifies every resulting shard identity, artifact hash, exact contiguity,
refusal, and resource bound and reconciles the parent aggregate. This preserves
both the per-window runtime budget and the 64 MiB plan-artifact limit instead
of weakening either one for long historical ranges. Shared monthly source
partitions count once in the parent inventory totals, and repeated strong-ref
verification is cached only while the file's device, inode, size, modification
time, and change time remain identical. A span with no scientifically
supported window is retained as a refusal-only shard with zero workflows and
zero output estimates; acknowledging refusals makes that no-op safe to skip,
but never turns the unsupported span into reconstructed output.

Plan-set shards preserve every operator-supplied storage base. Control
artifacts remain under `artifact_root`; committed products, checkpoints, and
disposable scratch use stable shard children under `output_root`,
`checkpoint_root`, and `scratch_root` respectively. Output and scratch must be
on the same mounted filesystem for atomic publication. The
[complete campaign runbook](docs/reconstruction-campaign-runbook.md) covers
mount qualification, gap-free support proof, first-party Temporal execution,
forced crash/resume evidence, product reconciliation, and provider-neutral
dataset publication.

Schema discovery and compatibility admission are also first-party:

```sh
histdatacom reconstruction schemas --json
histdatacom reconstruction engines --json
histdatacom reconstruction --json science
histdatacom reconstruction compatibility --plan plan-spec.json --json
histdatacom reconstruction --json experiment-list --root work/plan-artifacts
```

The registry audits public contracts, explicitly accounts for internal-only
schemas, distinguishes legacy raw and enriched HistData caches, and exposes the
executable proposal-engine portfolio contracts. The planner consumes the same
compatibility engine. A v1 input translates to an explicit motif-only
portfolio; a v2 input must declare engine order, product selection, and
retained evaluation evidence. Provider-neutral identity is the architectural
foundation, while alternate providers, OANDA, and broker conditioning remain
later-milestone work.

Engine evaluation and resolved product eligibility are also public:

```sh
histdatacom reconstruction --json engine-evaluate \
  --benchmark-manifest artifacts/reverse-degradation-manifest-<sha256>.json \
  --source-root data/ASCII/T \
  --output-directory work/proposal-evaluation
histdatacom reconstruction --json qualify \
  --evaluation work/proposal-evaluation/proposal-portfolio-evaluation-<sha256>.json \
  --experiment work/experiment/reconstruction-experiment-<sha256>.json \
  --output-directory work/qualification
histdatacom reconstruction --json hawkes-select \
  --policy work/selection/hawkes-product-selection-policy-<sha256>.json \
  --comparison work/selection/hawkes-validation-comparison-<sha256>.json \
  --qualification work/qualification/powered-qualification-dossier-<sha256>.json \
  --output-directory work/selection
histdatacom reconstruction --json observation-uncertainty-policy \
  --output-directory work/observation-uncertainty
histdatacom reconstruction --json portfolio \
  --plan work/plan-artifacts/synthetic-infill-plan-<sha256>.json
```

Failed and underpowered engines stay inspectable; they cannot enter a committed
product. Qualification binds the exact experiment, evaluation, row-free metric
trace, power study, engine decisions, and validation-fitted portfolio weights.
The current powered dossier admits diagonal and full self/cross excitation;
the frozen product portfolio selects only diagonal self-excitation. Every
nonpassing or eligible-but-unselected engine remains inspectable and is
excluded from execution. The product choice is replayed from paired,
validation-only evidence with frozen projection-burden, uncertainty, power,
Pareto, resource, and complexity rules; final-holdout results cannot change it.
There is no silent fallback or automatic winner. See
[`docs/powered-reconstruction-qualification.md`](docs/powered-reconstruction-qualification.md)
and [`docs/hawkes-product-selection.md`](docs/hawkes-product-selection.md).
The separate scenario/admission/calibration contract is documented in
[`docs/observation-process-uncertainty.md`](docs/observation-process-uncertainty.md).

Only HistData.com ASCII/T and the complete EURGBP/EURUSD/GBPUSD triangle are
accepted. M1, bar, partial-triangle, alternate-provider, OANDA, and broker
requests fail before execution. Temporal is the production path; `--local` is
an explicit first-party handler smoke and checkpoint-recovery mode, never an
automatic fallback. Operation receipts bind
each workflow handle to its actual reconstruction status store, and resumed
attempts preserve scientific/checkpoint identity while using fresh parent and
child Temporal IDs.

Committed outputs can be listed, previewed with bounded origin/anchor/generator/
confidence/constraint-decision lineage, and replay-verified from either public
surface. The CLI returns distinct invalid-plan, refusal, runtime, validation,
and success exit codes.

See
[`docs/reconstruction-public-interfaces.md`](docs/reconstruction-public-interfaces.md)
for exact JSON contracts, commands, Python examples, recovery semantics, and
the exit-code table, and
[`docs/reconstruction-schema-compatibility.md`](docs/reconstruction-schema-compatibility.md)
for discovery and admission semantics.

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
files, extracted CSV files, cache IPC files, and merged API artifacts stay
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
histdatacom -p eurusd -f ascii -t tick-data-quotes -s now
```

Interactive waited CLI runs render a live Rich progress view while the Temporal
job is running; piped output and API calls keep the machine-readable result
path.

Submit without waiting for completion:

```sh
histdatacom --submit-only -p eurusd -f ascii -t tick-data-quotes -s now
```

The JSON control surface supports job inspection and future GUI polling:

```sh
histdatacom jobs list --json
histdatacom --request-bundle-out run.json --no-overlap --schedule-key eurusd-cache --build-cache -p eurusd -f ascii -t tick-data-quotes -s now
histdatacom jobs preflight --bundle run.json --json
histdatacom jobs list --schedule-key eurusd-cache --active --json
histdatacom jobs progress histdatacom-<request-id> --watch
histdatacom jobs progress histdatacom-<request-id> --json
histdatacom jobs artifacts histdatacom-<request-id> --json
histdatacom jobs cancel histdatacom-<request-id> --reason "operator stop"
```

Use `--request-bundle-out PATH` to export a scheduled-run bundle from ordinary
CLI options plus `--no-overlap --schedule-key <key>` without starting Temporal,
submitting work, downloading archives, or mutating job state. Use
`--request-bundle-out -` to print the bundle to stdout. That payload can be
passed directly to `jobs preflight --bundle` and `jobs submit --bundle`; the
bundled schedule metadata is applied automatically. Explicit jobs flags override
the bundle when needed: `--schedule-key <key>` replaces the bundled key,
`--no-overlap` enables the guard, and `--allow-overlap` disables a bundled or
request-level guard for a deliberate one-off run.

Use `--request-json-out PATH` when a lower-level raw `RunRequest` is needed.
Raw request payloads still work with `jobs preflight --request-json` and
`jobs submit --request-json`; put `--no-overlap --schedule-key <key>` on those
jobs commands when schedule identity should be applied at preflight/submit time.
Allowed preflights exit `0`; blocked preflights exit `75` and include the
blocking job in JSON output. Use `jobs list --schedule-key <key> --active` to
inspect the non-terminal job that would block a scheduled `--no-overlap`
submission. Fingerprint-only scheduled runs can be matched with
`--schedule-fingerprint sha256:...`. `jobs inspect --json` includes a stable
`schedule_identity` object with the schedule key or fingerprint, active/terminal
state, and whether the job blocks duplicate submissions.

Omit `--json` on `jobs progress` for the Rich terminal progress view; add
`--watch` to live-refresh it until the job reaches a terminal state. The Rich
view includes a bounded operational health panel with runtime/component/PID
state, POSIX disk headroom, cache inventory, source-artifact cleanup counts,
active workflow counts, and ETA/rate information when progress metadata is
available.

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
- API calls with `options.output_timezone` append a timezone-aware
  `datetime_local` view after cache materialization. Canonical `datetime` and
  `timestamp_utc_ms` values remain UTC epoch milliseconds, and no localized
  value is persisted.
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
options.output_timezone = "America/New_York"  # optional IANA output view
```

The equivalent command/config option is `-z/--timezone IANA_ZONE`; YAML accepts
either `timezone` or `output_timezone`. Unknown timezone names fail before the
orchestration job is submitted. The returned `datetime_local` datatype carries
the selected timezone in Polars, pandas, and Arrow. Daylight-saving transitions
follow that output zone, but HistData source timestamps remain interpreted as
fixed EST without daylight-saving adjustments.

Set `options.orchestration_wait_result = False` to submit a job and receive
job metadata instead of a materialized API return object. Set
`options.orchestration_start = False` when a caller requires a pre-started
runtime. `options.use_orchestration = False` is not supported.

#### Cron Setup and Examples

Cron jobs should run from a stable project directory, use a predictable runtime
workspace, and write logs outside the package tree. Use the same workspace for
every scheduled `histdatacom`, `histdatacom runtime`, `histdatacom jobs`, and
`histdatacom cleanup` command that should share runtime state.

A crontab header can make those assumptions explicit:

```cron
SHELL=/bin/sh
PATH=/usr/local/bin:/usr/bin:/bin
HISTDATACOM_PROJECT=/srv/histdatacom
HISTDATACOM_DATA=/srv/histdatacom/data
HISTDATACOM_LOG_DIR=/var/log/histdatacom
HISTDATACOM_RUNTIME_WORKSPACE=/srv/histdatacom
```

Use `--no-overlap` with a stable `--schedule-key` for scheduled submissions that
must not run twice in the same runtime workspace. The application checks
persisted job state before submission and exits nonzero when an active matching
job already exists. A shell wrapper or `flock` can still be useful as an outer
defense when available, but it is no longer the only overlap protection. The
examples below append logs and use `--submit-only` for scheduled data/cache work
so cron records the job metadata quickly; inspect progress later with
`histdatacom jobs ...`.

```sh
histdatacom --request-bundle-out run.json --no-overlap --schedule-key eurusd-cache --build-cache --data-directory "$HISTDATACOM_DATA" -p eurusd -f ascii -t tick-data-quotes -s now
histdatacom jobs preflight --bundle run.json --json
histdatacom jobs list --schedule-key eurusd-cache --active --json
```

Direct CLI submissions that are not driven by a serialized `RunRequest` still
use the submit-time guard:

```sh
histdatacom --submit-only --no-overlap --schedule-key eurusd-cache --build-cache --data-directory "$HISTDATACOM_DATA" -p eurusd -f ascii -t tick-data-quotes -s now
```

```cron
# Submit a serialized EURUSD cache bundle only when preflight allows it.
15 1 * * 1-5 cd "$HISTDATACOM_PROJECT" && histdatacom jobs preflight --bundle run.json --json >> "$HISTDATACOM_LOG_DIR/eurusd-cache-preflight.jsonl" 2>&1 && histdatacom jobs submit --start --submit-only --bundle run.json --json >> "$HISTDATACOM_LOG_DIR/eurusd-cache.log" 2>&1

# Optional outer shell lock for hosts that provide flock.
15 1 * * 1-5 cd "$HISTDATACOM_PROJECT" && flock -n /tmp/histdatacom-eurusd.lock sh -c 'histdatacom jobs preflight --bundle run.json --json >> "$HISTDATACOM_LOG_DIR/eurusd-cache-preflight.jsonl" 2>&1 && histdatacom jobs submit --start --submit-only --bundle run.json --json >> "$HISTDATACOM_LOG_DIR/eurusd-cache.log" 2>&1'
```

Source cleanup can stay in dry-run mode until the reported paths are expected;
add `--apply` only when the cleanup policy is understood for that data root.

```cron
# Record cache/source cleanup status each morning.
30 6 * * * cd "$HISTDATACOM_PROJECT" && histdatacom cleanup status --data-directory "$HISTDATACOM_DATA" --pair-groups majors -f ascii -t tick-data-quotes --json >> "$HISTDATACOM_LOG_DIR/cleanup-status.jsonl" 2>&1

# Remove transient ZIP/CSV sources while preserving .data caches.
45 6 * * 0 cd "$HISTDATACOM_PROJECT" && flock -n /tmp/histdatacom-cleanup.lock histdatacom cleanup sources --data-directory "$HISTDATACOM_DATA" --apply >> "$HISTDATACOM_LOG_DIR/source-cleanup.log" 2>&1
```

Runtime health and maintenance jobs should use the same stable workspace as the
scheduled submissions:

```cron
# Emit runtime health for monitoring.
*/15 * * * * histdatacom runtime --workspace "$HISTDATACOM_RUNTIME_WORKSPACE" status --json >> "$HISTDATACOM_LOG_DIR/runtime-status.jsonl" 2>&1

# Prune runtime logs and persisted status metadata weekly.
10 3 * * 0 histdatacom runtime --workspace "$HISTDATACOM_RUNTIME_WORKSPACE" maintenance --json >> "$HISTDATACOM_LOG_DIR/runtime-maintenance.jsonl" 2>&1
```

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
- *to fit optional Statsmodels classical-model families*
  - `pip install "histdatacom[models]"`

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
options.output_timezone = "America/New_York"  # optional datetime_local column
options.formats = {"ascii"}  # Must be {"ascii"}
options.timeframes = {"tick-data-quotes"}  # can be tick-data-quotes or tick-data-quotes
options.pairs = {"eurusd"}
# Or choose named baskets with options.pair_groups = {"majors", "major-triangles"}
# Or one triangle with options.pair_groups = {"triangle-eurgbp-eurusd-gbpusd"}
options.start_yearmonth = "2021-04"
options.end_yearmonth = "2021-05"
options.cpu_utilization = "medium"
```

- This example uses just one pair/instrument/symbol `eurusd` and just one timeframe `tick-data-quotes`. When the api is called with this 'one-one` specificity, the api will directly return the requested data.
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
options.timeframes = {"tick-data-quotes"}
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
    'timeframe': 'T',
    'pair': 'EURUSD',
    'records': [<histdatacom.records.Record object ...>, ...],
    'data':
                    datetime      bid      ask  vol
      0       1609711200123  1.22396  1.22398    0
      1       1609711200456  1.22397  1.22399    0
      2       1609711200789  1.22395  1.22397    0
      3       1609711201123  1.22398  1.22400    0
      4       1609711201456  1.22399  1.22401    0
      ...               ...      ...      ...  ...
      994672  1650664680123  1.07980  1.07982    0
      994673  1650664680456  1.07981  1.07983    0
      994674  1650664680789  1.07979  1.07981    0
      994675  1650664681123  1.07978  1.07980    0
      994676  1650664681456  1.07980  1.07982    0

      [994677 rows x 4 columns]
  },
  {
    'timeframe': 'T',
    'pair': 'USDCAD',
    'records': [<histdatacom.records.Record object ...>, ...],
    'data':
                    datetime      bid      ask  vol
      0       1609711200123  1.27136  1.27138    0
      1       1609711200456  1.27137  1.27139    0
      2       1609711200789  1.27135  1.27137    0
      3       1609711201123  1.27138  1.27140    0
      4       1609711201456  1.27139  1.27141    0
      ...               ...      ...      ...  ...
      993946  1650664680123  1.27091  1.27093    0
      993947  1650664680456  1.27092  1.27094    0
      993948  1650664680789  1.27090  1.27092    0
      993949  1650664681123  1.27089  1.27091    0
      993950  1650664681456  1.27091  1.27093    0

      [993951 rows x 4 columns]
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
T EURUSD
               datetime      bid      ask  vol
0       20210103 170000123  1.22396  1.22398    0
1       20210103 170000456  1.22397  1.22399    0
2       20210103 170000789  1.22395  1.22397    0
3       20210103 170001123  1.22398  1.22400    0
4       20210103 170001456  1.22399  1.22401    0
...                   ...      ...      ...  ...
994672  20220422 165800123  1.07980  1.07982    0
994673  20220422 165800456  1.07981  1.07983    0
994674  20220422 165800789  1.07979  1.07981    0
994675  20220422 165801123  1.07978  1.07980    0
994676  20220422 165801456  1.07980  1.07982    0

[994677 rows x 4 columns]
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
    data_options.timeframes = {"tick-data-quotes"}  # can be tick-data-quotes or tick-data-quotes
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
pip install "histdatacom[models]"
pip install "histdatacom[query]"
pip install "histdatacom[all]"
```

`histdatacom[temporal]` remains available for explicit runtime installs, but
the Temporal Python SDK is part of the base package dependency set because
orchestration is the default runtime.

to install latest development version

```sh
pip install git+https://github.com/dmidlo/histdata.com-tools.git
```

### Container Image

Version tags publish a non-root Linux AMD64/ARM64 image to GHCR. Keep data,
runtime state, and the verified Temporal cache in one named workspace volume:

```sh
docker volume create histdatacom-workspace
docker run --rm \
  --mount type=volume,source=histdatacom-workspace,target=/workspace \
  ghcr.io/dmidlo/histdata.com-tools:2.1.0 \
  --version
```

The image is a one-shot CLI, not a persistent service. See the maintained
[container guide](docs/container.md) for builds, data operations, fixed
UID/GID ownership, first-run Temporal provisioning, lifecycle constraints,
verification, publication policy, and cleanup.

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

- `.[docs]` installs the pinned Sphinx, MyST, and Read the Docs theme
  toolchain.
- `.[test]` installs pytest, coverage, pandas, pyarrow, DuckDB, InfluxDB
  support, notebook execution support, and test-only support around the base
  Temporal SDK dependency.
- `.[lint]` installs pre-commit and direct lint/type/doc hygiene tools.
- `.[release]` installs build and publish tooling.
- `.[dev]` is the aggregate local contributor environment with test, lint,
  release, and optional integration dependencies.

Build the same warning-as-error documentation tree used by CI and Read the
Docs with:

```sh
python -m pip install -e ".[docs]"
python -m sphinx -W --keep-going -b html docs docs/_build/html
```

Open `docs/_build/html/index.html` to inspect the generated site locally.

The `dev`, `docs`, `lint`, `test`, and `release` extras pin direct developer
tools where reproducibility matters. Runtime dependencies keep compatibility
lower bounds rather than lock-file pins because `histdatacom` is a published
PyPI library. The active lint baseline is Black, Ruff, mypy, generic file checks,
Pyroma, ShellCheck, Commitizen, and the local CLI smoke hook. The
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
default PyPI path. The GitHub Release workflow builds them only for explicit
build-only dry runs: set `include_bundled_platform_wheels=true` with
`release_target=build-only`, and set
`bundled_platform_wheel_size_confirmed=true` only after confirming the
private/offline purpose and artifact-size policy.

For bundled platform-wheel release dry runs, Linux and macOS remain
worker-starting runtime smoke gates. Windows currently verifies installability,
bundled runtime metadata, Temporal executable version, and CLI entry points, and
collects layered startup diagnostics. Until #314's native Windows worker-start
blocker is fixed, the Windows bundled runtime gate is install/CLI-only.

Use `release_target=build-only` for metadata-only dry runs,
`release_target=testpypi` for the first publish rehearsal, and
`release_target=pypi` only after setting `testpypi_dry_run_confirmed=true`. The
final `histdatacom-dist` artifact contains only the metadata-only universal
wheel and source distribution; JSON build and checksum reports are uploaded
separately as release reports. Bundled platform-wheel artifacts are uploaded
separately only for the explicit private/offline build-only path and are not
consumed by TestPyPI/PyPI publish jobs.

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

Routine development and the Python/OS CI matrix run the full test suite without
coverage. Coverage runs once, in the dedicated `Production coverage` job, only
when a pull request promotes `dev` into `main`. That required production gate
runs pytest through `pytest-cov`, enforces the `.coveragerc` threshold, and
uploads one `coverage.xml` plus `htmlcov/` artifact. Ordinary commits, pushes,
issue closure, non-production pull requests, workflow dispatches, and pushes to
`main` do not execute coverage. The first-pass gate is total-only. Per-package
or domain thresholds belong with the broader testing work tracked in issues #9
and #68.

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
local tick fixtures through the packaged `DataQualityWorkflow` without contacting
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
