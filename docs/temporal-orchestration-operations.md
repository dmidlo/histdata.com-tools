# Temporal Orchestration User Guide

Issue: #248

This guide covers the user-facing orchestration workflow for `histdatacom`:
submitting work, monitoring progress, inspecting outputs, and recovering from
failures. It intentionally avoids maintainer-only runtime details such as PID
files, port allocation, SQLite internals, worker lanes, and release-smoke
mechanics. Those belong in
[`temporal-orchestration-runtime-runbook.md`](temporal-orchestration-runtime-runbook.md).

## Supported Public Surface

The V1 public surface for users, automation, services, and the future GUI is:

- the `histdatacom` CLI documented in `README.md`
- `histdatacom.Options` passed to `histdatacom.main(options)` or
  `histdatacom(options)`
- `histdatacom.orchestration.contracts.RunRequest`
- `histdatacom jobs ...` for job telemetry and control
- `histdatacom.orchestration.client` helpers for submit, inspect, list, cancel,
  resume, progress, logs, results, and artifact polling
- `histdatacom.orchestration.telemetry` helpers for reading job status,
  progress, logs, results, and artifacts

Do not build new user automation by importing the internal runtime
implementation package directly. Temporal activities use lower-level helpers
such as `histdatacom.activity_stages` and bounded adapters, but that worker
boundary is not the user or GUI automation boundary.

## Default Runtime Behavior

The local Temporal orchestration runtime is the production default for CLI and
API work. A normal command submits a `RunRequest`, starts the local runtime when
no healthy runtime is running, waits for completion by default, and materializes
the same user-facing output shape that the command promises.

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

The runtime never silently falls back to a foreground execution path. If
orchestration cannot start or cannot be contacted, CLI calls exit nonzero with a
clear error and API calls raise `OrchestrationUnavailableError`.

The foreground rollback flag is gone:

- `--foreground` is not accepted by the CLI.
- `Options.use_orchestration = False` is rejected by API runtime resolution.
- Old foreground rollback option names are not supported public API.

## Install Expectations

The base package install includes the Temporal Python SDK because orchestration
is part of the default package behavior:

```sh
pip install histdatacom
```

The Temporal server executable is provisioned separately by the runtime
resolver. Normal PyPI and TestPyPI artifacts are metadata-only: they ship the
package-owned runtime index, not the executable binary. On first use the
resolver uses an explicit operator override, an offline/private bundle, a
verified per-user cache entry, or a pinned first-run download.

For provisioning policy, offline behavior, cache integrity, and release
ownership, see
[`temporal-binary-provisioning.md`](temporal-binary-provisioning.md). For
maintainer runtime commands and local state layout, see the runtime runbook.

## Submit CLI Work

Submit work and wait for the result:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Interactive waited CLI requests render a live Rich progress view while the
Temporal job is running; piped output and API calls keep the machine-readable
result path.

Require an already-running healthy runtime:

```sh
histdatacom --no-orchestration-start \
  -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Submit work without waiting for the workflow result:

```sh
histdatacom --submit-only --no-overlap --schedule-key eurusd-cache \
  -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

`--no-overlap` is opt-in and checks persisted job state in the current runtime
workspace before submission. Use a stable `--schedule-key` for recurring
scheduled work; if no key is supplied, the guard falls back to a deterministic
request fingerprint.

The main operation flags keep their documented meaning before the request is
submitted:

- `-A` / `--available_remote_data`
- `-U` / `--update_remote_data`
- `-V` / `--validate_urls`
- `-D` / `--download_data_archives`
- `-X` / `--extract_csvs`
- `-C` / `--build-cache`
- `-I` / `--import_to_influxdb`

Use `--build-cache` for low-disk cache-building runs. It validates and
downloads supported ASCII `M1` or tick quote datasets, builds canonical Polars
`.data` caches, removes transient ZIP/CSV sources after each cache is ready,
and does not merge the caches into a dataframe result.

Waited repository requests keep the documented output contract: CLI calls render
the repository table, API calls return the available-data dictionary, and
submit-only repository requests return job metadata.

## Submit API Work

API calls use the same public `Options` object and runtime defaults:

```python
import histdatacom
from histdatacom.options import Options

options = Options()
options.orchestration_wait_result = True
options.api_return_type = "polars"
options.formats = {"ascii"}
options.timeframes = {"1-minute-bar-quotes"}
options.pairs = {"eurusd"}
options.start_yearmonth = "now"

data = histdatacom(options)
```

Set `options.build_cache = True` when automation should build reusable `.data`
caches and remove source ZIP/CSV files without returning a dataframe.

When `orchestration_wait_result` is `True`, API calls with `api_return_type`
return the requested `polars`, `pandas`, or `arrow` object by materializing
completed cache artifacts on disk. When `orchestration_wait_result` is `False`,
the call returns the orchestration job payload.

Set `options.orchestration_start = False` when an API caller should require a
pre-started runtime instead of starting one.

## Job Telemetry

`histdatacom jobs ...` is the user-facing telemetry and control surface. It is
JSON-friendly for shell automation and future GUI polling.

List recent jobs:

```sh
histdatacom jobs list --json
histdatacom jobs list --schedule-key eurusd-cache --active --json
```

Inspect one job:

```sh
histdatacom jobs inspect histdatacom-<request-id> --json
```

Read progress and logs:

```sh
histdatacom jobs progress histdatacom-<request-id> --watch
histdatacom jobs progress histdatacom-<request-id> --json
histdatacom jobs logs histdatacom-<request-id> --json
```

Omit `--json` on `jobs progress` for the Rich terminal progress view; add
`--watch` to live-refresh it until the job reaches a terminal state.

Read artifacts and results:

```sh
histdatacom jobs artifacts histdatacom-<request-id> --json
histdatacom jobs result histdatacom-<request-id> --json
```

Submit a serialized request:

```sh
histdatacom jobs submit --start --submit-only --no-overlap \
  --schedule-key eurusd-cache --request-json request.json --json
```

Use `jobs list --schedule-key <key> --active` to find the non-terminal job that
would block a scheduled `--no-overlap` submission. Jobs that rely on the fallback
request fingerprint can be found with `--schedule-fingerprint sha256:...`.
`jobs inspect --json` includes a `schedule_identity` object with the key or
fingerprint, active/terminal state, and whether the job currently blocks a
duplicate submission.

The workflow ID format is `histdatacom-<request_id>`.

Job telemetry stores bounded metadata: request IDs, workflow IDs, status events,
progress, artifact references, control state, and result metadata. Dataframe
rows, archive bytes, cache contents, and large result payloads stay on disk and
are represented by artifact references.

## Offline Job Inspection

Read-only job commands can inspect the last persisted local snapshot when
Temporal is unavailable:

```sh
histdatacom jobs --offline list --json
histdatacom jobs --offline inspect histdatacom-<request-id> --json
histdatacom jobs --offline progress histdatacom-<request-id> --json
histdatacom jobs --offline logs histdatacom-<request-id> --json
histdatacom jobs --offline artifacts histdatacom-<request-id> --json
histdatacom jobs --offline result histdatacom-<request-id> --json
```

Offline output is bounded and diagnostic. It is useful after a client crash,
runtime shutdown, or failed runtime startup. It is not a portable export format;
copy referenced artifacts and manifest data together when preparing a diagnostic
bundle.

## Cancel, Retry, And Resume

Cancel a job:

```sh
histdatacom jobs cancel histdatacom-<request-id> --reason "operator stop"
```

Retry a failed job:

```sh
histdatacom jobs retry histdatacom-<request-id> --reason "transient failure"
```

Resume a stopped or interrupted job:

```sh
histdatacom jobs resume histdatacom-<request-id> --reason "continue run"
```

Retry and resume are executable control operations. The client reads the
persisted `RunRequest` snapshot and starts a deterministic replacement workflow.
Complete ZIP, CSV/XLSX, and Polars cache artifacts are reused by default through
the normal stage helpers. Pass `--recompute-complete` only when the operator
intentionally wants a recompute-oriented replacement run.

Cancellation is cooperative at activity boundaries. Partial temp artifacts are
not promoted as complete outputs.

## Data Quality Work

`histdatacom --quality` runs offline checks against datasets that already exist
on disk. It submits a `DataQualityWorkflow`, uses local CPU/file activities, and
writes the detailed JSON report as a disk artifact. It does not contact
HistData.com or InfluxDB.

```sh
histdatacom --quality \
  --quality-target data/DAT_ASCII_EURUSD_M1_201202.csv \
  --quality-checks ingestion \
  --quality-report reports/quality-clean.json
```

Use `--repo-quality` to run the same offline quality workflow and write bounded
summary metadata back to the local `.repo` helper file:

```sh
histdatacom --repo-quality \
  --quality-target data/ASCII/M1/EURUSD \
  --quality-report reports/eurusd-quality.json
```

Ordinary `-A` and `-U` repository commands do not run quality checks. Use
`histdatacom -A --repo-quality-columns` to display previously stored quality
summaries in repository table output.

Quality mode supports focused groups with `--quality-checks`: `inventory`,
`ingestion`, `time`, `bars`, `ticks`, `domain`, `modeling`, and `provenance`.
The default is `all`.

Use `--quality-preflight` before large cache-backed quality batteries. It runs
a bounded `.data` sample locally, writes optional publish-safe JSON evidence
with `--quality-preflight-report PATH`, and reports a direct decision: safe,
warned, failed, or no matching targets. Validation rows stay `not-run` unless
`--quality-preflight-validation-report PATH`, the explicit
`--quality-preflight-validation-report latest` discovery mode, or
`--quality-preflight-run-validation` is supplied. When launching the full run,
pass that saved report with `--quality-preflight-evidence PATH`; if the evidence
does not match the target scope, current package version, freshness window,
Temporal `data_quality` budget, or cache inventory, the CLI warns and continues
without prompting. Use `--quality-preflight-evidence-max-age-seconds SECONDS` to
tune the freshness window, or `--quality-preflight-evidence-stale-ok` to bypass
only the age check explicitly.

Warnings are advisory by default. Errors make a target failed and make the
process exit nonzero under the default `--quality-fail-on error` policy. CI jobs
that want warnings to fail should pass
`--quality-fail-on warning --quality-max-warnings 0`; report-only jobs can pass
`--quality-fail-on never`.

The checks encode HistData-specific assumptions: ASCII M1 files are bid OHLC
bars, ASCII tick files include bid and ask, and source timestamps are fixed EST
with no daylight-saving adjustment before UTC normalization.

## Common User Failures

Runtime unavailable:

- Symptom: CLI exits nonzero or API raises `OrchestrationUnavailableError`.
- First action: run `histdatacom runtime doctor --json` and include that output
  in a support report. Maintainer-level interpretation is covered by the runtime
  runbook.

Temporal executable unavailable:

- Symptom: the runtime cannot resolve a Temporal executable on first use.
- First action: allow first-run provisioning, pre-seed the verified cache, pass
  an explicit executable if you are an operator, or install an offline/private
  bundle.

InfluxDB unavailable:

- Symptom: import activities fail or retry because no live Influx target is
  configured.
- First action: install `histdatacom[influx]`, provide `influxdb.yaml`, or omit
  `-I/--import_to_influxdb`. The orchestration runtime does not provide an
  InfluxDB service.

Data-quality findings:

- Symptom: quality mode exits nonzero or reports warnings.
- First action: inspect the detailed JSON report passed with `--quality-report`.
  Console output is intentionally bounded.

## Maintainer And Contributor Docs

Use these documents when the normal CLI/job surface is not enough:

- [`temporal-orchestration-runtime-runbook.md`](temporal-orchestration-runtime-runbook.md)
  for runtime lifecycle, resolver diagnostics, local state layout, ports,
  worker lanes, maintenance, persistence schema handling, and low-level
  troubleshooting.
- [`temporal-binary-provisioning.md`](temporal-binary-provisioning.md) for the
  Temporal executable provisioning model.
- [`temporal-workflow-topology.md`](temporal-workflow-topology.md) for workflow,
  activity, task queue, and testing boundaries.
- [`temporal-orchestration-performance.md`](temporal-orchestration-performance.md)
  for lane sizing and benchmark policy.
