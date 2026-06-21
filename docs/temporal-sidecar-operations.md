# Temporal Sidecar Operations

Issue: #169

This runbook documents the local Temporal sidecar model used by
`histdatacom`. It is written for users, operators, contributors, and the
future GUI surface. Data-quality operations are intentionally deferred until
the runtime foundation is stable.

## Current Packaging Status

The ordinary foreground CLI and API path remains the default:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Sidecar-backed execution is opt-in through `--sidecar` or
`Options.use_sidecar = True`.

Install the Temporal client and worker dependency surface when using sidecar
job submission, job inspection, or workers:

```sh
pip install "histdatacom[temporal]"
```

`histdatacom[all]` also includes the Temporal dependency surface. The current
wheel includes sidecar metadata, resource manifests, and CLI entry points. The
Temporal server executable is still metadata-only in the current package
artifacts, so development and operator smoke tests should pass an explicit
Temporal executable path to `histdatacom-sidecar start --executable` until
platform wheels bundle that binary.

## Runtime Model

The sidecar runs a local Temporal developer server with SQLite persistence.
The server is scoped by workspace so concurrent projects do not share process
state, logs, task queues, or SQLite history unless they intentionally use the
same workspace path.

The sidecar stores only orchestration state:

- process state
- transient supervisor locks
- Temporal SQLite persistence
- runtime manifests
- server and worker logs

Downloaded ZIP files, extracted CSV/XLSX files, cache IPC files, and merged
API-return artifacts stay under the existing HistData data-directory policy.
They are not moved into the sidecar runtime home.

## Runtime Paths

The default runtime home is per user and platform-specific:

- macOS: `~/Library/Application Support/histdatacom/sidecar`
- Linux: `$XDG_STATE_HOME/histdatacom/sidecar`, or
  `~/.local/state/histdatacom/sidecar`
- Windows: `%LOCALAPPDATA%\histdatacom\sidecar`, or
  `~/AppData/Local/histdatacom/sidecar`

Override the base directory with `HISTDATACOM_SIDECAR_HOME` or
`--runtime-home`.

Each workspace gets a deterministic directory:

```txt
<runtime-home>/workspaces/<workspace-name>-<workspace-hash>/
```

The workspace defaults to the launch directory. Automation, GUI launchers, and
service managers should pass `--workspace` or set
`HISTDATACOM_SIDECAR_WORKSPACE` so the sidecar is not accidentally scoped to a
different current working directory.

Workspace runtime contents:

| Path | Purpose |
| --- | --- |
| `state/sidecar.pid.json` | Persisted server PID, command, ports, and log paths |
| `state/sidecar.lock` | Transient supervisor lock while start/stop mutates state |
| `logs/temporal-server.log` | Temporal server stdout/stderr |
| `logs/temporal-worker.log` | Reserved worker log path for packaged worker supervision |
| `sqlite/temporal.db` | Temporal developer-server SQLite persistence |
| `manifests/runtime-policy.json` | Resolved runtime path, port, and workspace policy |

## Ports

The sidecar binds to `127.0.0.1` by default. Override with
`HISTDATACOM_SIDECAR_IP` only when a local operator intentionally needs a
different bind address.

The default gRPC port is selected deterministically from the workspace hash in
the `17233-19232` range. The UI port is the selected gRPC port plus `1000`.
If the derived port pair is unavailable, the allocator scans a bounded
deterministic window and records collisions in the runtime policy.

Explicit port overrides:

- `HISTDATACOM_SIDECAR_PORT`
- `HISTDATACOM_SIDECAR_UI_PORT`

Explicit port collisions fail with a clear error instead of silently selecting
a different port.

## Lifecycle Commands

Both spellings are supported where installed: the stable console script is
`histdatacom-sidecar`, and the top-level command routes `histdatacom sidecar`.

Check diagnostics:

```sh
histdatacom-sidecar doctor --json
```

Check status:

```sh
histdatacom-sidecar status --json
```

Start with an explicit Temporal executable while platform wheels are
metadata-only:

```sh
histdatacom-sidecar start --executable /path/to/temporal
```

Stop or restart:

```sh
histdatacom-sidecar stop
histdatacom-sidecar restart --executable /path/to/temporal
```

Scope every lifecycle command to a stable workspace when running from cron,
launchd, systemd, scheduled tasks, or a future GUI shell:

```sh
histdatacom-sidecar --workspace /path/to/project status --json
```

## Sidecar Job Submission

Foreground behavior stays unchanged unless `--sidecar` is set:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Submit the same request through the sidecar and start it if no healthy server
is already running:

```sh
histdatacom --sidecar --sidecar-start -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Submit without waiting for the workflow result:

```sh
histdatacom --sidecar --sidecar-start --sidecar-submit-only -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Sidecar-backed API calls use the same public options:

```python
import histdatacom
from histdatacom.options import Options

options = Options()
options.use_sidecar = True
options.sidecar_start = True
options.sidecar_wait_result = True
options.api_return_type = "polars"
options.formats = {"ascii"}
options.timeframes = {"1-minute-bar-quotes"}
options.pairs = {"eurusd"}
options.start_yearmonth = "now"

data = histdatacom(options)
```

When `sidecar_wait_result` is `True`, API calls with `api_return_type` return
the requested `polars`, `pandas`, or `arrow` object by materializing completed
cache artifacts on disk. When `sidecar_wait_result` is `False`, the call
returns the sidecar job payload instead.

## Job Control Commands

The sidecar control surface is intentionally JSON-friendly for CLI automation
and future GUI polling.

Submit a serialized `RunRequest`:

```sh
histdatacom-sidecar jobs submit --start --submit-only --request-json request.json --json
```

Inspect and control jobs:

```sh
histdatacom-sidecar jobs list --json
histdatacom-sidecar jobs inspect histdatacom-<request-id> --json
histdatacom-sidecar jobs progress histdatacom-<request-id> --json
histdatacom-sidecar jobs logs histdatacom-<request-id> --json
histdatacom-sidecar jobs artifacts histdatacom-<request-id> --json
histdatacom-sidecar jobs result histdatacom-<request-id> --json
histdatacom-sidecar jobs cancel histdatacom-<request-id> --reason "operator stop"
histdatacom-sidecar jobs retry histdatacom-<request-id> --reason "transient failure"
histdatacom-sidecar jobs resume histdatacom-<request-id> --reason "continue run"
```

The workflow ID format is `histdatacom-<request_id>`.

## Workers And Task Queues

Workers use workspace-scoped task queues. Defaults:

- namespace: `default`
- task queue prefix: `histdatacom`
- lane names: `orchestration`, `network`, `cpu-file`, `influx`

The queue name pattern is:

```txt
histdatacom.<workspace-id>.<lane>
```

Inspect worker configuration:

```sh
histdatacom-sidecar-worker config --lane network --json
```

Run a worker lane:

```sh
histdatacom-sidecar-worker run --lane orchestration
histdatacom-sidecar-worker run --lane network
histdatacom-sidecar-worker run --lane cpu-file
histdatacom-sidecar-worker run --lane influx
```

Concurrency is derived from the existing `--cpu-utilization` policy:

- orchestration: `1`
- network: CPU worker count multiplied by `--network-multiplier`
- cpu-file: CPU worker count
- influx: `1`

Use `--max-concurrent-activities` when tuning only the selected lane.

## Workflow And Activity Boundaries

The parent workflow is `HistDataRunWorkflow`, one user-visible CLI/API/GUI
job. It runs coarse children:

- `RepositoryRefreshWorkflow`
- `DatasetPlanWorkflow`
- `SymbolTimeframeWorkflow`

`SymbolTimeframeWorkflow` runs operation-family children:

- `ValidateUrlsWorkflow`
- `DownloadArchivesWorkflow`
- `ExtractCsvWorkflow`
- `BuildCacheWorkflow`
- `MergeCacheWorkflow`
- `ImportWorkflow`

Leaf workflows call Temporal activities for repository refresh, dataset
planning, URL validation, archive download, CSV extraction, cache building,
cache merge, and Influx import. Payloads stay bounded to request metadata,
partition IDs, status events, and artifact references. Rows, dataframes, ZIP
bytes, and CSV contents stay on disk.

See `docs/temporal-workflow-topology.md` for the contributor-facing topology
contract and `docs/temporal-sidecar-performance.md` for lane sizing and
benchmark policy.

## Cancellation And Resume

Cancellation is cooperative at the activity boundary. The policy is designed
to avoid promoting partial artifacts:

| Stage | Partial artifacts | Resume behavior |
| --- | --- | --- |
| `repository_refresh` | Remove hidden `.repo.*.tmp` files | Reuse complete repository metadata or refresh again |
| `dataset_plan` | No filesystem side effects | Replay deterministic planning |
| `validate_urls` | Reuse complete metadata only | Repeat unfinished validation |
| `download_archives` | Remove temp ZIP files | Reuse complete ZIP/CSV/cache artifacts or redownload |
| `extract_csv` | Remove temp CSV/XLSX files | Reuse complete CSV/cache artifacts or extract again |
| `build_cache` | Remove temp IPC cache files | Reuse complete cache or rebuild |
| `merge_cache` | No promoted partial merged data | Replay merge assembly from complete caches |
| `import_to_influx` | External bounded batches | Retry idempotent batches from cache metadata |

## Troubleshooting

Dependency install problems:

- Symptom: `Temporal support requires the optional dependency surface`.
- Fix: install `histdatacom[temporal]` in the active virtual environment.

Temporal executable not bundled:

- Symptom: `doctor` reports `executable_bundled: false`, or start cannot find a
  packaged executable.
- Fix: pass `--executable /path/to/temporal` until platform wheels include the
  sidecar binary.

Sidecar unavailable:

- Symptom: CLI exits nonzero or API raises `SidecarUnavailableError`.
- Fix: run `histdatacom-sidecar status --json`, start the sidecar manually, or
  use `--sidecar-start` / `Options.sidecar_start = True`.

Port collisions:

- Symptom: start fails with a port allocation error.
- Fix: inspect `histdatacom-sidecar doctor --json`, set
  `HISTDATACOM_SIDECAR_PORT` and optionally
  `HISTDATACOM_SIDECAR_UI_PORT`, or choose a different workspace.

Stale PID state:

- Symptom: status is `stale`, or the PID file references dead processes.
- Fix: `histdatacom-sidecar stop` removes stale PID and lock state. If a lock
  is held by a live process, do not delete it manually; stop the owner first.

Corrupted SQLite or runtime state:

- Symptom: the server repeatedly fails to start for the same workspace after a
  crash or interrupted disk write.
- Fix: stop the sidecar, preserve `logs/` and `sqlite/temporal.db` for
  diagnosis, then move aside or delete the workspace runtime directory. HistData
  ZIP, CSV, and cache artifacts are outside this runtime directory.

Worker crashes:

- Symptom: jobs remain queued or lane progress stops.
- Fix: inspect `logs/temporal-worker.log`, run
  `histdatacom-sidecar-worker config --lane <lane> --json`, then restart the
  affected worker lane with the same workspace and runtime home.

InfluxDB unavailable:

- Symptom: import activities fail or retry because no live Influx target is
  configured.
- Fix: install `histdatacom[influx]`, provide `influxdb.yaml`, or skip
  `-I/--import_to_influxdb`. The sidecar does not provide an InfluxDB service.

Data-quality checks:

- The runtime records status, artifacts, and failures. It does not yet run the
  future data-quality assessment operations. Those checks remain deferred to
  the data-quality issue block.

## Contributor Testing Strategy

Unit tests should keep most coverage independent from live Temporal and Influx:

- runtime path and port policy tests
- resource manifest and package-data tests
- supervisor lifecycle tests with fake process factories
- queue and worker configuration tests
- workflow topology and status-query tests
- activity tests with fixture HTML/CSV/cache data
- control API and job payload tests
- performance profile and benchmark fixture tests

Live smoke tests belong behind explicit operator setup because they require a
Temporal executable and, for import coverage, an Influx target. Package release
smoke should keep validating metadata, console entry points, packaged sidecar
resources, and offline `status`/`doctor` behavior until platform wheels bundle
the executable.

## GUI Integration Notes

The future GUI should treat the sidecar as a local workspace service:

- choose and persist a workspace path before starting the sidecar
- use JSON lifecycle commands for status and diagnostics
- submit requests through the same `RunRequest` contract as the CLI/API
- poll `jobs progress`, `jobs logs`, and `jobs artifacts`
- display `runtime-policy.json` paths when asking users for troubleshooting
- avoid direct reads of Temporal SQLite unless a dedicated diagnostic feature is
  being implemented
