# Temporal Sidecar Operations

Issue: #169

This runbook documents the local Temporal sidecar model used by
`histdatacom`. It is written for users, operators, contributors, and the
future GUI surface. Data-quality operations are intentionally deferred until
the runtime foundation is stable.

## Current Packaging Status

The local Temporal sidecar is now the production default for ordinary CLI and
API runs:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Default requests submit a `RunRequest` to the sidecar and start the bundled
local sidecar when no healthy sidecar is running. `--sidecar` remains accepted
as a compatibility alias for scripts that already passed it during migration.

Default sidecar submissions are built from resolved runtime context and
`RunRequest` payloads. The foreground rollback runtime has been removed after
its release-window deprecation period: `--foreground` is no longer accepted,
and API code that sets `Options.use_sidecar = False` receives a clear
`ValueError`. Legacy helper surfaces accept explicit argument dictionaries
rather than ambient parser state. New orchestration behavior should be
expressed as `RunRequest` payloads, Temporal workflows, and Temporal
activities.

The base package install includes the Temporal Python SDK because sidecar job
submission, job inspection, and workers are the default runtime surface:

```sh
pip install histdatacom
```

`histdatacom[temporal]` remains accepted as a backwards-compatible extra for
automation written during migration, and `histdatacom[all]` still includes the
same SDK dependency. Source distributions and universal fallback wheels include
sidecar metadata, resource manifests, and CLI entry points. Platform wheels can
include the Temporal server executable as package data. On a bundled platform wheel,
`histdatacom-sidecar start` resolves the packaged executable through
`importlib.resources`; on metadata-only artifacts and unsupported platforms,
development and operator smoke tests should pass an explicit Temporal
executable path to `histdatacom-sidecar start --executable`. The Python SDK and
the server executable are separate distribution concerns: base installs provide
the SDK, while bundled platform wheels provide the executable.

Release automation builds the metadata-only sdist/fallback wheel first, then
builds bundled Temporal platform wheels for Linux x86_64, Linux arm64, macOS
Intel, macOS arm64, and Windows x86_64. The bundled wheels use pinned Temporal
CLI `1.7.2` release artifacts and verify SHA-256 checksums before bundling. The
platform wheel payload includes `temporal-cli-provenance.json` plus the Temporal
CLI notice and MIT license under `third-party/temporal-cli/`; metadata-only
fallback artifacts intentionally omit executable provenance. Every bundled wheel
must pass `scripts/inspect_wheel.py --require-bundled-platform`, install on its
matching GitHub-hosted runner, run
`histdatacom-sidecar doctor --json` with
`platform.executable_bundled == true`, probe the executable version, start the
sidecar without `--executable`, and run the installed-wheel live sidecar smoke
job. The live smoke uses a minimal non-Influx request, waits for job completion,
validates the status snapshot and artifact references, and prints server/worker
diagnostics if the job or sidecar shutdown fails. Stop exceptions, missing stop
status, persistent `stopping` status, and known remaining sidecar PIDs are
treated as smoke failures. This live smoke is executed through
`scripts/smoke_sidecar_install.py --live-sidecar-smoke`, not as a default
pytest test, so missing Temporal executables fail the explicit smoke command
instead of appearing as skipped tests in the normal suite.

Rollback behavior is intentionally conservative. If a platform executable or
wheel is bad after publish, yank the affected platform wheel and cut a
replacement release. The sdist and universal fallback wheel are metadata-only
recovery artifacts; they keep the package installable while operators provide
an explicit Temporal executable path.

Default-runtime failure policy:

- Default CLI/API runs use the sidecar and start it when no healthy sidecar is
  running.
- `--foreground` is rejected by the CLI.
- `Options.use_sidecar = False` is rejected by API runtime resolution.
- Metadata-only wheels and unsupported platforms fail sidecar starts with a
  `SidecarUnavailableError`/nonzero CLI exit unless an operator supplies
  `histdatacom-sidecar start --executable /path/to/temporal`.
- The runtime never silently falls back from sidecar execution.

## Runtime Model

The sidecar runs a local Temporal developer server with SQLite persistence plus
one worker process for each configured task-queue lane. The runtime is scoped
by workspace so concurrent projects do not share process state, logs, task
queues, or SQLite history unless they intentionally use the same workspace
path.

The sidecar stores only orchestration state:

- process state
- transient supervisor locks
- Temporal SQLite persistence
- runtime manifests
- server and worker logs

Downloaded ZIP files, extracted CSV/XLSX files, cache IPC files, and merged
API-return artifacts stay under the existing HistData data-directory policy.
They are not moved into the sidecar runtime home.

Record status metadata is manifest-only for new writes. Sidecar and API paths
update `.histdatacom/manifest-status.sqlite3` under the
resolved data or sidecar status root and no longer create new hidden `.meta`
files beside individual records. Existing `.meta` files are migration inputs:
successful restore/import operations write the manifest row and remove the
legacy file, while missing or corrupt legacy files are reported and otherwise
ignored.

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
| `state/sidecar.pid.json` | Persisted component PIDs, commands, ports, worker fleet config, and log paths |
| `state/sidecar.lock` | Transient supervisor lock while start/stop mutates state |
| `logs/temporal-server.log` | Temporal server stdout/stderr |
| `logs/temporal-worker-<lane>.log` | Worker lane stdout/stderr |
| `sqlite/temporal.db` | Temporal developer-server SQLite persistence |
| `manifests/runtime-policy.json` | Resolved runtime path, port, and workspace policy |
| `manifests/.histdatacom/manifest-status.sqlite3` | Durable sidecar job snapshots, status events, and artifact references |

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

Start with the packaged executable on a bundled platform wheel:

```sh
histdatacom-sidecar start
```

Start with an explicit Temporal executable on metadata-only artifacts or when
testing an operator-provided executable:

```sh
histdatacom-sidecar start --executable /path/to/temporal
```

Stop or restart:

```sh
histdatacom-sidecar stop
histdatacom-sidecar restart --executable /path/to/temporal
```

Lifecycle `start` and `restart` start the Temporal server, wait until the
frontend port accepts connections, and then launch the worker lane fleet.
Worker lane settings can be passed to start/restart:

```sh
histdatacom-sidecar start \
  --executable /path/to/temporal \
  --namespace default \
  --task-queue-prefix histdatacom \
  --cpu-utilization medium \
  --network-multiplier 3
```

`status --json` and `doctor --json` include component health for `server`,
`worker:orchestration`, `worker:network`, `worker:cpu-file`, and
`worker:influx`. A live server without all required worker lanes is reported as
stale rather than healthy.

Scope every lifecycle command to a stable workspace when running from cron,
launchd, systemd, scheduled tasks, or a future GUI shell:

```sh
histdatacom-sidecar --workspace /path/to/project status --json
```

## Maintenance And Retention

Long-running PyPI installs and future GUI bundles should run sidecar maintenance
periodically for the same workspace they use to start jobs:

```sh
histdatacom-sidecar --workspace /path/to/project maintenance --json
```

`cleanup` is accepted as an alias for `maintenance`. The JSON payload is stable
for GUI use and reports log actions, status-store row counts, Temporal SQLite
size, warnings, and the data-directory policy. The safe default refuses to
mutate logs or SQLite-backed status rows while the sidecar is running; stop the
sidecar first, or pass `--allow-running` only when an operator intentionally
accepts active file-handle risk.

Default retention policy:

| State | Default |
| --- | --- |
| Active log file size | Rotate after 10 MiB |
| Rotated logs per log file | Keep 5 |
| Temporal SQLite history | Preserve by default, warn after 512 MiB |
| Durable job snapshots | Keep 500 newest jobs |
| Status events | Keep 1,000 newest rows per job or work item |
| Stage results | Keep 500 newest rows per work item |
| Artifact references | Keep 500 newest rows per job or work item |
| Spilled dataset plans | Keep 50 newest plans per request |

Each limit can be overridden on the maintenance command:

```sh
histdatacom-sidecar maintenance \
  --max-log-bytes 10485760 \
  --max-rotated-logs 5 \
  --max-job-snapshots 500 \
  --max-status-events-per-owner 1000 \
  --max-stage-results-per-work-item 500 \
  --max-artifacts-per-owner 500 \
  --max-dataset-plans-per-request 50 \
  --json
```

Maintenance is workspace-scoped and only mutates sidecar runtime state:

- log files under `logs/`
- durable sidecar manifest/status rows under
  `manifests/.histdatacom/manifest-status.sqlite3`

It does not remove downloaded HistData ZIP files, extracted CSV/XLSX files,
cache IPC files, merged API-return artifacts, or files referenced by artifact
rows. Temporal SQLite history is measured and preserved by default. If it grows
past the warning threshold, preserve `logs/` and `sqlite/temporal.db` for
diagnosis, stop the sidecar, and reset the workspace runtime directory only as
an explicit recovery action.

## Sidecar Job Submission

Submit through the default sidecar runtime and start it if no healthy server is
already running:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Pass `--sidecar` when existing automation still includes the old explicit flag:

```sh
histdatacom --sidecar --sidecar-start -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Require an already-running sidecar instead of autostarting one:

```sh
histdatacom --no-sidecar-start -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Submit without waiting for the workflow result:

```sh
histdatacom --sidecar --sidecar-start --sidecar-submit-only -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Sidecar-backed API calls use the same public options and sidecar defaults:

```python
import histdatacom
from histdatacom.options import Options

options = Options()
options.sidecar_wait_result = True
options.api_return_type = "polars"
options.formats = {"ascii"}
options.timeframes = {"1-minute-bar-quotes"}
options.pairs = {"eurusd"}
options.start_yearmonth = "now"

data = histdatacom(options)
```

Set `options.sidecar_start = False` when an API caller should require a
pre-started sidecar instead of starting one. `options.use_sidecar = False` is
no longer supported.

When `sidecar_wait_result` is `True`, API calls with `api_return_type` return
the requested `polars`, `pandas`, or `arrow` object by materializing completed
cache artifacts on disk. When `sidecar_wait_result` is `False`, the call
returns the sidecar job payload instead.

Waited sidecar repository requests preserve the historical output surface. API
calls using `available_remote_data` or `update_remote_data` return
the available-data dictionary, while CLI calls using `-A` or `-U` render the
repository table and use the same repository failure exit behavior. Submit-only
repository requests still return sidecar job metadata.

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

Job snapshots are persisted under the workspace-scoped sidecar runtime
manifests directory, not in HistData download/cache directories:

```txt
<sidecar-runtime>/<workspace-slug>/manifests/.histdatacom/manifest-status.sqlite3
```

Submit, inspect, progress, logs, artifacts, result, cancel, retry, and resume
commands write the latest bounded snapshot metadata to that store. The payloads
contain request IDs, workflow IDs, progress/status events, artifact references,
control state, and workflow result metadata; rows, dataframe contents, archive
bytes, and cache data remain on disk outside workflow history and outside the
job snapshot payload.

Run submissions also pass a compact `sidecar_status_store` reference through
request metadata. Activities use that reference to persist work-item state,
stage results, progress events, log entries, and artifact references after each
work item or bounded aggregate activity. This means `jobs progress`, `jobs
logs`, `jobs artifacts`, and `jobs result --offline` can still show the latest
observed state after a client crash or sidecar shutdown, even when no later
client-side inspect call ran. The write volume is proportional to work items and
activity progress events: one work-item upsert, one stage-result insert, and one
bounded job-snapshot merge per observed item or aggregate stage. Do not store
dataframe rows, archive bytes, cache contents, or large result payloads in this
SQLite database; use artifact references to point at those files on disk.

Use offline mode to inspect recent persisted jobs without connecting to
Temporal:

```sh
histdatacom-sidecar jobs --offline list --json
histdatacom-sidecar jobs --offline inspect histdatacom-<request-id> --json
histdatacom-sidecar jobs --offline progress histdatacom-<request-id> --json
histdatacom-sidecar jobs --offline logs histdatacom-<request-id> --json
histdatacom-sidecar jobs --offline artifacts histdatacom-<request-id> --json
histdatacom-sidecar jobs --offline result histdatacom-<request-id> --json
```

When Temporal is unavailable, read-only job commands fall back to this local
store when a matching snapshot exists.

## Dataset Plan References

Dataset planning stores full work-item metadata outside workflow history. The
`dataset_plan` activity writes the plan and its work items to
`<data-directory>/.histdatacom/manifest-status.sqlite3`, then returns a compact
`dataset_plan_ref` with the plan ID, store root, store path, schema version, and
work-item count.

Small plans remain simple: by default, plans with `64` or fewer work items can
still include inline `work_items`. Larger plans omit the full list and return
`dataset_plan_batches` instead. Each batch carries deterministic partition
metadata and a bounded comma-separated `work_ids` field. Child workflows pass
the reference and partition to leaf activities, and the first operation activity
hydrates only the assigned batch from the manifest store.

Override the inline threshold only for testing or targeted tuning:

```json
{
  "temporal_plan_spill": {
    "inline_work_item_limit": 32
  }
}
```

The plan reference is local to the workspace and data directory. It is not a
portable export format; copy the referenced artifacts and manifest database
together if a diagnostic bundle needs to reproduce a run elsewhere.

Retry and resume are executable control operations, not intent-only labels. The
client inspects the original job, reads the persisted `RunRequest` snapshot, and
starts a deterministic replacement `HistDataRunWorkflow` with a workflow ID like:

```txt
histdatacom-<request-id>-retry-<stage>-001
histdatacom-<request-id>-resume-<stage>-001
```

The original job snapshot is updated to `retry_requested` or
`resume_requested`; the replacement job snapshot is returned as `retrying` or
`resuming`. Both snapshots carry bounded `control_execution` metadata with the
parent workflow ID, previous run ID, replacement handle, stage-specific resume
policy, cleanup decisions, attempt number, and whether complete artifacts should
be reused. Complete ZIP, CSV/XLSX, and Polars cache artifacts are reused by
default through the existing stage helpers; pass `--recompute-complete` to mark a
replacement run as explicitly recompute-oriented. Known hidden temp artifacts are
removed before replacement submission when the stage resume policy says partials
must be removed.

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

The lifecycle supervisor normally starts every worker lane. Run an individual
worker lane manually only for debugging or targeted recovery:

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

- Symptom: `Temporal support requires temporalio`.
- Fix: reinstall `histdatacom` with dependencies enabled in the active virtual
  environment. This usually means avoiding `--no-deps`; the compatibility
  extra `histdatacom[temporal]` remains accepted for migration-era install
  scripts.

Temporal executable not bundled:

- Symptom: `doctor` reports `executable_bundled: false`, or start cannot find a
  packaged executable.
- Fix: install a platform wheel that bundles the current platform executable,
  or pass `--executable /path/to/temporal` for development and operator tests.

Sidecar unavailable:

- Symptom: CLI exits nonzero or API raises `SidecarUnavailableError`.
- Fix: run `histdatacom-sidecar status --json` and `histdatacom-sidecar doctor
  --json`, start the sidecar manually with an explicit executable when using a
  metadata-only artifact, and inspect server/worker logs from the doctor output.

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
- Fix: inspect `histdatacom-sidecar doctor --json` and the affected
  `logs/temporal-worker-<lane>.log`, then run `histdatacom-sidecar restart`
  with the same workspace and runtime home.

InfluxDB unavailable:

- Symptom: import activities fail or retry because no live Influx target is
  configured.
- Fix: install `histdatacom[influx]`, provide `influxdb.yaml`, or skip
  `-I/--import_to_influxdb`. The sidecar does not provide an InfluxDB service.
  Local contract tests replace only the final writer and do not prove live
  credentials, bucket permissions, network latency, or server-side write
  rejection behavior.

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
- contract-backed `ImportWorkflow` tests with a local Influx writer substitute
- control API and job payload tests
- performance profile and benchmark fixture tests

Live smoke tests belong behind explicit operator setup because they require a
Temporal executable, and live import coverage requires a real Influx target.
Package release smoke should validate metadata, console entry points, packaged
sidecar resources, and offline `status`/`doctor` behavior for every wheel. For
bundled platform wheels, release smoke should also require
`doctor.platform.executable_bundled == true`, run the packaged executable
version probe, and start the sidecar without `--executable`.

## GUI Integration Notes

The future GUI should treat the sidecar as a local workspace service:

- choose and persist a workspace path before starting the sidecar
- use JSON lifecycle commands for status and diagnostics
- submit requests through the same `RunRequest` contract as the CLI/API
- poll `jobs progress`, `jobs logs`, and `jobs artifacts`
- display `runtime-policy.json` paths when asking users for troubleshooting
- avoid direct reads of Temporal SQLite unless a dedicated diagnostic feature is
  being implemented
