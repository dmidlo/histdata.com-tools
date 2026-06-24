# Temporal Sidecar Operations

Issue: #169

This runbook documents the local Temporal sidecar model used by
`histdatacom`. It is written for users, operators, contributors, and the
future GUI surface. Data-quality operations run as offline CPU/file activities
and persist detailed reports as disk artifacts.

## Current Packaging Status

The local Temporal sidecar is now the production default for ordinary CLI and
API runs:

```sh
histdatacom -p eurusd -f ascii -t 1-minute-bar-quotes -s now
```

Default requests submit a `RunRequest` through Temporal orchestration and start
the local runtime when no healthy runtime is running.

Default sidecar submissions are built from resolved runtime context and
`RunRequest` payloads. The foreground rollback runtime has been removed after
its release-window deprecation period: `--foreground` is no longer accepted,
and API code that sets `Options.use_orchestration = False` receives a clear
`ValueError`. Removed sidecar-named option attributes raise `AttributeError`
instead of becoming stale state. Legacy helper surfaces accept explicit
argument dictionaries rather than ambient parser state. New orchestration
behavior should be expressed as `RunRequest` payloads, Temporal workflows, and
Temporal activities.

## Public API Boundary

The sidecar-era public boundary for new users, scripts, services, and the
future GUI is:

- `histdatacom.Options` passed to `histdatacom.main(options)` or
  `histdatacom(options)`
- `histdatacom.sidecar.contracts.RunRequest`
- the `histdatacom-sidecar` lifecycle and jobs CLI
- `histdatacom.sidecar.client` job-control helpers for submit, inspect, list,
  cancel, resume, progress, and artifact polling

The supported Temporal worker adapter boundary is lower level:

- `histdatacom.activity_stages.*`
- `histdatacom.sidecar.activities.*`
- `histdatacom.sidecar.workflows.*`
- bounded adapters such as `InfluxBatchWriter` when an activity needs a live
  sink

The following public helper methods are compatibility surfaces only. They
perform direct side effects and emit `LegacyHelperSideEffectWarning` so new GUI
or automation code cannot silently bypass durable job status, cancellation,
retry/resume, sidecar lifecycle, and worker-lane routing:

- `Repo.get_available_repo_data`
- `Repo.update_repo_from_github`
- `Scraper.plan_initial_records`
- `Scraper.validate_urls`
- `Scraper.download_zips`
- `Scraper.get_zip_file`
- `Api.test_for_cache_or_create`
- `Api.validate_caches`
- `Api.merge_caches`
- `Influx.import_data`

`Api.merge_records` remains a synchronous materializer for explicit cache
records, including the completed sidecar artifact path used by
`histdatacom.main(Options)`. It should not be used as an orchestration entry
point for validation, downloads, extraction, cache building, or imports.

The base package install includes the Temporal Python SDK because sidecar job
submission, job inspection, and workers are the default runtime surface:

```sh
pip install histdatacom
```

`histdatacom[temporal]` remains accepted as a backwards-compatible extra for
automation written during migration, and `histdatacom[all]` still includes the
same SDK dependency. Source distributions and universal wheels include runtime
metadata, resource manifests, third-party notices, and CLI entry points.

The accepted V1.0 packaging design keeps normal PyPI/TestPyPI artifacts
metadata-only and provisions the pinned Temporal executable through a verified
runtime cache on first use. The design is documented in
`docs/temporal-binary-provisioning.md`; #250 implemented the resolver and #251
hardens release preflight around that resolver.

Metadata-only artifacts resolve the Temporal executable from an explicit path,
an offline/private bundle, a verified per-user cache entry, or a pinned first-run
download. Bundled executable wheels remain an offline/private distribution path,
not the normal PyPI release path. The Python SDK and the server executable are
separate distribution concerns: base installs provide the SDK, while the runtime
resolver owns executable availability.

Release automation should build the metadata-only sdist/fallback wheel for
normal PyPI/TestPyPI publication, enforce the upload-size gate, and smoke a clean
install through the runtime resolver. Existing bundled platform-wheel tooling may
still build offline/private artifacts with pinned Temporal CLI `1.7.2` release
artifacts, SHA-256 verification, `temporal-cli-provenance.json`, and Temporal CLI
notice/license resources, but those wheels require an explicit operator decision.
Bundled wheels must pass `scripts/inspect_wheel.py --require-bundled-platform`,
install on a matching runner, run `histdatacom-sidecar doctor --json` with
`platform.executable_bundled == true`, probe the executable version, start the
runtime without `--executable`, and run the installed-wheel hermetic smoke job.
The hermetic smoke uses a local-only dataset-planning request:
`available_remote_data`, `update_remote_data`, `validate_urls`, download,
extract, and import flags are all false. It still starts the packaged Temporal
executable, starts workers, submits a workflow, waits for completion, validates
the status snapshot and artifact references, and prints server/worker
diagnostics if the job or sidecar shutdown fails. The explicit hermetic smoke is
executed through `scripts/smoke_sidecar_install.py --hermetic-sidecar-smoke`.
Bundled platform wheels also run
`scripts/smoke_sidecar_install.py --default-routing-sidecar-smoke`, which starts
the sidecar with non-default worker-fleet routing and submits without an
explicit worker config. That gate fails if the installed package cannot resolve
the running frontend, namespace, and task queues from persisted sidecar state.
Stop exceptions, missing stop status, persistent `stopping` status, and known
remaining sidecar PIDs are treated as smoke failures. These release gates are
not default pytest tests, so missing Temporal executables fail the explicit
smoke command instead of appearing as skipped tests in the normal suite.

The external HistData.com smoke remains available as an operator gate through
`scripts/smoke_sidecar_install.py --live-sidecar-smoke`. That command uses a
minimal non-Influx request with URL validation enabled, so it can detect vendor
availability, network, and website/form drift, but it should not be the default
PyPI publish gate for otherwise-good platform wheels.

Rollback behavior is intentionally conservative. If the Python artifact is bad,
yank it and cut a replacement release. If a remote Temporal artifact is bad or
unreachable, fix the packaged artifact index in a patch release. Explicit
operator executables, pre-seeded caches, and offline/private bundles remain
recovery paths.

Default-runtime failure policy:

- Default CLI/API runs use Temporal orchestration and start the local runtime
  when no healthy runtime is running.
- `--foreground` is rejected by the CLI.
- `Options.use_orchestration = False` is rejected by API runtime resolution.
- Sidecar-named option attributes such as `Options.use_sidecar`,
  `Options.sidecar_start`, and `Options.sidecar_wait_result` are removed and
  raise `AttributeError` if assigned.
- Metadata-only wheels and unsupported platforms fail sidecar starts with a
  `SidecarUnavailableError`/nonzero CLI exit when no explicit executable,
  verified bundle, verified cache entry, or allowed first-run download is
  available.
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

Start through the runtime resolver:

```sh
histdatacom-sidecar start
```

Start with an explicit Temporal executable override:

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

### Persistence Schema Handling

The sidecar manifest/status SQLite store tracks its schema with
`PRAGMA user_version`. Opening an unversioned v1 store marks it as the current
schema in place; opening a store with a newer unsupported schema fails clearly
without pruning rows. `maintenance --json` reports `status_store.schema_state`,
`status_store.schema_version`, and `status_store.expected_schema_version`.

The sidecar PID/state JSON also carries a schema version. Missing v1-era
`schema_version` values are treated as legacy state for compatibility, while
newer unsupported versions make `status --json` report stale state and
`doctor --json` report `persistence.sidecar_state.schema_state` as
`"unsupported"`. Operators should upgrade HistData.com Tools before reusing a
newer state file, or stop the sidecar and move the affected workspace runtime
directory aside after preserving logs and SQLite files for diagnosis.

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

Orchestrated API calls use the same public options and runtime defaults:

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

Set `options.orchestration_start = False` when an API caller should require a
pre-started runtime instead of starting one. `options.use_orchestration = False`
is no longer supported.

When `orchestration_wait_result` is `True`, API calls with `api_return_type`
return the requested `polars`, `pandas`, or `arrow` object by materializing
completed cache artifacts on disk. When `orchestration_wait_result` is `False`,
the call returns the orchestration job payload instead.

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

Client commands resolve routing from the running sidecar state before submitting
or controlling jobs. If `start` or `restart` used a non-default namespace, task
queue prefix, or dynamically reallocated frontend port, ordinary
`histdatacom` runs and `histdatacom-sidecar jobs ...` commands use the
persisted runtime and worker-fleet metadata. If that running state is stale or
missing worker-fleet metadata, job submission fails before enqueueing work; stop
and restart the sidecar to regenerate the state file.

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

Temporal executable unavailable:

- Symptom: `doctor` reports `executable_bundled: false`, or start cannot find a
  packaged or cached executable.
- Fix: pre-seed the verified runtime cache, allow first-run provisioning, pass
  `--executable /path/to/temporal`, or install an offline/private bundled
  artifact for development and operator tests.

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

Unsupported persistence schema:

- Symptom: `status --json` reports stale state with an unsupported sidecar
  schema version, `doctor --json` reports
  `persistence.sidecar_state.schema_state: "unsupported"`, or
  `maintenance --json` reports `status_store.schema_state: "unsupported"`.
- Fix: upgrade HistData.com Tools to a version that supports the newer schema.
  If the state came from a failed downgrade, stop the sidecar and move the
  workspace runtime directory aside only after preserving `logs/`,
  `sqlite/temporal.db`, and `manifests/.histdatacom/manifest-status.sqlite3`
  for diagnosis.

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
  rejection behavior. When Docker is available, run
  `python scripts/smoke_influx_docker.py` to start a disposable InfluxDB v2
  container, write representative HistData line protocol through the real
  `InfluxBatchWriter`, query the bucket, and tear the container down.

Data-quality checks:

- `histdatacom --quality` runs offline against local files and directories and
  submits a `DataQualityWorkflow` to the CPU/file lane. It is the operator path
  for assessing ZIP archives, extracted CSV files, extracted Excel `.xlsx`
  payloads, and `.data` cache files that already exist on disk; the workflow
  does not contact HistData.com or InfluxDB.
- The activity writes the detailed `quality-report` JSON artifact on disk and
  keeps workflow history limited to counters, policy decisions, progress,
  failures, and artifact references.
- Quality mode supports focused groups with `--quality-checks`: `inventory`,
  `ingestion`, `time`, `bars`, `ticks`, `domain`, `modeling`, and
  `provenance`. The default is `all`.
- The `provenance` group is optional and offline. It checks local quality
  targets against `.histdatacom/manifest-status.sqlite3` when present; explicit
  provenance runs without a store record a clean info result instead of failing
  file-only workflows.
- `histdatacom --repo-quality` runs the same offline quality workflow and then
  writes bounded per-instrument summary metadata back to the local `.repo`
  helper file. It stores status, counts, checked groups, format/timeframe/period
  coverage, and artifact references only; detailed findings remain in the JSON
  quality report. Ordinary `-A` and `-U` repository commands do not run quality
  checks. Use `histdatacom -A --repo-quality-columns` to display previously
  stored quality summaries in repository table output.
- Full-dataset campaign batches should run as bounded symbol/format/timeframe
  slices. For each slice, run download/extract, then `--repo-quality`; normal
  execution keeps cache artifacts. The `.repo` file and detailed quality reports
  are the durable audit artifacts. When disk pressure requires cleanup, run it
  only after `--repo-quality` succeeds and never remove `.repo` or reports. The
  issue-240 cache cleanup removes canonical `.data` cache files with
  `find <slice-target> -name .data -type f -delete`; more aggressive low-disk
  runs may remove the entire slice working directory after `.repo` and reports
  have been written. Source checkouts whose doctor output reports
  `platform.executable_bundled=false` need an explicit Temporal executable, a
  verified cache entry, or network access for first-run provisioning.
- Use `--quality-report PATH` to write the full JSON report. The report schema
  is `histdatacom.quality-report.v1`; console output remains a bounded human
  summary with clean, warning, and failed file sections.
- Use `--quality-profile PATH` to embed a validated
  `histdatacom.quality-profile.v1` JSON profile into the `DataQualityWorkflow`
  request. Profiles tune rule thresholds, severities, precision/tick-size
  overrides, gap/session tolerances, tick microstructure profiles,
  cross-instrument tolerances, and modeling-readiness assumptions. The workflow
  keeps only bounded profile metadata in history while the full report records
  the active profile source, name, configured rule IDs, and assumption keys.
- Warnings are advisory by default. Errors make a target failed and make the
  process exit nonzero under the default `--quality-fail-on error` policy.
  CI jobs that want warnings to fail should pass
  `--quality-fail-on warning --quality-max-warnings 0`; report-only jobs can
  pass `--quality-fail-on never`.
- The checks encode HistData-specific assumptions: ASCII M1 files are bid OHLC
  bars, ASCII tick files include bid and ask, and source timestamps are fixed
  EST with no daylight-saving adjustment before UTC normalization.
- Format coverage is explicit per target through `quality_support` metadata.
  ASCII `M1` and `T` artifacts receive parser-level checks. MetaTrader `M1`,
  NinjaTrader `M1`/`T_LAST`/`T_BID`/`T_ASK`, MetaStock `M1`, and Excel `M1`
  artifacts are inventory-only today: ZIP integrity and expected filename/member
  checks are supported, but parser-level content checks emit
  `HISTDATA_FORMAT_INVENTORY_ONLY` warnings instead of reporting the target as
  deeply clean. Recognized formats used with unsupported timeframes emit
  `HISTDATA_FORMAT_UNSUPPORTED`.

Example clean focused ingestion run:

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

Clean files
- csv: /path/to/data/DAT_ASCII_EURUSD_M1_201202.csv (findings=1, warnings=0, errors=0)

Warning files
- none

Failed files
- none
```

Example strict profile run:

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

```sh
histdatacom --quality \
  --quality-target data/ \
  --quality-profile profiles/strict-ci.json \
  --quality-fail-on warning \
  --quality-report reports/quality-strict.json
```

Example failing ingestion run:

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

Clean files
- none

Warning files
- none

Failed files
- csv: /path/to/data/bad/DAT_ASCII_EURUSD_M1_201202_BAD.csv (findings=2, warnings=0, errors=1)
```

Representative JSON report fields:

```json
{
  "schema_version": "histdatacom.quality-report.v1",
  "summary": {
    "status": "failed",
    "target_count": 1,
    "warning_count": 0,
    "error_count": 1
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
Temporal executable, and live import coverage requires a real Influx target or
the Docker-backed `scripts/smoke_influx_docker.py` helper.
Package release smoke should validate metadata, console entry points, runtime
resources, and offline `status`/`doctor` behavior for every wheel. Normal
metadata-only release smoke should exercise the verified resolver with an
isolated cache. For bundled offline/private wheels, release smoke should also
require `doctor.platform.executable_bundled == true`, run the packaged
executable version probe, start the runtime without `--executable`, and run
`--hermetic-sidecar-smoke` plus `--default-routing-sidecar-smoke`. Use
`--live-sidecar-smoke` separately when the
operator intentionally wants to include external HistData.com availability and
URL-validation coverage.

## GUI Integration Notes

The future GUI should treat the sidecar as a local workspace service:

- choose and persist a workspace path before starting the sidecar
- use JSON lifecycle commands for status and diagnostics
- submit requests through the same `RunRequest` contract as the CLI/API
- poll `jobs progress`, `jobs logs`, and `jobs artifacts`
- display `runtime-policy.json` paths when asking users for troubleshooting
- avoid direct reads of Temporal SQLite unless a dedicated diagnostic feature is
  being implemented
