# Temporal Workflow Topology

The Temporal topology is intentionally coarse-grained. Workflows carry request
metadata, partition IDs, status events, and artifact references. Downloaded
archives, extracted files, cache data, dataframes, and rows stay on disk.

The parent runnable boundary is `HistDataRunWorkflow`, which represents one
user-visible CLI/API/GUI job.

## Hierarchy

`HistDataRunWorkflow` runs top-level child workflows:

- `RepositoryRefreshWorkflow` for repository metadata refresh requests.
- `DatasetPlanWorkflow` for bounded dataset planning metadata.
- `SymbolTimeframeWorkflow` for bounded dataset-period batches after planning.

The initial request shape starts from pair/timeframe intent.
`DatasetPlanWorkflow` persists the dataset plan to the manifest store and
returns `dataset_plan_ref` plus deterministic `dataset_plan_batches`. Plans at
or below the inline threshold can still include `work_items` for simple paths.
Plans above the threshold omit the full `work_items` list from workflow history.
The default inline threshold is `64` work items and can be overridden with
`temporal_plan_spill.inline_work_item_limit`.

After planning, the parent expands each pair/timeframe group into deterministic
batches by pair, timeframe, data format, and ordered year-month periods. Batch
partitions carry only bounded metadata such as `batch_key`, `batch_index`,
`batch_count`, `work_item_count`, and a bounded `work_ids` list. Independent
`SymbolTimeframeWorkflow` batches are then started with deterministic bounded
fan-out. Parent summaries still record child results in planned order.

`SymbolTimeframeWorkflow` runs operation-family child workflows:

- `ValidateUrlsWorkflow`
- `DownloadArchivesWorkflow`
- `ExtractCsvWorkflow`
- `BuildCacheWorkflow`
- `MergeCacheWorkflow`
- `ImportWorkflow`

These boundaries are intentionally larger than individual rows or records while
remaining bounded by work-item batch size. The workflows pass request metadata,
batch partition IDs, status events, and artifact references. Downloaded
archives, CSVs, cache files, dataframes, and rows stay on disk and must be
referenced through
`ArtifactRef`/`StageResult` payloads.

Operation-family workflows inside one `SymbolTimeframeWorkflow` remain
sequential because validation, download, extraction, cache build, merge, and
import depend on forwarded work-item state from the previous stage. If a large
plan was spilled, the first operation activity hydrates only the batch
`work_ids` from `dataset_plan_ref`; later operation stages use the normally
forwarded bounded batch state.

## Activities

Leaf workflows call Temporal activities through `TemporalActivityExecutor`.
The default activities are:

- `repository_refresh`
- `dataset_plan`
- `validate_urls`
- `download_archives`
- `extract_csv`
- `build_cache`
- `merge_cache`
- `import_to_influx`

The activity functions delegate to queue-free stage helpers and return
`StageResult`, `ArtifactRef`, progress metadata, cancellation metadata, and
bounded operation summaries. `dataset_plan` stores full plan metadata in the
manifest SQLite store and returns a compact reference. Operation activities can
load their assigned batch from that reference when no inline `work_items`
payload is present. Activities do not return rows, raw archive bytes, or
materialized dataframes through Temporal history.

## Task Queues

Task queues are workspace-scoped and derived from the runtime policy:

`histdatacom.<workspace-id>.<lane>`

Known lanes:

- `orchestration`
- `network`
- `cpu-file`
- `influx`

`python -m histdatacom.orchestration.worker config --json` exposes the resolved namespace,
target host, task queues, lane, and worker concurrency. See
`docs/temporal-orchestration-performance.md` for the current lane sizing policy.

## Status Queries

Parent and child workflow classes expose a `status` query. The query returns a
JSON-safe progress document with request ID, workflow name, current stage,
planned children, completed children, status events, and artifact references.
This is the contract that later CLI and GUI surfaces can poll without importing
activity implementation modules.

## Control Surface

`histdatacom jobs inspect`, `progress`, `logs`, `artifacts`, `result`,
`cancel`, `retry`, and `resume` use the same bounded status and artifact
contracts. Workflow IDs use the format `histdatacom-<request-id>`.

Retry and resume create deterministic replacement parent workflows rather than
mutating completed workflow histories. Replacement requests carry
`control_execution` metadata with the parent workflow ID, previous run ID,
attempt number, stage-specific resume policy, cleanup results, and artifact reuse
preference. Existing complete artifacts remain on disk and are referenced through
the normal `ArtifactRef`/`StageResult` path; hidden temp artifacts are removed or
ignored according to the stage resume policy before the replacement workflow is
submitted.

## Testing Strategy

Workflow tests should exercise composition, metadata shape, status queries,
task queue routing, fake child execution, and fake activity execution without a
live Temporal server. Activity tests should use fixture HTML, CSV, cache, and
fake Influx sinks where possible. Live Temporal and live Influx checks belong
in explicit smoke tests because they require operator-provided services or
executables.
