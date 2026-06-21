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

The initial request shape starts from pair/timeframe intent. Once
`DatasetPlanWorkflow` returns planned `WorkItem` metadata, the parent expands
each pair/timeframe group into deterministic batches by pair, timeframe, data
format, and ordered year-month periods. Batch partitions carry only bounded
metadata such as `batch_key`, `batch_index`, `batch_count`, `work_item_count`,
and a bounded `work_ids` list.

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
bounded operation summaries. They do not return rows, raw archive bytes, or
materialized dataframes through Temporal history.

## Task Queues

Task queues are workspace-scoped and derived from the runtime policy:

`histdatacom.<workspace-id>.<lane>`

Known lanes:

- `orchestration`
- `network`
- `cpu-file`
- `influx`

`histdatacom-sidecar-worker config --json` exposes the resolved namespace,
target host, task queues, lane, and worker concurrency. See
`docs/temporal-sidecar-performance.md` for the current lane sizing policy.

## Status Queries

Parent and child workflow classes expose a `status` query. The query returns a
JSON-safe progress document with request ID, workflow name, current stage,
planned children, completed children, status events, and artifact references.
This is the contract that later CLI and GUI surfaces can poll without importing
activity implementation modules.

## Control Surface

`histdatacom-sidecar jobs inspect`, `progress`, `logs`, `artifacts`, `result`,
`cancel`, `retry`, and `resume` use the same bounded status and artifact
contracts. Workflow IDs use the format `histdatacom-<request-id>`.

## Testing Strategy

Workflow tests should exercise composition, metadata shape, status queries,
task queue routing, fake child execution, and fake activity execution without a
live Temporal server. Activity tests should use fixture HTML, CSV, cache, and
fake Influx sinks where possible. Live Temporal and live Influx checks belong
in explicit smoke tests because they require operator-provided services or
executables.
