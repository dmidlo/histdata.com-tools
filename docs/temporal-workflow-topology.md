# Temporal Workflow Topology

Issue #150 establishes the Temporal workflow hierarchy without extracting the
legacy queue-bound activities yet. The first runnable boundary is the parent
`HistDataRunWorkflow`, which represents one user-visible CLI/API/GUI job.

## Hierarchy

`HistDataRunWorkflow` runs coarse child workflows:

- `RepositoryRefreshWorkflow` for repository metadata refresh requests.
- `DatasetPlanWorkflow` for bounded dataset planning metadata.
- `SymbolTimeframeWorkflow` once per requested pair/timeframe partition.

`SymbolTimeframeWorkflow` runs operation-family child workflows:

- `ValidateUrlsWorkflow`
- `DownloadArchivesWorkflow`
- `ExtractCsvWorkflow`
- `BuildCacheWorkflow`
- `MergeCacheWorkflow`
- `ImportWorkflow`

These boundaries are intentionally larger than individual files, rows, or
records. The workflows pass request metadata, pair/timeframe partition IDs,
status events, and artifact references. Downloaded archives, CSVs, cache files,
dataframes, and rows stay on disk and must be referenced through
`ArtifactRef`/`StageResult` payloads.

## Status Queries

Parent and child workflow classes expose a `status` query. The query returns a
JSON-safe progress document with request ID, workflow name, current stage,
planned children, completed children, status events, and artifact references.
This is the contract that later CLI and GUI surfaces can poll without importing
activity implementation modules.

## Current Activity Boundary

Issue #151 owns extraction of queue-free activity implementations. Until then,
leaf workflows use a pending activity executor by default and tests inject fake
activity executors. This keeps the workflow composition testable without
reaching into `QueueManager`, `config.CURRENT_QUEUE`, `Records`, or the legacy
progress rendering code.
