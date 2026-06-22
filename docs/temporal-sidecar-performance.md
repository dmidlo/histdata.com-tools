# Temporal Sidecar Performance Baseline

Issues: #166, #180, #181, #187

See `docs/temporal-sidecar-operations.md` for lifecycle commands, runtime
paths, troubleshooting, and worker startup guidance. This page is limited to
the performance baseline and lane sizing policy.

## Retired Runtime Baseline

The retired manager-backed runtime had three distinct concurrency behaviors:

- URL validation and archive download use thread pools sized as
  `get_pool_cpu_count(cpu_utilization) * 3`.
- CSV extraction and cache building use process pools sized as
  `get_pool_cpu_count(cpu_utilization)`.
- Influx import uses bounded line-protocol batches and writes sequentially
  through one `InfluxBatchWriter`.

The sidecar keeps those lanes separate as orchestration, network, CPU/file, and
Influx task queues. Workflow payloads stay bounded to metadata, artifacts, and
status events; downloaded archives, CSV files, and cache data remain on disk.

## Sidecar Worker Policy

`histdatacom.sidecar.performance.build_sidecar_concurrency_profile()` derives a
lane policy from the existing `cpu_utilization` setting:

- orchestration: `1`
- network: `get_pool_cpu_count(cpu_utilization) * 3`
- cpu-file: `get_pool_cpu_count(cpu_utilization)`
- influx: `1`

`histdatacom-sidecar start` and `histdatacom-sidecar restart` pass the same
policy to every supervised worker lane. `histdatacom-sidecar-worker` exposes
the same flags for manual lane debugging:

- `--cpu-utilization`
- `--network-multiplier`
- `--orchestration-workers`
- `--influx-workers`
- `--max-concurrent-activities`

The explicit `--max-concurrent-activities` flag overrides only the selected
lane, so operators can tune a hot lane without changing the package.

Inspect resolved worker settings with:

```sh
histdatacom-sidecar-worker config --lane network --json
```

## Benchmark Coverage

`histdatacom.sidecar.performance.benchmark_operation()` captures compact local
metrics:

- elapsed wall-clock seconds
- process CPU seconds
- peak RSS bytes
- throughput per second
- retry count
- startup seconds

Unit coverage includes local fake/fixture baselines for representative
operations:

- `validate_url_work_item` with fake HistData form HTML
- `build_cache_work_item` with checked-in M1 CSV fixture data
- `import_to_influx_work_item` with a local line sink instead of live Influx
- `ImportWorkflow` with the real Influx activity and a local
  contract-backed writer

This keeps the baseline runnable without HistData.com, Temporal server, or
InfluxDB availability.

`histdatacom.sidecar.performance.benchmark_partition_batching()` compares the
old coarse pair/timeframe partition shape with the period-batch fanout used by
the sidecar. Its metadata reports:

- coarse partition count
- period batch count
- total work item count
- configured maximum work items per batch
- maximum work items carried by any coarse child
- maximum work items carried by any period-batch child
- the resulting maximum child payload reduction

This benchmark is deterministic and fixture-friendly: it only plans workflow
metadata and does not require a live Temporal server.

## Dataset-Period Batching

`DatasetPlanWorkflow` persists the full dataset plan to the manifest store. It
keeps inline `WorkItem` metadata only for small plans, then spills larger plans
out of workflow history and returns `dataset_plan_ref` plus
`dataset_plan_batches`. Before operation workflows run, each coarse
pair/timeframe partition is expanded into deterministic child workflow batches
grouped by:

- pair
- timeframe
- data format
- ordered year-month periods

The default maximum batch size is `64` work items. Requests can override it in
metadata:

```json
{
  "temporal_batching": {
    "max_work_items_per_batch": 32
  }
}
```

The default dataset-plan inline threshold is also `64` work items. Requests can
override it in metadata:

```json
{
  "temporal_plan_spill": {
    "inline_work_item_limit": 32
  }
}
```

`HistDataRunWorkflow` starts independent `SymbolTimeframeWorkflow` period
batches with bounded fan-out after `DatasetPlanWorkflow` returns compact plan
metadata. The production default fan-out window is `4` child workflows. Requests
can override it in metadata:

```json
{
  "temporal_fanout": {
    "max_parallel_child_workflows": 2
  }
}
```

Each batch partition includes `format`, `start_yearmonth`, `end_yearmonth`,
`periods`, `batch_index`, `batch_count`, `batch_key`, `work_item_count`, and a
bounded `work_ids` list. The child workflow ID is derived from those fields, so
retry and resume behavior is deterministic for the same dataset plan.

This keeps throughput high by preserving lane-level concurrency while avoiding
one large symbol/timeframe workflow carrying every monthly work item for a
multi-year request. Cancellation and retry scope also become smaller: a failed
batch can be reasoned about as a specific pair/timeframe/format/period slice.
For spilled plans, the first operation activity loads only the batch `work_ids`
from the manifest reference, then downstream operation stages forward the
bounded batch state as usual.
Fan-out applies only to independent symbol/timeframe batch workflows. Within a
single batch, `ValidateUrlsWorkflow`, `DownloadArchivesWorkflow`,
`ExtractCsvWorkflow`, `BuildCacheWorkflow`, `MergeCacheWorkflow`, and
`ImportWorkflow` still run in dependency order so work-item forwarding remains
correct.

## Influx Sidecar Contract

Issue #187 accepts deterministic contract-backed Influx coverage when no real
InfluxDB service is available. The contract test executes `ImportWorkflow`
through the same activity-executor seam used by Temporal workers, calls the real
`import_to_influx_activity`, and replaces only the final `InfluxBatchWriter`
with a local writer. This proves:

- Influx work is routed to the `influx` task queue.
- `batch_size=2` emits bounded line-protocol batches of `2` and `1` rows for
  the checked-in EURUSD M1 fixture.
- the generated line protocol keeps the existing HistData ESTnoDST-to-UTC
  timestamp conversion and field names.
- retryable writer failures preserve `INFLUX_IMPORT_RETRYABLE`,
  `retryable=true`, and `idempotent_retry=true` metadata at workflow-summary
  level.
- `delete_after_influx=true` removes local ZIP/cache artifacts only after a
  successful import.

This does not prove live Influx authentication, bucket permissions, network
latency, server-side write rejection behavior, or production retention policy.
Those remain operator-gated live checks requiring `histdatacom[influx]`, a real
`influxdb.yaml`, a disposable target bucket, or the Docker-backed
`python scripts/smoke_influx_docker.py` helper.

## Tuning Guidance

Keep network activity concurrency higher than CPU/file work because validation
and archive downloads spend most time waiting on remote I/O. Keep Influx
concurrency at `1` by default: contract coverage proves batch formation,
idempotent retry metadata, and cleanup behavior, but real Influx service
throughput should be tuned against a live target before raising lane
concurrency. Use the existing CLI/API `batch_size` default of `5000` for normal
imports; lower it only for memory-constrained runs, diagnostics, or test
fixtures that need visible batch boundaries. Increase CPU/file workers only when
cache builds are CPU-bound and memory headroom remains stable.

## Live Throughput Matrix

Issue #180 added an operator-gated benchmark script for the live Temporal
sidecar request matrix:

```sh
HISTDATACOM_LIVE_SIDECAR_THROUGHPUT=1 \
HISTDATACOM_TEMPORAL_EXECUTABLE=/opt/local/bin/temporal \
python scripts/benchmark_sidecar_throughput.py \
  --workspace /tmp/histdatacom-issue180-benchmark/workspace \
  --runtime-home /tmp/histdatacom-issue180-benchmark/runtime \
  --data-directory /tmp/histdatacom-issue180-benchmark/data \
  --output /tmp/histdatacom-issue180-benchmark/report.json \
  --temporal-executable /opt/local/bin/temporal
```

The default matrix uses one EURUSD M1 period for the single-item scenarios and
a two-pair, three-month ASCII tick validation scenario for fan-out coverage. It
covers:

- repository refresh
- dataset planning and URL validation
- bounded multi-partition `SymbolTimeframeWorkflow` fan-out
- archive download and CSV extraction
- Polars cache build and cache merge
- no-Influx import-skipped behavior, represented by a cache/merge request that
  intentionally omits `ImportWorkflow` and starts only orchestration, network,
  and CPU/file worker lanes

The benchmark uses `max_work_items_per_batch=1` to force visible child-workflow
handoff and uses `max_parallel_child_workflows=2` to prove bounded fan-out
windows. Production defaults remain `max_work_items_per_batch=64` and
`max_parallel_child_workflows=4`.

## Multi-Partition Envelope

The accepted issue #181 envelope is:

- Expand coarse pair/timeframe requests into deterministic period batches after
  dataset planning.
- Start only independent `SymbolTimeframeWorkflow` batches concurrently.
- Keep the default production fan-out window at `4`.
- Preserve ordered parent summary payloads by recording child results in plan
  order, not completion order.
- On cancellation, wait for the already-started bounded window to finish and
  do not start later windows.
- Do not fan out dependent operation workflows inside a symbol/timeframe batch.

## Live Result

Live run date: 2026-06-21. Temporal executable:
`/opt/local/bin/temporal`. InfluxDB was intentionally unavailable for the live
matrix at that time. Influx behavior is covered by the issue #187
contract-backed workflow test described above, and Docker-backed live
writer/query validation is available through
`python scripts/smoke_influx_docker.py`.

| Scenario | Retired baseline elapsed | Sidecar elapsed | Ratio | Sidecar process CPU | Artifacts | Failures/retries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| repository-refresh | 0.054s | 0.944s | 17.482x | 2.670s | 1 | 0/0 |
| validate-url | 0.388s | 0.665s | 1.716x | 0.320s | 0 | 0/0 |
| multi-partition-validate-fanout | 2.230s | 2.522s | 1.131x | 1.090s | 0 | 0/0 |
| download-extract | 1.883s | 2.492s | 1.323x | 0.630s | 2 | 0/0 |
| cache-merge-no-influx | 1.791s | 2.338s | 1.306x | 0.700s | 3 | 0/0 |

Sidecar startup was 0.133s in this run. The fan-out scenario used EURUSD and
GBPUSD ASCII tick data for 202201 through 202203 with
`max_work_items_per_batch=1` and `max_parallel_child_workflows=2`. The sidecar
summary reported six planned work items, six `SymbolTimeframeWorkflow` child
results, seven total child stages including dataset planning, and no failures
or retries. The sidecar path successfully forwarded planned work items through
live Temporal child workflows and produced expected ZIP, CSV, and cache
artifacts for the non-fan-out artifact scenarios.

## Accepted Envelope

No lane default changes are warranted from the issue #180 measurements:

- Keep orchestration workers at `1`.
- Keep network workers at `get_pool_cpu_count(cpu_utilization) * 3`.
- Keep CPU/file workers at `get_pool_cpu_count(cpu_utilization)`.
- Keep Influx workers at `1`; the contract-backed test proves workflow handoff,
  batching, retry metadata, and cleanup, while live service throughput remains
  target-specific.
- Keep the CLI/API Influx `batch_size` default at `5000`; use smaller values
  only for constrained memory, debugging, or fixtures.
- Keep the production batch default at `64` work items per child workflow.
- Keep the production fan-out default at `4` parallel child workflows.

The sidecar has fixed orchestration overhead, so repository-only and
single-item jobs include startup and workflow bookkeeping costs. That tradeoff
is acceptable for the Temporal migration because the live path proves bounded
workflow history, real activity handoff, artifact references instead of
dataframes in history, and lane-isolated execution. Throughput tuning should
happen on multi-period and multi-pair workloads before changing defaults.
