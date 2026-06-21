# Temporal Sidecar Performance Baseline

Issue: #166

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

The foreground runtime now uses the same explicit work-item stage functions as
the sidecar, and the sidecar keeps those lanes separate as orchestration,
network, CPU/file, and Influx task queues. Workflow payloads stay bounded to
metadata, artifacts, and status events; downloaded archives, CSV files, and
cache data remain on disk.

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

`DatasetPlanWorkflow` still produces bounded `WorkItem` metadata for the full
request. Before operation workflows run, each coarse pair/timeframe partition
is expanded into deterministic child workflow batches grouped by:

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

Each batch partition includes `format`, `start_yearmonth`, `end_yearmonth`,
`periods`, `batch_index`, `batch_count`, `batch_key`, `work_item_count`, and a
bounded `work_ids` list. The child workflow ID is derived from those fields, so
retry and resume behavior is deterministic for the same dataset plan.

This keeps throughput high by preserving lane-level concurrency while avoiding
one large symbol/timeframe workflow carrying every monthly work item for a
multi-year request. Cancellation and retry scope also become smaller: a failed
batch can be reasoned about as a specific pair/timeframe/format/period slice.

## Tuning Guidance

Keep network activity concurrency higher than CPU/file work because validation
and archive downloads spend most time waiting on remote I/O. Keep Influx
concurrency conservative until idempotent external write behavior is validated
under a live Influx target. Increase CPU/file workers only when cache builds are
CPU-bound and memory headroom remains stable.

Future sidecar benchmarks should compare realistic validate/download/cache/import
runs against this retired-runtime policy so regressions are visible before
worker defaults are changed.
