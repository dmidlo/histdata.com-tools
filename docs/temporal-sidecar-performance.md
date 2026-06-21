# Temporal Sidecar Performance Baseline

Issues: #166, #180

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

## Live Throughput Matrix

Issue #180 adds an operator-gated benchmark script that compares the
queue-free foreground runtime and a live Temporal sidecar on the same request
matrix:

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

The default matrix uses one EURUSD M1 period and covers:

- repository refresh
- dataset planning and URL validation
- archive download and CSV extraction
- Polars cache build and cache merge
- no-Influx import-skipped behavior, represented by a cache/merge request that
  intentionally omits `ImportWorkflow` and starts only orchestration, network,
  and CPU/file worker lanes

The benchmark uses `max_work_items_per_batch=1` to force visible child-workflow
handoff in the one-period matrix. Production defaults remain
`max_work_items_per_batch=64`.

## Live Result

Live run date: 2026-06-21. Temporal executable:
`/opt/local/bin/temporal`. InfluxDB was intentionally unavailable and skipped.

| Scenario | Foreground elapsed | Sidecar elapsed | Ratio | Sidecar process CPU | Artifacts | Failures/retries |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| repository-refresh | 0.053s | 2.357s | 44.487x | 2.690s | 1 | 0/0 |
| validate-url | 0.419s | 0.658s | 1.569x | 0.310s | 0 | 0/0 |
| download-extract | 1.756s | 2.288s | 1.303x | 0.620s | 2 | 0/0 |
| cache-merge-no-influx | 1.904s | 2.255s | 1.185x | 0.710s | 3 | 0/0 |

Sidecar startup was 0.142s in this run. The sidecar path successfully forwarded
planned work items through live Temporal child workflows and produced expected
ZIP, CSV, and cache artifacts. The benchmark also caught and fixed a live SDK
payload-boundary issue: activity entrypoints now use `dict[str, Any]` rather
than the recursive `JSONValue` alias so Temporal's data converter preserves
nested request payloads.

## Accepted Envelope

No lane default changes are warranted from the issue #180 measurements:

- Keep orchestration workers at `1`.
- Keep network workers at `get_pool_cpu_count(cpu_utilization) * 3`.
- Keep CPU/file workers at `get_pool_cpu_count(cpu_utilization)`.
- Keep Influx workers at `1` until a live Influx target is available.
- Keep the production batch default at `64` work items per child workflow.

The sidecar has fixed orchestration overhead, so repository-only and
single-item jobs can be slower than foreground. That tradeoff is acceptable for
the Temporal migration because the live path now proves bounded workflow
history, real activity handoff, artifact references instead of dataframes in
history, and lane-isolated execution. Throughput tuning should happen on
multi-period and multi-pair workloads before changing defaults.
