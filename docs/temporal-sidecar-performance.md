# Temporal Sidecar Performance Baseline

Issue: #166

## Current Runtime Baseline

The legacy runtime has three distinct concurrency behaviors:

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

`histdatacom-sidecar-worker` exposes:

- `--cpu-utilization`
- `--network-multiplier`
- `--orchestration-workers`
- `--influx-workers`
- `--max-concurrent-activities`

The explicit `--max-concurrent-activities` flag overrides only the selected
lane, so operators can tune a hot lane without changing the package.

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

## Tuning Guidance

Keep network activity concurrency higher than CPU/file work because validation
and archive downloads spend most time waiting on remote I/O. Keep Influx
concurrency conservative until idempotent external write behavior is validated
under a live Influx target. Increase CPU/file workers only when cache builds are
CPU-bound and memory headroom remains stable.

Do not remove the legacy concurrency implementation until sidecar benchmarks
are captured across realistic validate/download/cache/import runs and compared
against this policy.
