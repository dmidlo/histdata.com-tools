# Issue 234 Batch 2 Campaign Report

Parent: #93. Depends on: #232. Related: #223, #225, #240.

## Result

Status: `completed`.

The batch completed the ASCII/M1 ZIP and extracted CSV quality surface for all
ten requested instruments. Each instrument now has bounded quality metadata in
the tracked local `data/.repo` file and a detailed ignored JSON report under
`data/.quality/issue-234`.

All ten repo-quality summaries finished with status `warning` and zero hard
errors. The warnings are retained as market-data quality findings rather than
treated as ingestion failures.

## Runtime

- Data root: `data`
- Sidecar mode: source checkout with explicit Temporal executable
- Temporal executable: `/opt/local/bin/temporal`
- Source checkout doctor note: `platform.executable_bundled=false` is expected
  outside an installed platform wheel.
- Completed surface: `ascii` / `1-minute-bar-quotes`

## Disk Preflight

- Starting available space: about `34` GiB
- Ending available space: about `30` GiB
- Starting data directory size: about `7.0` GiB
- Ending data directory size: about `10` GiB
- Issue report directory size: about `43` MiB

Per-symbol ASCII/M1 working-artifact sizes after completion:

| Instrument | Size |
| --- | ---: |
| `etxeur` | `81M` |
| `euraud` | `445M` |
| `eurcad` | `364M` |
| `eurchf` | `417M` |
| `eurczk` | `220M` |
| `eurdkk` | `162M` |
| `eurgbp` | `421M` |
| `eurhuf` | `224M` |
| `eurjpy` | `516M` |
| `eurnok` | `306M` |

## Repo Quality Summary

| Instrument | Repo start | Repo end | Quality | Targets | Findings | Warnings | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `etxeur` | `201011` | `201902` | `warning` | 20 | 184 | 74 | 0 |
| `euraud` | `200208` | `202606` | `warning` | 60 | 625 | 262 | 0 |
| `eurcad` | `200703` | `202606` | `warning` | 50 | 524 | 221 | 0 |
| `eurchf` | `200201` | `202606` | `warning` | 60 | 612 | 249 | 0 |
| `eurczk` | `201011` | `202606` | `warning` | 44 | 443 | 176 | 0 |
| `eurdkk` | `200808` | `202606` | `warning` | 48 | 459 | 168 | 0 |
| `eurgbp` | `200203` | `202606` | `warning` | 60 | 617 | 254 | 0 |
| `eurhuf` | `201011` | `202606` | `warning` | 44 | 430 | 179 | 0 |
| `eurjpy` | `200203` | `202606` | `warning` | 60 | 664 | 301 | 0 |
| `eurnok` | `200808` | `202606` | `warning` | 48 | 509 | 218 | 0 |

The repo table command used for verification was:

```sh
venv/bin/histdatacom -A \
  -p etxeur euraud eurcad eurchf eurczk eurdkk eurgbp eurhuf eurjpy eurnok \
  --repo-quality-columns \
  --data-directory data
```

## Command Evidence

The sidecar was started explicitly because this source checkout does not bundle
the platform executable:

```sh
venv/bin/histdatacom-sidecar start --executable /opt/local/bin/temporal
```

Availability metadata was refreshed before running quality:

```sh
venv/bin/histdatacom -U \
  -p etxeur euraud eurcad eurchf eurczk eurdkk eurgbp eurhuf eurjpy eurnok \
  --repo-quality-columns \
  --data-directory data
```

Each instrument used this download/extract shape:

```sh
venv/bin/histdatacom -D -X \
  -p <symbol> \
  -f ascii \
  -t 1-minute-bar-quotes \
  --data-directory data
```

Each instrument then refreshed repo quality with this shape:

```sh
venv/bin/histdatacom --repo-quality \
  --quality-target data/ASCII/M1/<symbol> \
  --quality-checks all \
  --quality-report data/.quality/issue-234/issue-234-ascii-m1-<symbol>-quality.json \
  --data-directory data \
  --quality-fail-on never
```

## Implementation Findings

The first slice exposed two implementation issues that were fixed before
completing the batch:

- The storage-backed campaign planner emitted internal timeframe keys such as
  `M1`. The public CLI accepts enum values such as `1-minute-bar-quotes`, so the
  planner now renders CLI-compatible timeframe arguments.
- The live repo-quality activity exceeded the previous `30` second heartbeat
  timeout on `etxeur`. The data-quality activity heartbeat is now `300` seconds,
  while the existing start-to-close timeout remains unchanged.

## Boundaries

This issue completed the ASCII/M1 ZIP and extracted CSV campaign surface. The
current `histdatacom -D -X` CLI path did not create canonical `.data` cache
files, so the explicit disk-pressure cache cleanup command was a no-op for this
batch:

```sh
find data/ASCII/M1/<symbol> -name .data -type f -delete
```

Deep cache validation itself is covered by #223, and non-ASCII quality support
boundaries are covered by #225. No new data-quality rule issue was discovered
while closing this batch.
