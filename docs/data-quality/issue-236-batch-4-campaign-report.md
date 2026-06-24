# Issue 236 Batch 4 Campaign Report

Parent: #93. Depends on: #232. Related: #223, #224, #225, #229, #240.

## Result

Status: `completed`.

The batch completed the ASCII/M1 ZIP and extracted CSV quality surface for all
ten requested instruments. Each instrument now has bounded quality metadata in
the tracked local `data/.repo` file and a detailed ignored JSON report under
`data/.quality/issue-236`.

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

- Starting available space: about `28` GiB
- Ending available space: about `23` GiB
- Starting data directory size: about `14` GiB
- Ending data directory size: about `17` GiB
- Issue report directory size: about `43` MiB

Per-symbol ASCII/M1 working-artifact sizes after completion:

| Instrument | Size |
| --- | ---: |
| `gbpnzd` | `344M` |
| `gbpusd` | `452M` |
| `grxeur` | `240M` |
| `hkxhkd` | `182M` |
| `jpxjpy` | `283M` |
| `nsxusd` | `335M` |
| `nzdcad` | `341M` |
| `nzdchf` | `343M` |
| `nzdjpy` | `398M` |
| `nzdusd` | `368M` |

## Repo Quality Summary

| Instrument | Repo start | Repo end | Quality | Targets | Findings | Warnings | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `gbpnzd` | `200803` | `202606` | `warning` | 48 | 504 | 213 | 0 |
| `gbpusd` | `200005` | `202606` | `warning` | 64 | 679 | 293 | 0 |
| `grxeur` | `201011` | `202606` | `warning` | 44 | 410 | 160 | 0 |
| `hkxhkd` | `201011` | `202606` | `warning` | 44 | 389 | 145 | 0 |
| `jpxjpy` | `201011` | `202606` | `warning` | 44 | 434 | 168 | 0 |
| `nsxusd` | `201011` | `202606` | `warning` | 44 | 424 | 158 | 0 |
| `nzdcad` | `200803` | `202606` | `warning` | 48 | 509 | 218 | 0 |
| `nzdchf` | `200803` | `202606` | `warning` | 48 | 523 | 232 | 0 |
| `nzdjpy` | `200609` | `202606` | `warning` | 52 | 585 | 270 | 0 |
| `nzdusd` | `200508` | `202606` | `warning` | 54 | 579 | 252 | 0 |

The repo table command used for verification was:

```sh
venv/bin/histdatacom -A \
  -p gbpnzd gbpusd grxeur hkxhkd jpxjpy nsxusd nzdcad nzdchf nzdjpy nzdusd \
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
  -p gbpnzd gbpusd grxeur hkxhkd jpxjpy nsxusd nzdcad nzdchf nzdjpy nzdusd \
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
  --quality-report data/.quality/issue-236/issue-236-ascii-m1-<symbol>-quality.json \
  --data-directory data \
  --quality-fail-on never
```

## Boundaries

This issue completed the ASCII/M1 ZIP and extracted CSV campaign surface. The
current `histdatacom -D -X` CLI path did not leave canonical `.data` cache
files in this batch, so the explicit disk-pressure cache cleanup command
removed nothing:

```sh
find data/ASCII/M1/<symbol> -name .data -type f -delete
```

Deep cache validation itself is covered by #223, and non-ASCII quality support
boundaries are covered by #225. No new parser/support issue or hard data-defect
issue was discovered while closing this batch.
