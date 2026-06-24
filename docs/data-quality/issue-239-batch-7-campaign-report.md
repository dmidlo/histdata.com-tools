# Issue 239 Batch 7 Campaign Report

Parent: #93. Depends on: #232. Related: #223, #224, #225, #229, #240.

## Result

Status: `completed`.

The batch completed the ASCII/M1 ZIP and extracted CSV quality surface for all
six requested instruments. Each instrument now has bounded quality metadata in
the tracked local `data/.repo` file and a detailed ignored JSON report under
`data/.quality/issue-239`.

All six repo-quality summaries finished with status `warning` and zero hard
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

- Starting available space: about `16` GiB
- Ending available space: about `14` GiB
- Starting data directory size: about `23` GiB
- Ending data directory size: about `25` GiB
- Issue report directory size: about `15` MiB

Per-symbol ASCII/M1 working-artifact sizes after completion:

| Instrument | Size |
| --- | ---: |
| `xauaud` | `216M` |
| `xauchf` | `216M` |
| `xaueur` | `212M` |
| `xaugbp` | `206M` |
| `xauusd` | `380M` |
| `zarjpy` | `265M` |

## Repo Quality Summary

| Instrument | Repo start | Repo end | Quality | Targets | Findings | Warnings | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `xauaud` | `200905` | `201812` | `warning` | 20 | 183 | 65 | 0 |
| `xauchf` | `200905` | `201812` | `warning` | 20 | 187 | 70 | 0 |
| `xaueur` | `200905` | `201812` | `warning` | 20 | 183 | 66 | 0 |
| `xaugbp` | `200905` | `201812` | `warning` | 20 | 186 | 69 | 0 |
| `xauusd` | `200903` | `202606` | `warning` | 46 | 444 | 167 | 0 |
| `zarjpy` | `201011` | `202606` | `warning` | 44 | 506 | 239 | 0 |

The repo table command used for verification was:

```sh
venv/bin/histdatacom -A \
  -p xauaud xauchf xaueur xaugbp xauusd zarjpy \
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
  -p xauaud xauchf xaueur xaugbp xauusd zarjpy \
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
  --quality-report data/.quality/issue-239/issue-239-ascii-m1-<symbol>-quality.json \
  --data-directory data \
  --quality-fail-on never
```

## Boundaries

This issue completed the ASCII/M1 ZIP and extracted CSV campaign surface for
the final full-dataset campaign batch. The current `histdatacom -D -X` CLI path
did not leave canonical `.data` cache files in this batch, so the explicit
disk-pressure cache cleanup command removed nothing:

```sh
find data/ASCII/M1/<symbol> -name .data -type f -delete
```

Deep cache validation itself is covered by #223, and non-ASCII quality support
boundaries are covered by #225. No new parser/support issue or hard data-defect
issue was discovered while closing this batch.
