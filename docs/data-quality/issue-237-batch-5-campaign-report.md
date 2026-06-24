# Issue 237 Batch 5 Campaign Report

Parent: #93. Depends on: #232. Related: #223, #224, #225, #229, #240.

## Result

Status: `completed`.

The batch completed the ASCII/M1 ZIP and extracted CSV quality surface for all
ten requested instruments. Each instrument now has bounded quality metadata in
the tracked local `data/.repo` file and a detailed ignored JSON report under
`data/.quality/issue-237`.

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

- Starting available space: about `23` GiB
- Ending available space: about `19` GiB
- Starting data directory size: about `17` GiB
- Ending data directory size: about `20` GiB
- Issue report directory size: about `42` MiB

Per-symbol ASCII/M1 working-artifact sizes after completion:

| Instrument | Size |
| --- | ---: |
| `sgdjpy` | `360M` |
| `spxusd` | `287M` |
| `udxusd` | `305M` |
| `ukxgbp` | `257M` |
| `usdcad` | `410M` |
| `usdchf` | `458M` |
| `usdczk` | `308M` |
| `usddkk` | `335M` |
| `usdhkd` | `203M` |
| `usdhuf` | `315M` |

## Repo Quality Summary

| Instrument | Repo start | Repo end | Quality | Targets | Findings | Warnings | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `sgdjpy` | `200808` | `202606` | `warning` | 48 | 530 | 239 | 0 |
| `spxusd` | `201011` | `202606` | `warning` | 44 | 431 | 164 | 0 |
| `udxusd` | `201011` | `202606` | `warning` | 44 | 421 | 168 | 0 |
| `ukxgbp` | `201011` | `202606` | `warning` | 44 | 420 | 166 | 0 |
| `usdcad` | `200006` | `202606` | `warning` | 64 | 660 | 275 | 0 |
| `usdchf` | `200005` | `202606` | `warning` | 64 | 673 | 288 | 0 |
| `usdczk` | `201011` | `202606` | `warning` | 44 | 462 | 195 | 0 |
| `usddkk` | `200808` | `202606` | `warning` | 48 | 496 | 205 | 0 |
| `usdhkd` | `200808` | `202606` | `warning` | 48 | 455 | 164 | 0 |
| `usdhuf` | `201011` | `202606` | `warning` | 44 | 457 | 203 | 0 |

The repo table command used for verification was:

```sh
venv/bin/histdatacom -A \
  -p sgdjpy spxusd udxusd ukxgbp usdcad usdchf usdczk usddkk usdhkd usdhuf \
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
  -p sgdjpy spxusd udxusd ukxgbp usdcad usdchf usdczk usddkk usdhkd usdhuf \
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
  --quality-report data/.quality/issue-237/issue-237-ascii-m1-<symbol>-quality.json \
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
