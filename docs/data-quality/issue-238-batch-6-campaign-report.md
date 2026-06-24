# Issue 238 Batch 6 Campaign Report

Parent: #93. Depends on: #232. Related: #223, #224, #225, #229, #240.

## Result

Status: `completed`.

The batch completed the ASCII/M1 ZIP and extracted CSV quality surface for all
ten requested instruments. Each instrument now has bounded quality metadata in
the tracked local `data/.repo` file and a detailed ignored JSON report under
`data/.quality/issue-238`.

All ten repo-quality summaries finished with status `warning` and zero hard
errors. The warnings are retained as market-data quality findings rather than
treated as ingestion failures.

## Runtime

- Data root: `data`
- Runtime mode: source checkout with explicit Temporal executable
- Temporal executable: `/opt/local/bin/temporal`
- Source checkout doctor note: `platform.executable_bundled=false` is expected
  outside an installed platform wheel.
- Completed surface: `ascii` / `1-minute-bar-quotes`

## Disk Preflight

- Starting available space: about `19` GiB
- Ending available space: about `16` GiB
- Starting data directory size: about `20` GiB
- Ending data directory size: about `23` GiB
- Issue report directory size: about `41` MiB

Per-symbol ASCII/M1 working-artifact sizes after completion:

| Instrument | Size |
| --- | ---: |
| `usdjpy` | `517M` |
| `usdmxn` | `295M` |
| `usdnok` | `338M` |
| `usdpln` | `288M` |
| `usdsek` | `339M` |
| `usdsgd` | `297M` |
| `usdtry` | `247M` |
| `usdzar` | `286M` |
| `wtiusd` | `235M` |
| `xagusd` | `319M` |

## Repo Quality Summary

| Instrument | Repo start | Repo end | Quality | Targets | Findings | Warnings | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `usdjpy` | `200005` | `202606` | `warning` | 64 | 715 | 330 | 0 |
| `usdmxn` | `201011` | `202606` | `warning` | 44 | 472 | 205 | 0 |
| `usdnok` | `200808` | `202606` | `warning` | 48 | 520 | 229 | 0 |
| `usdpln` | `201011` | `202606` | `warning` | 44 | 468 | 201 | 0 |
| `usdsek` | `200808` | `202606` | `warning` | 48 | 521 | 230 | 0 |
| `usdsgd` | `200808` | `202606` | `warning` | 48 | 482 | 191 | 0 |
| `usdtry` | `201011` | `202606` | `warning` | 44 | 470 | 204 | 0 |
| `usdzar` | `201011` | `202606` | `warning` | 44 | 482 | 215 | 0 |
| `wtiusd` | `201011` | `202312` | `warning` | 28 | 279 | 113 | 0 |
| `xagusd` | `200905` | `202606` | `warning` | 46 | 464 | 189 | 0 |

The repo table command used for verification was:

```sh
venv/bin/histdatacom -A \
  -p usdjpy usdmxn usdnok usdpln usdsek usdsgd usdtry usdzar wtiusd xagusd \
  --repo-quality-columns \
  --data-directory data
```

## Command Evidence

The runtime was started explicitly because this source checkout does not bundle
the platform executable:

```sh
venv/bin/histdatacom runtime start --executable /opt/local/bin/temporal
```

Availability metadata was refreshed before running quality:

```sh
venv/bin/histdatacom -U \
  -p usdjpy usdmxn usdnok usdpln usdsek usdsgd usdtry usdzar wtiusd xagusd \
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
  --quality-report data/.quality/issue-238/issue-238-ascii-m1-<symbol>-quality.json \
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
