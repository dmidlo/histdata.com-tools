# Issue 235 Batch 3 Campaign Report

Parent: #93. Depends on: #232. Related: #223, #225, #240, #243.

## Result

Status: `completed-with-reviewed-data-defect`.

The batch completed the ASCII/M1 ZIP and extracted CSV quality surface for all
ten requested instruments. Each instrument now has bounded quality metadata in
the tracked local `data/.repo` file and a detailed ignored JSON report under
`data/.quality/issue-235`.

Nine instruments finished with status `warning` and zero hard errors. `EURUSD`
finished with status `failed` because the 2001 source file contains one
non-positive OHLC row. That source-data defect is tracked in #243 and recorded
in `docs/data-quality/known-data-defects.md`; the hard error is intentionally
not downgraded.

## Runtime

- Data root: `data`
- Runtime mode: source checkout with explicit Temporal executable
- Temporal executable: `/opt/local/bin/temporal`
- Source checkout doctor note: `platform.executable_bundled=false` is expected
  outside an installed platform wheel.
- Completed surface: `ascii` / `1-minute-bar-quotes`

## Disk Preflight

- Starting available space: about `30` GiB
- Ending available space: about `27` GiB
- Starting data directory size: about `10` GiB
- Ending data directory size: about `14` GiB
- Issue report directory size: about `45` MiB

Per-symbol ASCII/M1 working-artifact sizes after completion:

| Instrument | Size |
| --- | ---: |
| `eurnzd` | `345M` |
| `eurpln` | `243M` |
| `eursek` | `310M` |
| `eurtry` | `264M` |
| `eurusd` | `458M` |
| `frxeur` | `206M` |
| `gbpaud` | `354M` |
| `gbpcad` | `352M` |
| `gbpchf` | `444M` |
| `gbpjpy` | `516M` |

## Repo Quality Summary

| Instrument | Repo start | Repo end | Quality | Targets | Findings | Warnings | Errors |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| `eurnzd` | `200803` | `202606` | `warning` | 48 | 501 | 210 | 0 |
| `eurpln` | `201011` | `202606` | `warning` | 44 | 448 | 181 | 0 |
| `eursek` | `200808` | `202606` | `warning` | 48 | 508 | 217 | 0 |
| `eurtry` | `201011` | `202606` | `warning` | 44 | 467 | 206 | 0 |
| `eurusd` | `200005` | `202606` | `failed` | 64 | 667 | 281 | 1 |
| `frxeur` | `201011` | `202606` | `warning` | 44 | 398 | 154 | 0 |
| `gbpaud` | `200709` | `202606` | `warning` | 50 | 521 | 218 | 0 |
| `gbpcad` | `200709` | `202606` | `warning` | 50 | 526 | 223 | 0 |
| `gbpchf` | `200208` | `202606` | `warning` | 60 | 626 | 263 | 0 |
| `gbpjpy` | `200205` | `202606` | `warning` | 60 | 669 | 306 | 0 |

The repo table command used for verification was:

```sh
venv/bin/histdatacom -A \
  -p eurnzd eurpln eursek eurtry eurusd frxeur gbpaud gbpcad gbpchf gbpjpy \
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
  -p eurnzd eurpln eursek eurtry eurusd frxeur gbpaud gbpcad gbpchf gbpjpy \
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
  --quality-report data/.quality/issue-235/issue-235-ascii-m1-<symbol>-quality.json \
  --data-directory data \
  --quality-fail-on never
```

## Reviewed Data Defect

The single hard error is #243:

```text
data/ASCII/M1/eurusd/2001/DAT_ASCII_EURUSD_M1_2001.csv
row 180757
20010911 201200;-.000100;-.000100;-.000100;-.000100;0
```

HistData source timestamps are fixed EST without daylight-saving adjustment, so
`20010911 201200` normalizes to `2001-09-12T01:12:00Z`. The rule
`bars.ascii.m1_ohlc` correctly reports `ASCII_M1_PRICE_NON_POSITIVE` because
all OHLC bid prices are `-0.0001`. This remains a hard error.

## Boundaries

This issue completed the ASCII/M1 ZIP and extracted CSV campaign surface. The
current `histdatacom -D -X` CLI path did not create canonical `.data` cache
files, so the explicit disk-pressure cache cleanup command was a no-op for this
batch:

```sh
find data/ASCII/M1/<symbol> -name .data -type f -delete
```

Deep cache validation itself is covered by #223, and non-ASCII quality support
boundaries are covered by #225. No new parser/support issue was discovered
while closing this batch.
