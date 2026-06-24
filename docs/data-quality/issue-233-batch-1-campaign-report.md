# Issue 233 Batch 1 Campaign Report

Parent: #93. Depends on: #232. Related: #223, #224, #225, #240.

## Result

Status: `blocked`.

The hosted GitHub `.repo` index was reachable and the listed batch rows matched the local tracked `data/.repo`. The full dataset campaign was not downloaded on this workstation because the current free disk is below the explicit operator preflight floor and the planned surface includes tick archives plus non-ASCII formats whose quality boundary is still tracked separately.

## Disk Preflight

- Available: `28.959` GiB
- Minimum operator floor for this full-surface batch: `100.0` GiB
- Status: `blocked`
- Follow-up: #240

## Work Surface

- Symbols: `10`
- Planned archive work items: `10843`
- Deep-supported ASCII work items: `2646`
- Deferred work items: `8197`

| Format | Timeframe | Work items | Deep quality |
| --- | --- | ---: | --- |
| `ascii` | `M1` | 259 | yes |
| `ascii` | `T` | 2387 | yes |
| `excel` | `M1` | 259 | deferred |
| `metastock` | `M1` | 259 | deferred |
| `metatrader` | `M1` | 259 | deferred |
| `ninjatrader` | `M1` | 259 | deferred |
| `ninjatrader` | `T_ASK` | 2387 | deferred |
| `ninjatrader` | `T_BID` | 2387 | deferred |
| `ninjatrader` | `T_LAST` | 2387 | deferred |

## Command Evidence

- `df -h . data`
- `venv/bin/python -m histdatacom -U -p audcad audchf audjpy audnzd audusd auxaud bcousd cadchf cadjpy chfjpy --repo-quality-columns --data-directory data`
- `venv/bin/python - <<PY ... repository_refresh_stage(update_remote_data=True, pairs=batch_1) ... PY`
- `venv/bin/python -m pytest -q tests/unit/test_data_quality_campaign.py`

## Deferred Boundaries

- #240: full-surface execution needs a storage-backed installed/platform-wheel sidecar environment.
- #225: non-ASCII quality support is not complete, so those formats must not be treated as deeply clean.
- #223: canonical cache artifacts still need deep cache validation.
- #224: installed-package sidecar quality smoke remains the release-grade proof path for the packaged Temporal executable.

## Resume Path

Run this batch on a storage-backed installed/platform-wheel environment, starting with repo refresh, then bounded symbol/format/timeframe slices. After each successful slice, run `--repo-quality` so `data/.repo` carries bounded quality summaries while detailed reports remain as ignored artifacts on disk.

Suggested first safe slice once storage is available:

```bash
histdatacom -D -X -p audcad audchf audjpy audnzd audusd auxaud bcousd cadchf cadjpy chfjpy -f ascii -t M1 --data-directory data
histdatacom --repo-quality --quality-target data --quality-report reports/issue-233-ascii-m1.json --data-directory data
```
