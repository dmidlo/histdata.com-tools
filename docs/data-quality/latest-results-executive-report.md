# Data Quality Executive Report

This report is generated from the latest existing local data-quality JSON outputs. It does not rerun the full data-quality battery; it sanitizes the existing evidence set and renders a publication-safe view for GitHub.

## Publication posture

| Control | Value |
| --- | --- |
| Source | `data/.quality` |
| Raw local paths sanitized | yes |
| Existing reports cleaned in place | yes |
| Full quality battery rerun | no |

## Corpus

| Measure | Count |
| --- | ---: |
| Report files | 65 |
| Source bytes | 285,293,487 |
| Report-counted targets | 4,274 |
| Report-counted findings | 37,513 |
| Informational findings | 21,106 |
| Warning findings | 15,489 |
| Error findings | 918 |

The report-counted totals intentionally preserve the evidence exactly as produced by each JSON output. Some reports overlap by issue, symbol, or campaign batch, so these counts should not be read as a deduplicated inventory of unique market-data files.

## Issue-level inventory

| Issue | Reports | Statuses | Targets | Findings | Warnings | Errors |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| issue-233 | 6 | clean: 2, failed: 1, unknown: 3 | 2,074 | 3,475 | 1,098 | 916 |
| issue-234 | 10 | warning: 10 | 267 | 5,135 | 2,112 | 0 |
| issue-235 | 10 | failed: 1, warning: 9 | 276 | 5,399 | 2,269 | 1 |
| issue-236 | 10 | warning: 10 | 265 | 5,104 | 2,119 | 0 |
| issue-237 | 10 | warning: 10 | 266 | 5,072 | 2,077 | 0 |
| issue-238 | 10 | warning: 10 | 249 | 4,943 | 2,117 | 0 |
| issue-239 | 6 | warning: 6 | 97 | 1,727 | 682 | 0 |
| issue-241 | 1 | failed: 1 | 778 | 6,642 | 3,015 | 1 |
| unscoped | 2 | clean: 2 | 2 | 16 | 0 | 0 |

## High-signal findings

### Finding codes

| Code | Count |
| --- | ---: |
| `ASCII_ROW_COUNT_SUMMARY` | 2,665 |
| `DOMAIN_SYMBOL_METADATA_SUMMARY` | 1,886 |
| `MODELING_READINESS_SUMMARY` | 1,886 |
| `MODELING_BID_ONLY_EXECUTION_RISK` | 1,886 |
| `MODELING_CURRENT_BAR_LEAKAGE_RISK` | 1,886 |
| `MODELING_SPREAD_COST_MISSING` | 1,886 |
| `ASCII_TIMESTAMP_EST_NO_DST_SUMMARY` | 1,629 |
| `ASCII_TIMESTAMP_SEQUENCE_SUMMARY` | 1,629 |
| `ASCII_TIMESTAMP_GAP_SUMMARY` | 1,629 |
| `ASCII_M1_OHLC_SUMMARY` | 1,628 |
| `ASCII_M1_PRECISION_SUMMARY` | 1,628 |
| `ASCII_M1_OUTLIER_SUMMARY` | 1,628 |
| `DOMAIN_CALENDAR_SESSION_SUMMARY` | 1,627 |
| `ASCII_TIMESTAMP_SUSPICIOUS_GAP` | 1,572 |
| `ASCII_TIMESTAMP_EXPECTED_SESSION_CLOSURE_GAP` | 1,422 |

### Rule IDs

| Rule | Count |
| --- | ---: |
| `inventory.zip.integrity` | 3,639 |
| `ingestion.ascii.row_count` | 3,123 |
| `ingestion.ascii.text` | 3,123 |
| `ingestion.ascii.schema` | 3,123 |
| `time.ascii.est_no_dst` | 2,605 |
| `time.ascii.sequence` | 2,605 |
| `time.ascii.gaps` | 2,605 |
| `bars.ascii.m1_ohlc` | 2,604 |
| `bars.ascii.m1_precision` | 2,604 |
| `bars.ascii.m1_outliers` | 2,604 |
| `ticks.ascii.spread` | 2,604 |
| `ticks.ascii.microstructure` | 2,604 |
| `ticks.ascii.spread_regimes` | 2,604 |
| `domain.symbol_metadata` | 2,603 |
| `domain.calendar_sessions` | 2,603 |

## Report inventory

| Source | Status | Targets | Findings | Warnings | Errors |
| --- | --- | ---: | ---: | ---: | ---: |
| `data/.quality/issue-233/ascii-m1-ingestion-quality-report.json` | clean | 518 | 518 | 0 | 0 |
| `data/.quality/issue-233/ascii-m1-ingestion-quality-summary.json` | unknown | 0 | 0 | 0 | 0 |
| `data/.quality/issue-233/ascii-m1-inventory-quality-report.json` | clean | 777 | 0 | 0 | 0 |
| `data/.quality/issue-233/ascii-m1-inventory-quality-summary.json` | unknown | 0 | 0 | 0 | 0 |
| `data/.quality/issue-233/ascii-m1-quality-report.json` | failed | 779 | 2,957 | 1,098 | 916 |
| `data/.quality/issue-233/ascii-m1-summary.json` | unknown | 0 | 0 | 0 | 0 |
| `data/.quality/issue-234/issue-234-001-ascii-m1-etxeur-quality.json` | warning | 12 | 190 | 75 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-euraud-quality.json` | warning | 32 | 632 | 263 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurcad-quality.json` | warning | 27 | 531 | 222 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurchf-quality.json` | warning | 32 | 619 | 250 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurczk-quality.json` | warning | 24 | 450 | 177 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurdkk-quality.json` | warning | 26 | 466 | 169 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurgbp-quality.json` | warning | 32 | 624 | 255 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurhuf-quality.json` | warning | 24 | 436 | 180 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurjpy-quality.json` | warning | 32 | 671 | 302 | 0 |
| `data/.quality/issue-234/issue-234-ascii-m1-eurnok-quality.json` | warning | 26 | 516 | 219 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-eurnzd-quality.json` | warning | 26 | 508 | 211 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-eurpln-quality.json` | warning | 24 | 455 | 182 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-eursek-quality.json` | warning | 26 | 515 | 218 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-eurtry-quality.json` | warning | 24 | 473 | 207 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-eurusd-quality.json` | failed | 34 | 674 | 282 | 1 |
| `data/.quality/issue-235/issue-235-ascii-m1-frxeur-quality.json` | warning | 24 | 404 | 155 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-gbpaud-quality.json` | warning | 27 | 528 | 219 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-gbpcad-quality.json` | warning | 27 | 533 | 224 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-gbpchf-quality.json` | warning | 32 | 633 | 264 | 0 |
| `data/.quality/issue-235/issue-235-ascii-m1-gbpjpy-quality.json` | warning | 32 | 676 | 307 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-gbpnzd-quality.json` | warning | 26 | 511 | 214 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-gbpusd-quality.json` | warning | 34 | 686 | 294 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-grxeur-quality.json` | warning | 24 | 416 | 161 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-hkxhkd-quality.json` | warning | 24 | 395 | 146 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-jpxjpy-quality.json` | warning | 24 | 441 | 169 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-nsxusd-quality.json` | warning | 24 | 431 | 159 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-nzdcad-quality.json` | warning | 26 | 516 | 219 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-nzdchf-quality.json` | warning | 26 | 530 | 233 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-nzdjpy-quality.json` | warning | 28 | 592 | 271 | 0 |
| `data/.quality/issue-236/issue-236-ascii-m1-nzdusd-quality.json` | warning | 29 | 586 | 253 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-sgdjpy-quality.json` | warning | 26 | 537 | 240 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-spxusd-quality.json` | warning | 24 | 438 | 165 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-udxusd-quality.json` | warning | 24 | 427 | 169 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-ukxgbp-quality.json` | warning | 24 | 426 | 167 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-usdcad-quality.json` | warning | 34 | 667 | 276 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-usdchf-quality.json` | warning | 34 | 680 | 289 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-usdczk-quality.json` | warning | 24 | 469 | 196 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-usddkk-quality.json` | warning | 26 | 503 | 206 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-usdhkd-quality.json` | warning | 26 | 462 | 165 | 0 |
| `data/.quality/issue-237/issue-237-ascii-m1-usdhuf-quality.json` | warning | 24 | 463 | 204 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdjpy-quality.json` | warning | 34 | 722 | 331 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdmxn-quality.json` | warning | 24 | 479 | 206 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdnok-quality.json` | warning | 26 | 527 | 230 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdpln-quality.json` | warning | 24 | 475 | 202 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdsek-quality.json` | warning | 26 | 528 | 231 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdsgd-quality.json` | warning | 26 | 489 | 192 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdtry-quality.json` | warning | 24 | 477 | 205 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-usdzar-quality.json` | warning | 24 | 489 | 216 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-wtiusd-quality.json` | warning | 16 | 286 | 114 | 0 |
| `data/.quality/issue-238/issue-238-ascii-m1-xagusd-quality.json` | warning | 25 | 471 | 190 | 0 |
| `data/.quality/issue-239/issue-239-ascii-m1-xauaud-quality.json` | warning | 12 | 189 | 66 | 0 |
| `data/.quality/issue-239/issue-239-ascii-m1-xauchf-quality.json` | warning | 12 | 193 | 71 | 0 |
| `data/.quality/issue-239/issue-239-ascii-m1-xaueur-quality.json` | warning | 12 | 189 | 67 | 0 |
| `data/.quality/issue-239/issue-239-ascii-m1-xaugbp-quality.json` | warning | 12 | 192 | 70 | 0 |
| `data/.quality/issue-239/issue-239-ascii-m1-xauusd-quality.json` | warning | 25 | 451 | 168 | 0 |
| `data/.quality/issue-239/issue-239-ascii-m1-zarjpy-quality.json` | warning | 24 | 513 | 240 | 0 |
| `data/.quality/issue-241/ascii-m1-all-quality-report.json` | failed | 778 | 6,642 | 3,015 | 1 |
| `data/.quality/reports/run-92a6621e60044fc687a09f09df9b60cd.json` | clean | 1 | 9 | 0 | 0 |
| `data/.quality/reports/run-b49f799d1f68434391e87d62432b12f6.json` | clean | 1 | 7 | 0 | 0 |

## Operational interpretation

The current evidence set is suitable for publication because local home directories, sidecar workspace locations, temporary directories, and absolute report paths are converted to relative project/report paths. The detailed raw report tree remains a local working artifact; the tracked GitHub surface is this executive report and the compact JSON index.
