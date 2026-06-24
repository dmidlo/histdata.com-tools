# Known Data Quality Defects

This file records reviewed source-data defects that should stay visible in
quality reports. It is not an allowlist and does not downgrade severities.

## AUDCHF / CADCHF ~= AUDCAD, 2008 M1

- Tracking issue: #242
- Parent issue: #233
- Tracker issue: #93
- Source report: `data/.quality/issue-241/ascii-m1-all-quality-report.json`
- Source report SHA-256:
  `073cde6bfdd92583778802bd27dff44dfe743c0c6659d8a5fa42164b3c673084`
- Rule: `domain.cross_instrument_consistency`
- Code: `DOMAIN_CROSS_INSTRUMENT_TRIANGULAR_ERROR`
- Classification: reviewed source-data defect
- Decision: keep as a hard error

The #233 batch-1 ASCII/M1 quality run found a triangular consistency failure
for `AUDCHF / CADCHF ~= AUDCAD` in the 2008 files. The sampled timestamp in the
report is `1212357720000` UTC milliseconds, which corresponds to the HistData
fixed EST-without-DST source timestamp `20080601 170200`.

Raw source rows around the sampled timestamp:

| Symbol | Source row | Close bid |
| --- | --- | ---: |
| `AUDCHF` | `20080601 170200;0.994500;0.994500;0.994400;0.994400;0` | `0.994400` |
| `CADCHF` | `20080601 170200;1.047000;1.047000;1.046900;1.046900;0` | `1.046900` |
| `AUDCAD` | `20080601 170200;1.041700;1.041700;1.041700;1.041700;0` | `1.041700` |

For same-quote FX pairs, the direct `AUDCAD` close bid should approximately
match `AUDCHF / CADCHF`. The reproduced implied value is approximately
`0.9498519438`, while the direct `AUDCAD` value is `1.041700`. The relative
difference is approximately `0.0881713124`, far above the default triangular
error tolerance of `0.05`.

This is not a symbol-semantics issue: all three symbols use normal FX
base/quote conventions. It is not a timestamp normalization issue: the source
timestamp is fixed EST without daylight saving and normalizes to the same UTC
millisecond value carried by the report. It is also not a tolerance calibration
edge; the mismatch is materially above the hard-error threshold.

Future full-dataset batches should treat this as an already-reviewed vendor
data defect when interpreting #233/#241 outputs, but the default quality result
should remain failed/error so repo-quality metadata and release gates do not
silently hide the anomaly.
