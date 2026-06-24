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

## EURUSD 2001 M1 Non-Positive OHLC Row

- Tracking issue: #243
- Parent issue: #235
- Tracker issue: #93
- Source report: `data/.quality/issue-235/issue-235-ascii-m1-eurusd-quality.json`
- Source report SHA-256:
  `ea28fdf32980aaac92d24e063df815565b0675e1396faf06872dcf0d8aacfe56`
- Source CSV: `data/ASCII/M1/eurusd/2001/DAT_ASCII_EURUSD_M1_2001.csv`
- Source CSV SHA-256:
  `d21fd56cc2f1df019abf8e6fe46cf52e5bfe8e0caa554b3110cf94564ac7905a`
- Rule: `bars.ascii.m1_ohlc`
- Code: `ASCII_M1_PRICE_NON_POSITIVE`
- Classification: reviewed source-data defect
- Decision: keep as a hard error

The #235 batch-3 ASCII/M1 quality run found one `EURUSD` 2001 row where all
OHLC prices are non-positive:

```text
20010911 201200;-.000100;-.000100;-.000100;-.000100;0
```

HistData source timestamps are fixed EST without daylight-saving adjustment.
The source timestamp `20010911 201200` normalizes to
`2001-09-12T01:12:00Z`.

Nearby rows contain ordinary positive EURUSD prices before and after the
defect:

```text
20010911 182500;0.911100;0.911100;0.911000;0.911000;0
20010911 182600;0.910900;0.910900;0.910800;0.910800;0
20010911 201200;-.000100;-.000100;-.000100;-.000100;0
20010911 212300;0.910900;0.910900;0.910800;0.910800;0
20010911 212600;0.910700;0.910700;0.910600;0.910600;0
```

This is not a timestamp normalization issue and not an OHLC ordering issue. It
is a vendor/source row with an impossible negative bid price across all OHLC
columns. Future full-dataset batches should treat this as an already-reviewed
source-data defect, but the default quality result should remain failed/error
so operator-facing repo-quality metadata does not hide the anomaly.
