# HistData Raw Data Specification

The application currently supports one raw HistData dimension:

| Format | Timeframe | Archive member |
| --- | --- | --- |
| `ascii` | `T` / `tick-data-quotes` | `DAT_ASCII_<SYMBOL>_T_<YYYY[MM]>.csv` |

Other platform formats and raw bar timeframes are intentionally out of scope.
Future downsampling and platform-specific exports should be derived from the
ASCII tick cache substrate.

## ASCII Tick Quotes

Example member name:

```txt
DAT_ASCII_EURUSD_T_201202.csv
```

Rows are comma-delimited:

```txt
YYYYMMDD HHMMSSmmm,bid,ask,volume
```

Example row:

```txt
20120201 000003660,1.306600,1.306770,0
```

Semantics:

| Column | Description |
| --- | --- |
| `datetime` | HistData timestamp in EST without daylight-saving adjustment, converted internally to UTC epoch milliseconds |
| `bid` | Bid price as a floating-point value |
| `ask` | Ask price as a floating-point value |
| `volume` | Tick volume as an integer |

The canonical internal cache stores this data as a Polars Arrow IPC `.data`
file under the ASCII/T directory layout.
