# Validation-only vendor M1 comparisons

The `histdatacom.data_quality.vendor_m1` API compares Generic ASCII M1 reference
rows with 1-minute bars derived from the corresponding Generic ASCII/T bytes.
It is a read-only diagnostic: it does not download data, admit M1 into a source
catalog, repair ticks, fill gaps, overwrite reconstructed bars, or qualify a
training dataset. No real-provider agreement was established by its synthetic
tests, and same-provider agreement is not independent market truth.

## Source semantics

HistData documents six semicolon-separated M1 fields:
`YYYYMMDD HHMMSS;open;high;low;close;Volume`. OHLC uses **bid** prices, minute
seconds are `00`, and the clock is fixed EST without daylight-saving changes
(UTC−05:00, not the DST-observing America/New_York zone). See the official
[format specification](https://www.histdata.com/f-a-q/data-files-detailed-specification/)
and [FAQ](https://www.histdata.com/f-a-q/).

`Volume` is the sixth M1 field (the fourth field belongs to T records). Its
raw integer is preserved as `unqualified_nominal_integer_not_traded_volume`.
The separate source-semantic investigation under #653 remains open; neither a
column name nor M1 agreement qualifies centralized traded volume.

## Explicit inputs and replay

Import the supported API directly:

```python
from histdatacom.data_quality.vendor_m1 import (
    VendorM1PolicyV1,
    load_histdata_m1_reference,
    compare_histdata_m1_to_ticks,
    replay_histdata_m1_comparison,
)

reference = load_histdata_m1_reference(
    reference_path,
    expected_sha256=selected_reference_sha256,
    symbol="EURUSD",
    source_period="202001",
)
policy = VendorM1PolicyV1(
    symbol="EURUSD",
    source_period="202001",
    start_ns=start_ns,
    end_ns=end_ns,
    pip_size="0.0001",
    rounding_tolerance_pips="0",
)
report = compare_histdata_m1_to_ticks(
    reference, tick_path,
    expected_tick_sha256=selected_tick_sha256,
    policy=policy,
)
replayed = replay_histdata_m1_comparison(
    report, reference_path, tick_path,
    expected_report_id=independently_retained_report_id,
    expected_reference_sha256=selected_reference_sha256,
    expected_tick_sha256=selected_tick_sha256,
)
```

Expected roots must come from the caller's independently retained selection,
not be replaced with the identities in an unexpected incoming report. Replay
reopens both sources and recomputes the comparison; a structurally valid or
resealed report is not proof. Content possession does not authenticate a
provider or establish full-corpus coverage.

Prices retain exact decimal lexemes. Signed differences are tick-derived minus
vendor-reference (`tick_minus_vendor_v1`), with absolute differences and signed
pip-normalized differences calculated as bounded rational values. Pip size is
explicit; it is not guessed from the currency symbol. The policy version and
rounding tolerance participate in the report identity. Native T parsing and
the existing observed-only bar aggregator independently check timestamp, bin,
count and floating-price projection. Those native bar IDs describe an in-memory
diagnostic, **not** a committed reconstruction publication. Floating rounding
is not used to manufacture exact decimal agreement.

## Eligibility and outcomes

Minute ownership is `[t, t + 60 seconds)`. Separate outcomes are exact match,
bounded rounding match, material mismatch, missing M1, missing tick support,
and partial-query refusal. The denominator is the union of reference minutes
and tick-supported minutes in the declared query; it does not fabricate a
dense market calendar. A complete query bin is not proof of complete provider
coverage. Duplicate M1 minutes and malformed rows are refused, never deduplicated
or repaired. Tick order is strict by default; the explicit bounded-regression
policy uses event time then original row ordinal, retaining regression facts.

Absent availability evidence is not inferred from event timestamps. The default
is historical ex-post comparison only. Optional `available_at_ns` on the M1
reference and `tick_available_at_ns`/`as_of_ns` on the policy remain operator
declarations, not independently attested arrival times. Unknown or unavailable
as-of evidence cannot be consumed as point-in-time eligibility even if price
values match.
Inspect `report.as_of_eligible`, `report.availability_decision`, and
`report.status_counts` separately: price agreement cannot substitute for
availability evidence. The opt-in ordering policy is
`tick_order="bounded_regressions_event_time_row"`.
`bounded_rounding_match` means within the declared tolerance; it does not prove
that rounding, rather than another source difference, caused the discrepancy.
Supplied availability must cover the entire retained snapshot: it cannot
predate the latest T event or the end of the latest M1 minute, including rows
outside the comparison window. Whole-snapshot row/time facts are retained;
future rows are not silently removed to manufacture as-of eligibility.

## Finite scope

V1 accepts regular, single-link ASCII files, at most 32 MiB each, 262,144 ticks,
65,536 reference rows, and 512 bytes per line. Plain decimals have at most 32
digits and 15 fractional places; rational components are bounded to 256 bits.
The query spans at most 65,536 minutes. A conservative per-minute reservation
also enforces a 64 MiB serialized report ceiling before native aggregation;
the reservation permits at most 8,184 retained minute outcomes per report,
independently of the larger query-span limit.
These are wire/work bounds, not a measured peak-memory guarantee. Larger work
must be partitioned explicitly; nothing is silently truncated.
Inputs are plain CSV files only; the adapter performs no ZIP extraction.
JSON admission also preflights depth 20 and 1,048,576 structural tokens before
decoding; integer tokens are bounded before conversion and floating tokens are
refused. These parser limits are compatible with the report-byte reservation.
The opt-in event-time ordering policy admits at most 1,024 timestamp reversals,
each no greater than 3,600,000 milliseconds; those facts remain in the report.

Descriptor/byte checks refuse ordinary source changes and final symlinks.
Ancestor directories are not pinned against a hostile concurrent writer.
Reports distinguish raw vendor-reference lineage from tick-derived native
bar lineage and from synthetic/reconstructed bars; the raw T-only import and
committed bar-reader contracts remain unchanged.
