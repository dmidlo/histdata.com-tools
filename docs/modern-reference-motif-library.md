# Real modern reference-motif library

The production library turns small, synchronized windows from real monthly
HistData ASCII tick caches into a compact `ReferenceMotifIndexV1`. It is the
installed build and qualification boundary for issue #464; it does not package
dense source or holdout rows.

## Fixed production profile

The default profile uses EURUSD, GBPUSD, and EURGBP inside the stable
`technology_epoch_04` interval established by the v2 feed-epoch campaign.
Chronological roles are fixed before extraction:

| Role | Periods |
| --- | --- |
| train | 201901, 202001, 202101, 202201, 202301 |
| calibration | 202307 |
| validation | 202401 |
| final holdout | 202510 |

Six synchronized ten-minute parent windows per period cover Asia, London, and
New York plus available point-in-time event windows. Each symbol contributes
at most 96 real quotes per parent. Three-event fragments are needed to support
the short anchor gaps that caused the provisional #463 candidate to refuse;
the retained index is deterministically bounded to 256 fragments and 64 query
matches.

Every monthly `.data` digest must equal the immutable source hash in the v2
feed-epoch lineage. The manifest records all 24 selected partitions, their
periods, split assignments, hashes, row counts, sizes, and evidence IDs. It
records only compact offsets, quote deltas, transition marks, and source-row
identities—not the dense rows themselves.

## Feature and leakage policy

`reference_motif_condition_from_quotes()` is shared by library extraction and
benchmark retrieval. Its fixed, versioned feature schema covers symbol,
session, weekday, event/news and CFTC state, epoch, return, range, volatility,
spread, activity, inter-arrival, timestamp/price precision, and source quality.
Thresholds are code constants and are not fitted on a withheld period.

All calibration, validation, and final-holdout fragments are projected only
for leakage and coverage audits. A train fragment whose normalized timing,
bid/ask shape, and transition signature occurs in any later role is removed
with an explicit exclusion reason. The ordinary fail-closed index builder then
reruns its source-overlap and near-duplicate audit over the filtered set. Only
eligible train fragments can remain in the persisted index.

Coverage is aggregated by symbol, session, epoch, event state, volatility,
activity, spread, weekday, and split. For every retained stratum, withheld
queries publish query counts, match/refusal status, backoff counts, and actual
backoff rates. A zero-distance query for an impossible epoch must return
`no_supported_cell`; the builder refuses an emitted patch in that case.

## Candidate corrections and qualification

The provisional benchmark exposed five implementation seams that fixtures had
not exercised:

1. long source fragments could not support short real anchor gaps;
2. uniformly spaced output discarded the source event clock;
3. candidate event-state labels did not reflect actual bid/ask transitions;
4. rounded bid-only and ask-only marks could collapse into unchanged quotes;
5. unbounded full-library rescans made a period-scale qualification process
   appear to crash.

The qualified generator cycles the selected fragment's empirical gap weights
inside each anchor interval, materializes bid-only/ask-only/both/unchanged
marks after rounding, preserves the linear internal transform seams and raw
anchors, and recomputes benchmark transition state from the merged quote
stream. Price residual amplification is bounded to `[0.05, 1.0]`. The compact
256-fragment shortlist keeps benchmark memory and runtime bounded while the
manifest still publishes support and omissions from the complete source pool.

Qualification reuses the unchanged policy frozen for #463. It runs the real
validation and final-holdout windows twice, compares the complete candidate
report for deterministic replay, and records transparent dense, degraded,
interpolation, and negative controls. Closure requires the empirical candidate
to be non-provisional and promotion eligible, with zero failures, refusals,
anchor violations, and unsupported emissions.

## Installed command and artifacts

```console
histdatacom analytics modern-reference-motif-library \
  --source-root data/ASCII/T \
  --definition data/.histdatacom/analytics/feed-epochs-v2-issue-460/feed-epochs-v2-definition.json \
  --market-context-corpus .histdatacom/market-context-461-final-v4/market-context-corpus-9255f8c39f999b7a54e41a59a6f1d96f02e897af8383795e464a2f8738b08e00.json \
  --cftc-positioning-corpus data/.histdatacom/analytics/cftc-positioning-issue-468-final/cftc-positioning-corpus-887a47840090cdab1982fe910a4bdf8c1fcc9af256ab687bceae1b8dd1cbd3e0.json \
  --benchmark-manifest data/.histdatacom/analytics/reverse-degradation-benchmark-issue-463-final-v5/reverse-degradation-manifest-d1ddf45d68ade8c1ba4abc3df5a60a26483bb3eab950d4c29f53709e9214ed24.json \
  --artifact-dir data/.histdatacom/analytics/modern-reference-motif-issue-464-final-v4
```

The command writes six content-addressed JSON artifacts:

- production index;
- source/feature/split manifest;
- leakage audit;
- support and backoff coverage;
- frozen-policy benchmark qualification; and
- runtime, peak-memory, source, scratch, and compact-storage audit.

`read_modern_reference_motif_index()` and
`read_modern_reference_motif_artifact()` are installed hash-verifying readers.
Existing content-addressed files are reused only when their bytes are exact;
otherwise the writer fails.

## Issue #464 reference campaign

The authoritative local evidence directory is
`data/.histdatacom/analytics/modern-reference-motif-issue-464-final-v4`.
The library ID is
`modern-reference-motif-library:sha256:a723fb5ce639dd4363a02e5680f5b640f53d9eb4fe5652fd13834174870b3e0e`
and its installed index ID is
`reference-motif-index:sha256:b5d5e7d9580fac375c42677fe5d03be96fafc190f799364a52566af7aa5a2589`.

The build verified 24 monthly sources containing 49,630,455 real Arrow rows
and 1,389,795,816 bytes. From 4,572 projected three-event windows it removed
28 train shapes found in later roles, audited 69,754 post-exclusion
comparisons with zero findings, and retained 256 train fragments. No
calibration, validation, or final-holdout fragment is persisted. All 1,692
withheld queries matched through the declared `symbol_epoch_session` backoff;
2,288 source windows carry a matched market event state, and retained symbol
support is 78 EURGBP, 85 EURUSD, and 93 GBPUSD fragments.

The empirical candidate is non-provisional and passed every frozen hard gate
with zero failures, refusals, raw-anchor violations, and unsupported emissions.
Worst real-window errors were `0.17317708333333334` for event count,
`0.2850022470073947` for inter-arrival shape, `0.6261398637127946` for path
variation, `0.11612903225838411` for spread tail, and
`0.5156356696373855` for update transitions. Deterministic replay, variable
cardinality, internal boundary continuity, immutable anchors, and unsupported
refusal all passed.

The complete build and two benchmark replays took 43.596332 seconds at
550,666,240 bytes peak RSS, used no scratch, and wrote 856,512 compact bytes.
The six exact artifact SHA-256 values are:

| Artifact | SHA-256 |
| --- | --- |
| index | `048dd46deabf66643fc55b9cb2a996828c88f8c099a9d9573a4d29a632bce9a3` |
| manifest | `3ab008c97a45d5930f958d2cfd7cb8d2b9d14fc6fdcb5fd84ab73440bc1adb36` |
| leakage audit | `97936f21c74703a42e1074835bea05a5c7ac1320e3df0104602d46ff9717fc2e` |
| coverage | `8b12a79da78e57ff4422fa7f394564be03d4f6c414d9a3061b3075bc9988b3ce` |
| qualification | `fb6fac22d7f1c8a9e71a4c55160147b285c643e03cc9ccdb165b77bd8f6dc927` |
| resource audit | `772bbd1c533b332edb1afbd883364e50cf09608cc3b7a3594b28881bd1920bea` |

## Nonclaims

- The library does not identify the exact historical ticks that were missing.
- It does not use bars, synthetic OHLC, broker style, or a neural generator.
- Event/CFTC conditioning is retrieval context, not a causal trading claim.
- A benchmark gate pass does not select a strategy or authorize publication.
