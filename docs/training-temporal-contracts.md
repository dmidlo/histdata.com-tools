# Temporal training contracts

Issue #608 adds contract SemVer **1.0.0** for the v2.10 research substrate.
The frozen #606 training-row and #751 diagnostic contracts are unchanged.
These are executable research contracts, not an empirical result or a historical
availability certificate.

## Supported evidence and distinct guarantees

The public consumer uses existing canonical artifacts:

- A complete `TrainingSourceV1` catalog, its complete `TrainingOwnershipV1`,
  and every declared committed product and derived artifact are freshly replayed.
  Quote values are read from verified IPC/CSV source bytes; generated values are
  read from replayed committed products with exact observed-anchor ancestry.
- A declared #561 `FeatureMatrixSnapshotV1` is restored through its strict,
  content-addressed reader. Its retained observations, vintage chains, definitions
  and schedules are resliced at the decision cutoff and its cells recomputed.
  The saved final cell is not backdated.
- `temporal_plan_from_training_batch` binds the entire canonical #606 batch.
  Each invocation replays that original batch as well as the temporal operation.
  Its decision rows, product/member, source and complete ownership must agree.

Four dimensions must not be conflated: source/value replay; consistency with
retained normalized clocks; validity of the declared split/dependency policy;
and authenticity of historical availability. The first three are executable
checks. The fourth is **not established** by a normalized record, digest,
availability declaration, fixture, or this API.

`NORMALIZED_AS_OF` admits only clocked vintage values at the requested cutoff.
It does not claim official raw-source authenticity. `EX_POST` explicitly admits
source-replayed quote/reconstruction research with unknown historical
availability. Such values retain `available_at_ns=None`; neither a row status
of `admitted` nor a successful replay makes them causal or generally ML-eligible.
Legacy batches cannot be promoted to normalized-clock mode.

Current unsupported families include arbitrary external feature functions,
caller-fitted model/indicator state, arbitrary label values, direct bar snapshot
inputs, provider receipts not mapped into the declared source inventory, and
unqualified historical causal quote inputs. These fail closed; there is no
generic "verified ID" or clock-override parameter.

## Features and state

`QUOTE_AT_DECISION` means an exact decision-time quote, not last-known sampling.
No implicit stale carry or interpolation is performed and a lookback argument
is refused. At duplicate clocks, the last physical source ordinal (or native
event sequence for the exact selected member) determines the quote.
Positive uncrossed midpoint arithmetic is stable at subnormal and maximal
finite prices. Crossed selected raw quotes are refused, not silently projected.

Quote rolling mean and sample z-score use only `(t-lookback,t]` observations.
The complete declared lookback remains a dependency even with sparse support.
Zero/nonfinite sample scales and unrepresentable arithmetic refuse. Labels,
future observations and caller-fitted normalization parameters cannot enter
these computations. Vintage transformations retain their existing fixed-grid,
no-skip-missing semantics and their exact observation/definition/schedule IDs.

State crossing a partition boundary is refused by default.
`WARMUP_ONLY` permits explicitly retained earlier observation support, not a
fitted trader/model state, labels or future conditioning. Its contributing
historical-unit IDs remain in the result. Warmup does not waive purge/embargo;
warmup-dependent rows may be returned as explicitly excluded and are not scored.
No implicit cross-partition state is reused.

## Labels and clocks

Each example freezes a decision `t`, positive horizon `h`, label cutoff,
symbol, historical owner, partition, feature definitions, target source and
label definition. Targets use exactly `(t,t+h]`; the baseline at `t` is
separately retained and is not a future target. Exact baseline and horizon
endpoints are mandatory, with no nearest-row substitution or interpolation.

Supported definitions are:

- log return: `log(P(t+h)) - log(P(t))`;
- sampled realized variance: sum of squared adjacent log-price increments
  from the baseline through the concrete future target observations;
- direction: `1{return > frozen_nonnegative_deadband}` (not a universal
  economic-good/bad, trading-profit, or three-class direction claim);
- event response: the same log return, owned by an exact **initial macro
  release** whose retained availability equals the decision time. Quotes,
  revisions and classifications cannot masquerade as release events.

Targets are either actual verified quote midpoints for the exact native member
or initial MARKET-vintage levels from an exact declared feature series. Unit,
scale, base and other semantic identities must be homogeneous across the
actual target support. Later revisions never substitute for initial targets.
A missing/unavailable point inside the required support refuses; an unrelated
future observation does not alter an earlier label.

For clocked targets, baseline availability is no later than `t`; every target
and definition is available by the label cutoff. For ex-post native labels,
unknown availability stays unknown and the cutoff must cover the entire
reconstruction conditioning interval, not just `t+h`.
Every outcome retains concrete baseline/target/event IDs, actual dependency
bounds and all touched historical ownership units.

## Complete splits, purge and embargo

The split map assigns **every** unit of the freshly verified #606 ownership
inventory in chronological train/validation/test order, including empty
calendar units. Missing, extra, duplicate or reordered assignments refuse.
Symbols, scenarios, populations and all selected synthetic members inherit
their original historical owner; they cannot create independent partitions.
A changed complete source inventory requires a newly verified ownership map.

The closed plan computes its maximum dependency span before any output
selection or admission filtering. The span includes declared feature lookback,
actual vintage support, all baseline/target conditioning support and the
inclusive target endpoint. At each partition boundary `b`, decisions in
`(b-span,b)` are purged and `[b,b+span)` embargoed. A target dependency owned
by another partition is also excluded. This conservative policy is frozen as
`complete-unit-chronological-purge-embargo.1.0.0`, not a tunable result-driven
margin. Different definitions or horizons create a different plan identity.

The full output has exactly one result for every planned example, including
exclusion reasons; no successful truncation exists. Projection through
`training_temporal_rows`, including an empty selection, occurs only after
fresh complete source/ownership/value replay.

## Executable API and persistence

Import the types from
`histdatacom.data_quality.training_temporal_contracts`, and the operations from
`training_temporal_views` and `training_temporal_artifacts`.

```python
batch = materialize_training_temporal(plan)
rows = training_temporal_rows(
    batch, information_mode=plan.information_mode, admitted_only=True
)
path = write_training_temporal_artifact(batch, output_directory)
restored = read_training_temporal_artifact(
    path, information_mode=plan.information_mode
)
assert restored == batch
```

The canonical batch binds the complete source and ownership, optional complete
legacy batch, exact split map, mode, every cutoff/feature/label/horizon, native
product/member selection, all results, maximum span and boundaries. Existing
contract v1 meanings are not changed. All durable temporal classes use strict
`from_dict/from_json` and `to_dict/to_json`; those constructors establish
bounded structure only. Filesystem readers and writers additionally re-execute
the whole operation. A correctly re-sealed false metric, clock, ownership map,
or exclusion ledger is rejected by replay.

Writes use a same-directory temporary, file fsync, atomic no-clobber hard-link
publication, and directory fsync where supported. Existing conflicting files,
symlinks, directories and FIFOs refuse. Reads are bounded regular-file/no-follow
operations and require the content-addressed filename and canonical bytes.

Limits are 256 examples, 32 features per example, 4,096 aggregate selected
quote points, 8 MiB canonical envelope, depth 16 and the inherited 200,000-node
structural bound. Existing source replay additionally enforces #606 declared
source row/byte limits. Actual-support interval unions avoid validating unused
quote gaps as numerical inputs; complete source integrity is still checked.
Quote support is reused only within a single closed materialization, never as a
persistent/global verification cache. Budget excess refuses without truncation.

## Validation and scope

Controlled synthetic IPC, genuine committed synthetic products and persisted
canonical vintage matrices exercise the same public paths. Canaries cover
member split leakage, revised macro and final classification leakage,
future-contaminated rolling normalization, and forbidden state transfer.
Additional tests cover full native conditioning, exact interval math, future
unavailable records, source corruption under empty projection, correctly
re-sealed forgeries, partial ownership, numerical extremes and atomic I/O.

These tests establish architecture and computation on controlled evidence.
They do not authorize protected inputs, prove historical clock authenticity,
freeze a real empirical train/test campaign, certify predictive performance,
or satisfy any separate holdout approval.
