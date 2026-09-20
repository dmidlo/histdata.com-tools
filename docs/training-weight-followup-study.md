# Synthetic-member weighting: separate follow-up study

Design identity: `training-weight-followup-design`, version **1.0.0**.
Tracking: [design #750](https://github.com/dmidlo/histdata.com-tools/issues/750),
parent [research #607](https://github.com/dmidlo/histdata.com-tools/issues/607).
Status: **design only; no new empirical execution is authorized or reported**.

This design specifies a bounded exploratory diagnostic study, followed by a
decision gate and a separate confirmation protocol. It does not change the
[original weighting protocol or failed attempt](training-weight-contracts.md),
implement an executor, qualify reconstruction, or authorize a release. Version
1.0.0 identifies these design semantics, not the package version or an approval.
Changes to the diagnostic questions, inputs, selection rules or estimand require
a separately versioned design and explicit deviation record before execution.

## Why a separate study is necessary

Attempt 001 admitted none of its 62 scheduled triangle-days. Its terminal remains
`failed` at `training_export`, without an evaluation. The later empty-export
fix does not reclassify that attempt. Even if every engine and artifact-budget
failure were repaired, unchanged source/degradation admission gives:

| Role | Scheduled days | Source/degradation refusals | Maximum remaining | Required |
| --- | ---: | ---: | ---: | ---: |
| Calibration | 42 | 17 | 25 | 30 |
| Application | 20 | 14 | 6 | 20 common units |

These are generous ceilings, not achieved sample sizes or independent samples.
The 26 stale-probe refusals combine parent and thinned-subset checks. They do not
say whether the raw source lacks support or the degradation introduced the gap.
The 30 engine refusals omit the failing member, symbol, interval and numerical
time-scale comparison. Those omissions motivate diagnosis, not looser gates.

The original protocol remains the asset committed in
`4e1eadf6862a192906d2c8f69efd356578252044`, file SHA-256
`025178395cc8da1224132e8a480428e7ea7324e56c2859a2cf9a9f1d5d703b55`.
Attempt 001's terminal SHA-256 remains
`f4d3e51cc348b01c9130ca7d0e2f28bbd3051448387c7a64b2acaad2479ed1c3`.
Neither file is an output destination for the follow-up.

## Study sequence and permitted conclusions

| Stage | Inputs and work | Output | Cannot establish |
| --- | --- | --- | --- |
| A: source/degradation diagnosis | All 62 old dates, fixed parent/subset rules | Complete paired support inventory | Generator feasibility or policy accuracy |
| B: generator diagnosis | Four preselected old failure days, same fixed model/configuration | New, explicitly identified diagnostic realization | Failure prevalence, exact numerical cause of an unrecorded old draw, or policy superiority |
| C: design decision | A/B evidence and synthetic regressions | One frozen successor method, or a documented stop | Confirmation on exposed data |
| D: separately registered confirmation | Independently qualified fresh nonprotected support | Source-bound six-policy comparison and limitations | Protected-release approval, path recovery or trading profitability |

There is no automatic transition from C to D. Exploration informs a later
registration; registering a plan after seeing data does not make that data fresh.
This separation follows the [Center for Open Science's preregistration guidance](https://www.cos.io/initiatives/prereg).

## Exploratory protocol: fixed scope

The proposed diagnostic attempt is `issue607-followup-diagnostic-001`, with an
exploratory-only evidence kind and a new no-clobber output directory. The exact
diagnostic protocol bytes and final implementation/runtime must be committed,
reviewed and separately approved before it may start. This Markdown design is
not that machine-readable approval.

The input allowlist is defined by the immutable original protocol, not a new
filesystem discovery:

- Exactly its nine EURGBP/EURUSD/GBPUSD partitions for January 2010, January
  2011 and February 2011, with the declared hashes, byte sizes and physical row
  counts. They contain 2,989,245 rows and 83,714,359 bytes in total.
- Exactly its 62 scheduled UTC weekdays and complete three-symbol graph. Old
  fit/application labels remain provenance only; all 62 are now exploratory.
- Its complete 256-fragment, 20-TRAIN-day index, SHA-256
  `048dd46deabf66643fc55b9cb2a996828c88f8c099a9d9573a4d29a632bce9a3`,
  and its 15 declared TRAIN source references. Verify immutable ancestry and
  bytes; no index refit, fragment deletion, reranking or new training source.
- Its epoch definition, SHA-256
  `28f41c7f6081e5eb6e30ed3d1a11229e33da19dc8d6e4197fcf17098cd0c15b1`.
  Historical queries retain `technology_epoch_03`; modern-reference backoff
  remains explicit, ex post, and not historical information availability.

Use the unchanged 08:00–08:10 UTC half-open core, 07:59–08:11 support, nearest
boundary rules, at least 64 core parent rows, at most 4,096 support rows per
symbol, and all 600 one-second probes with an inclusive 60-second age limit.
Keep both boundaries and interior ordinals 0, 4, 8, ... exactly as before.
No new time window, quote fill, alternative thinning, new model, seed search,
external context or whole-corpus replay is part of A/B.

Reads retain original physical ordinal and duplicate-timestamp order. Validate
bounded regular-file snapshots and hashes before and after access. Invalid,
missing or changed sources are operational/integrity failures, not dates to
silently drop. Previously exposed sources are not relabelled fixtures.

### A. Separate raw support from degradation failure

Compute a complete 62-day by three-symbol inventory: **186 cells**, including
cells the original first-failure path never reached. For every cell retain:

1. Input identity, UTC date, core/support boundaries, support/core row counts,
   boundary presence/distances, duplicate-time count and invalid-quote count.
2. For the unchanged parent support, probe success count, missing-prior count,
   stale count, maximum defined age and the earliest failing probe with the
   selected physical ordinal/time/age, or explicit missing-prior status.
3. The same diagnostics for the exact degraded subset; also retained and hidden
   row counts and the hash of the exact parent-to-subset ordinal map.
4. Independent gate flags and prerequisites, not just one winning refusal.
   Use `not_evaluable` when absent anchors or invalid/budget-exceeding support
   prevents a downstream calculation; do not invent ages or count it as a pass.
   Counts are exact only after a complete bounded pass; otherwise label the
   available lower bound and incomplete status explicitly.

Evaluate parent and subset probe support separately whenever their prerequisites
hold. Report `parent_failed`, `subset_only_failed`, `both_passed`, or
`not_evaluable`, with the gate flags preserving other simultaneous failures.
Do not output hidden bid/ask values, reconstruction errors, policy losses or
quality rankings. Reading quote values for integrity is not permission to use
hidden values in generation or method selection.

Reconcile the original short-circuit result in a separate field. The unchanged
admission rules should reproduce the old **17 + 8 + 6 = 31** bridge-eligible
days, with 31 source/degradation-refused days. Any mismatch is an implementation
or evidence discrepancy: stop before B and investigate under a new explicit
diagnostic amendment. Do not overwrite the old manifest with a revised count.

### B. Diagnose generation without selecting successful members

Use a purposive failure sample: the earliest original date in each
`(month, candidate-refusal-code)` cell of the retained nine shard manifests.
This rule gives exactly the following four days, fixed before new execution:

| Date | Original reason used for selection |
| --- | --- |
| 2010-01-04 | Unsupported transform, January 2010 |
| 2010-01-15 | Expanded artifact node budget, January 2010 |
| 2011-01-04 | Unsupported transform, January 2011 |
| 2011-02-02 | Unsupported transform, February 2011 |

Process those days in ascending UTC order, all four base seeds 60701–60704 in
order, and EURGBP/EURUSD/GBPUSD in that order. The denominator is **48
member/symbol cells**. The other 27 source-admitted days are explicitly
`not_selected_by_diagnostic_design`; the 31 source-refused days retain their A
reasons. This sample cannot estimate failure prevalence or confirmation yield.
Do not replace a selected day if its new stochastic realization has a different
outcome. Its original reason is selection provenance, not the expected new result.

Use empirical motif generator 1.2.0 and the complete original configuration,
including at most 64 retrieved matches. Walk intervals in original anchor order.
For each member/symbol stop at its first deterministic engine/resource refusal;
continue the other cells only within the unchanged day/attempt budget. Later
intervals of that cell are `not_attempted_after_refusal`, never successful.
Operational failures abort the attempt, not just the affected cell.

For a refused interval retain the day/member/symbol, anchor IDs and times,
query/result and ordered retrieved-match identities, the retained canonical
query/condition payload, configuration and run identity. Retain timestamp
precision, target cadence, the exact bounded planner target-time vector, declared
event count, output cursor, failing segment ordinal and its derived seed, and
the previous selected-transform trace. Content-addressed references are allowed
only when their full values are retained and charged to the day byte/node budget;
a bare hash cannot reconstruct omitted values. Timestamp quantization and
per-segment seed derivation must remain independently replayable. Preserve
native decision code and bounded diagnostic details.

For cadence/time-scale refusal, record for each actually retrieved fragment:
duration, source gap count, allowed scale interval, tested event-count count,
normal-scale extrema, terminal-adjusted extrema where the existing terminal
branch applies, and counts below/inside/above the allowed interval. Keep the
first offending ordinal and the exact inputs needed to recompute these summaries.
These are at most 64 summary rows, not an unbounded match-by-count matrix.
Report query refusal, zero interior timestamps, native-event bound, transform
bound, serialization-byte bound and expanded-node bound distinctly. Missing
diagnostics due to their own bound are an explicit diagnostic failure.
Charge shared query/native content to the complete day representation as well
as cell-level work; a day-level serialization failure must not disappear merely
because its individual cells fit. Retain stage-specific byte/node/event counts.

Successful cells retain native lineage sufficient for replay plus diagnostic
counts; no calibration, application error, confidence allocation or policy
comparison is executed. Do not try unretrieved fragments, backtrack the greedy
planner, expand transform envelopes or use a different seed after refusal.

Instrumentation can change the whole-package fingerprint, stochastic model ID
and hence effective draws. B is therefore a **new diagnostic realization**, not
a recovered trace of attempt 001. Record both identities and do not force an
old approval/fingerprint onto new code. Synthetic observer-equivalence tests
must hold effective run/seed inputs fixed and compare observed versus unobserved
engine behavior; they do not prove empirical replay across fingerprints.

### Budgets, attempts and durable evidence

These are simultaneous limits, not targets to raise after results:

- Preserve the original per-partition 128 MiB and per-month 2,000,000-row bounds,
  2 MiB model-index bound, and 2 GiB aggregate model-source bound.
- A has a 300-second deadline per original month; B has a 120-second deadline
  per complete day across all twelve cells. The single 10,800-second attempt
  clock includes input/model preparation, A, B and report publication.
- Each artifact is at most 8 MiB, 4,096 collection items, nesting depth 16 and
  131,072 expanded nodes. Native event budget remains 4,096 per day/member.
  Output is sharded by at most eight days; complete root inventory, explicit
  refusals and all referenced components must remain bounded and replayable.
- Exactly one initial attempt; no automatic retries, resume-on-new-code,
  successful-shard selection or substitution. Interruptions, timeouts, I/O
  errors and unexpected exceptions retain a failed terminal and completed
  partial inventory, without pretending the remaining cells were evaluated.

Persist the input/protocol/runtime bindings and started record before work;
record progress before each month/day/cell; publish content-addressed component
files without overwrite; publish the terminal last. A root ledger must account
for every scheduled cell as evaluated, deterministic refusal, not evaluable or
not attempted. Partial artifacts are not a completed study. A later verification
replay needs its own named attempt and approval, does not replace this attempt,
and adds no historical units. Existing trusted POSIX worker containment is not
a hostile-code sandbox or a guarantee against unresponsive kernel/storage.

## C. Decision rules after diagnosis

Do not rank policy performance: A/B deliberately produce none. Publish all
scheduled denominators and the following conclusions, including an honest stop:

| Finding | Permitted next design action | Prohibited shortcut |
| --- | --- | --- |
| Parent support fails | Consider a separately defined population/calendar or stop for insufficient data | Fill gaps, shift failed windows, replace failed dates within this study |
| Parent passes, subset fails | Specify and fixture-test a new timestamp-based degradation operator; measure retained/hidden fraction and changed target | Claim unchanged degradation or recovered raw observations |
| Retrieved scale support fails | Analyze the planner/envelope using recorded inputs and synthetic counterexamples; distinguish a defect from declared incompatibility | Assume all 256 fragments are unusable, widen envelopes or search seeds until success |
| Artifact limit binds | Consider independently verified lossless representation changes, or declare infeasible | Remove lineage, truncate events or lift limits after seeing refusals |
| Operational failure | Diagnose implementation/operations from the retained failure; register a distinct attempt if justified | Count a favorable retry as the original experiment |

A method amendment must explain the scientific estimand it changes, not just
its improved completion count. Freeze at most one successor method for the next
confirmation; further exploratory variants need separately declared protocols
and cannot reuse a confirmation frame already opened. If no defensible successor
or sufficient untouched support exists, stop with #607 open.

## D. Confirmation blueprint, not yet a runnable preregistration

### Freshness, calendar and roles

Before choosing confirmation inputs, build a row-free exposure ledger from
existing manifests, source identities and recorded prior work. Exclude all nine
previously decoded monthly partitions, not merely their 62 windows. Also exclude
all known training, development, validation, diagnostic and outcome-exposed
support, and every current, final or retired protected holdout. Unknown exposure
or protection status is ineligible; an unmentioned month is not proof of freshness.
TRAIN index sources remain fixed model inputs, never new calibration/application
units. Renaming or copying a file cannot reset its exposure.

Apply exact identity, temporal-neighbor, shared-anchor/context/cohesion and
near-duplicate checks to whole evidence units and model ancestry, using frozen
row-free evidence where available. If establishing freshness needs new content
inspection, authorize a separately bounded inventory procedure first, record
that exposure and its permitted use; do not call it metadata-only. Existing
[release-holdout governance](release-holdout-governance.md) supplies exclusion
and lineage rules, not permission to open protected data for this research.

Freeze the successor generator, degradation, window, available-information
stratum and deterministic seed schedule **before any fresh eligibility or
calibration-error inspection**. Define the intended population explicitly as
the scheduled nonprotected historical triangle-days under that known observation
operator, not unknown real-world missingness.

Select a bounded calendar and disjoint calibration/application roles solely
from the audited identity/calendar frame, with a documented deterministic
ordering/tie-break and fixed total counts. Enumerate every date and shard in
the new machine-readable registration before source support is opened. The
same scientific admission rule must apply to both roles; role-specific outcome
screening is forbidden. Run support checks once on that frozen frame. Record
all failures, even if failure prevents a comparison. Do not keep adding months,
dates or replicas until the accepted count reaches a threshold.

The exact eligible frame, dates, scheduled counts, ordering rule and total
resource envelope are intentionally **unbound** at design time: no freshness
audit or A/B result exists yet. They are mandatory execution-blocking fields,
not executor defaults or discretion left to a running experiment. Source
eligibility itself defines a selected population, even without looking at loss.
If fresh-support results cause redesign, retire that frame from confirmation
and record it as exploratory before choosing a new, separately registered one.

### Sample size and claims

Keep floors of **30 admitted calibration units** and **20 common application
units across the six policies**, including supported confidence allocation.
They are lower limits for this research path, not power calculations, proof of
independence, or enough evidence to verify a 90% coverage theorem empirically.
Use all admitted units in the fixed schedule, not the first passing 30 or 20.

Before registration, justify scheduled counts with explicit attrition scenarios
from exploratory source/generator feasibility, and a predeclared precision
target for loss differences and coverage if those claims are intended. Fix any
simulation assumptions and seeds without fresh outcomes. If the available frame
cannot support the intended claim or resource budget, stop or predeclare a
different scientific study; never lower these floors after opening the frame.
The study need not show one policy wins: adverse measurements and null effects
are valid results and must remain visible.

### Freeze the six-policy estimand and analysis

Carry forward the original evidence-unit contract: one complete parent
triangle-day has mass `W_h = 1`, however many synthetic members or rows exist.
Require complete source lineage and four fixed members. Compare observed-only,
equal-unit mass, deterministic one-member selection at epochs 0–3,
mean-of-member-loss through the actual callback, calibrated/capped confidence,
and naive concatenation labelled a failed negative control, never a default.
Filtering cannot redistribute removed mass. Duplication/near-duplication cannot
increase total default mass; reports distinguish raw/core/positive-weight rows,
historical units, member count, exact mass, Kish concentration ESS and member
correlation participation ratio. Undefined diagnostics remain unavailable.

The default point loss remains mean absolute midpoint error in pips across the
three symbols and identical 600 probes. Preserve the original joint
member/symbol calibration score, available scale, rank
`ceil((n + 1) * 9 / 10)`, caps and exact applicability stratum unless the successor
registration explicitly identifies a scientifically justified change. Do not
choose changes using fresh calibration errors or application performance.
Fit on the separate calibration role; seal all application allocations before
application error/coverage evaluation. Unsupported confidence stays unavailable,
not a uniform-weight fallback silently counted as confidence execution.

Nominal 90% conformal coverage is marginal under exchangeability conditional on
the fixed model, not conditional coverage for each day or an admitted subset.
Calendar separation and unit clustering do not prove exchangeability or remove
regime drift. Report pre-applicability coverage and admitted-subset coverage
separately with exact numerators, denominators, radii and refusal inventories.
These limits follow the [conformal prediction reference, sections 1 and 3](https://arxiv.org/html/2107.07511v6).

Retain the seven predeclared paired coordinates against equal-unit mass:
observed-only, four epoch choices, marginalized loss and confidence. The naive
negative control is not an inferential competitor. Report every coordinate and
its actual common-unit set; unavailable coordinates keep null reported values,
with `p=1` used only inside the fixed seven-coordinate Holm calculation.
The baseline remains 10,000 paired whole-day bootstrap draws, seed 60795,
95% linear-quantile intervals and two-sided sign tests with ties omitted.
These are assumption-qualified diagnostics, not new independent evidence from
rows or members. Preserve the entire paired policy vector in each resample.

The confirmation registration must additionally state how serial dependence
limits inference and whether a predeclared block-based sensitivity analysis is
included. If included, bind its calendar block construction, missing-day handling,
minimum block count, resampling method and seed before outcomes; retain all
results rather than selecting the most favorable method. Without adequate
dependence assumptions/support, population-level significance and coverage
claims remain unsupported even if the software emits numbers. A successful
comparison is not certification of useful radius width, coverage or superiority.

## Implementation and authorization gates

Existing v1 readers and approvals hard-code the old policy, dates and operation
scope. Do not mutate their asset, loosen their validation, mint an old receipt
for the new study, or use fixture mode to bypass those checks. Implement additive
diagnostic/successor contracts with new identities and least-privilege operations.
A/B approvals cannot grant calibration, application-outcome inspection or policy
comparison; D requires separate grants and its own complete source bindings.

Before any diagnostic execution, the coordinator must verify all of:

1. Committed diagnostic protocol and complete allowlist, with old input bindings
   resolved byte-for-byte; new artifact schemas and source exposure classification.
2. Exact reviewed implementation and runtime fingerprint, source snapshots,
   protected-input exclusions, execution budgets, new attempt label/directory,
   named coordinator and timestamp. Approval remains a workflow declaration,
   not source authentication or a hostile same-user security boundary.
3. Synthetic fixtures for parent versus subset staleness, absent boundaries,
   inclusive 60-second edge, duplicate timestamps and physical ordinals; multi-
   failure cells and `not_evaluable` statuses; exact 186/48 denominator handling,
   the frozen four-day selection and the explicit 27/31 unexecuted-day inventory.
4. Synthetic numerical references for below/inside/above transform envelopes,
   terminal correction, greedy dead ends and observer equivalence at fixed
   effective seeds; byte/node limits, interrupted attempts, no-clobber output,
   failed worker containment and missing-component refusal.
5. Full source and clean installed supported-Python qualification, repository
   hooks, strict docs and package/asset equality on final implementation bytes.
   Metadata-only design checks are not a substitute for these future code gates.

D additionally requires the frozen successor method, audited fresh-data ledger,
exact dates and roles, sample-size rationale, source and shard identities,
metric/inference definitions, all scientific/resource stop rules and separate
approval. None has been fabricated or declared complete by this design.

## Completion boundaries

The design issue can close when this document is independently reviewed,
documentation checks pass, and it is committed and pushed to `dev`. That does
not implement or execute A/B/D and does not close #607.

Exploratory completion means a durable, fully accounted diagnostic report, even
when infeasible. Confirmation operational completion means a valid final report,
even when support or results fail. #607 completion still requires the actual
source-bound six-policy comparison with supported confidence, mass/diagnostic
invariants and honest unit-level inference, followed by full code qualification
and verified delivery. No TestPyPI/PyPI publication, protected holdout access,
release promotion or execution of the follow-up is part of this design delivery.
