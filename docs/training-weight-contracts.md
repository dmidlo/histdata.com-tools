# Synthetic-member evidence mass and weighting v1

This #607 research workflow compares six fixed weighting policies without
turning alternative synthetic paths into additional historical observations.
It extends the [origin-preserving training contracts](training-row-contracts.md)
with source-bound degradation, research-native candidates, calibration and
historical-unit evaluation. It does not qualify a training corpus, recover lost
ticks, assign train/test splits, certify causal availability or authorize a
release.

The protocol and mathematical interpretation below are frozen. Synthetic
contract fixtures qualify implementation behavior, not empirical support.
The first separately approved real-data attempt admitted no candidate days and
failed during export; its retained observations and limitations are reported
below. This page does not approve another execution or qualify the method.

## Frozen protocol and evidence ownership

The authority is
[the preregistration committed in 4e1eadf](https://github.com/dmidlo/histdata.com-tools/blob/4e1eadf6862a192906d2c8f69efd356578252044/src/histdatacom/data_quality/assets/training_weight_preregistration_v1.json).
Its file SHA-256 is
`025178395cc8da1224132e8a480428e7ea7324e56c2859a2cf9a9f1d5d703b55`.
Configuration, schedule, thresholds, method identities and seeds are not tuned
after inspecting outcomes.

The fixed model is the complete, immutable TRAIN-only nearest-neighbor index:
256 retained fragments from 20 training days, with its exact ranking, backoff,
metric scales, transform policies and source inventory. Its declared source
months are January 2019, 2020, 2021, 2022 and 2023. Calibration schedules all 42
weekdays in January 2010 and January 2011; application schedules all 20 weekdays
in February 2011. These are scheduled counts, not claims of usable observations.
Exact source hashes, sizes and row counts must match the preregistration.
Protected holdouts, other periods, external market context and adaptive index
updates are outside this workflow.

Each day covers the complete EURGBP/EURUSD/GBPUSD graph. The core is the
half-open interval 08:00:00–08:10:00 UTC, with boundary-search support from
07:59:00 through 08:11:00. The chosen anchors are the last quote at or before
the core start and the first quote at or after its end. Each symbol requires
at least 64 core parent rows and at most 4,096 support rows. Quotes are ordered
by timestamp and original physical row ordinal; duplicate timestamps do not
erase distinct source rows.

Known degradation retains both boundary anchors and interior ordinals
`0, 4, 8, ...` relative to the interior sequence. It creates a fresh observed
subset dataset, not an alias for the unabridged parent. Every subset quote binds
its original one-based physical ordinal, timestamp, exact bid/ask and source
bytes. The subset's placeholder `vol` is not traded volume or model evidence.
Hidden parent quotes are truth-only, not query features or weighting inputs.

Historical evidence belongs to the verified original parent's complete
three-symbol UTC-day dependency unit, before selecting a member or output
window. All same-support runs, members, windows and shards map back to that unit.
They do not increase the historical sample count. Cross-day anchors or external
context cannot be silently removed to make units appear independent. The
immutable fixed model is explicitly conditioned-on state, not a general
exemption from dependency closure.

## The six policies and row mass

Let `W_h = 1` be the mass of historical unit `h`, and let `M_h` be its complete
admitted member count. The normal invariant is `sum_m w_hm = W_h`; filtering
may reduce this sum but must never increase it. Exact rational arithmetic
preserves mass; floating-point summaries are bounded diagnostic views.

| Policy identity | Allocation and executable meaning |
| --- | --- |
| `observed-only.v1` | Allocate `W_h` over the retained degraded observed rows. Full-parent truth is not the predictor. |
| `equal-unit-mass.v1` | Allocate `W_h / M_h` to each member. |
| `one-member-per-epoch.v1` | Allocate all `W_h` to one canonically sorted member chosen by the frozen hash of policy, unit and training epoch. Report epochs 0–3 separately; a seed is not a quality ranking. |
| `member-marginalized-loss.v1` | Execute each member's loss callback and use `W_h * mean(member_loss)`. This is not the loss of an averaged prediction. |
| `calibrated-capped-radius.v1` | Use supported calibrated radii only. Set `a_m = 1 / (1 + max_symbol(radius_m / pip))`, `b_m = max(a_m / max(a), 1/4)`, then `w_hm = W_h * b_m / sum(b)`. The largest member mass is at most four times the smallest. |
| `naive-concatenation-negative-control.v1` | Deliberately allocate `W_h` to every member, totaling `M_h * W_h`. It is a failed mass negative control for multiple members, never an eligible default or inferential winner. |

A row's mass is its member mass divided by that member's exact complete row
count. Dropping rows or members preserves the original weights; it does not
renormalize the remainder to regain `W_h`. More generated rows therefore cannot
manufacture historical evidence. Protected targets are never upweighted.

## Two different dependence diagnostics

Kish weight ESS is `(sum_i w_i)^2 / sum_i(w_i^2)`, unavailable for zero total
mass. It measures weight concentration, not temporal independence, correlation
adjustment or the number of historical days. The sampling reference is
[Kish, *Survey Sampling*](https://www.wiley-vch.de/en/areas-interest/mathematics-statistics/survey-sampling-978-0-471-10949-5).

Campaign row ESS is recomputed from all retained row masses together, not by
summing per-day ESS values. Reports distinguish complete native support rows,
core rows, positive-weight core rows, member/unit combinations and original
historical units. Zero-weight members in one-member sampling remain explicit;
their rows do not become positive-weight evidence.

The separate member diagnostic uses fixed-grid pip-normalized midpoint
increments, concatenated across the three symbols within one unit. For the
member Pearson correlation matrix `C`, participation-ratio dimension is
`trace(C)^2 / trace(C @ C)`. Here `@` is matrix multiplication and the denominator
equals `sum_ij C_ij^2`; it is not an elementwise-product trace. A constant member
vector makes this diagnostic unavailable, not uncorrelated. Participation ratio
summarizes spectral concentration; it is not a replacement sample size for
inference. See the definition discussed by
[Giaffar et al., section 2.1](https://proceedings.mlr.press/v238/giaffar24a/giaffar24a.pdf).

Report raw rows, members, admitted historical units, exact mass, Kish ESS and
member correlation dimension separately. None certifies the others.

## Research-native generation, not legacy certification

Four members use base seeds 60701–60704 and the frozen empirical motif
generator 1.2.0. Query conditions use only the day's degraded quotes and fixed
epoch/session declarations. Historical `technology_epoch_03` remains the query
label; modern `technology_epoch_04` reference support may be reached through
the retained backoff trace, not by relabeling history.

Conditioning uses the whole retained window and is explicitly **ex post**.
An earlier native bookkeeping clock does not make the full-window query
historically available. The exact index, query results, selected fragments,
transformations, seeds, anchors and native events remain replayable. Compact
references must resolve to retained content and reproduce the original values;
compaction must not alter event values or select different successful members.

The day artifact retains each exact native query-result identity, condition and
clock once. Repeated ranked matches and backoff traces share content-addressed
`query_supports` records; each match references the complete fragment retained
in the immutable model index. Interval records reference their query-result
identity rather than copying the support for every member. Native batch
payloads, transformations, full-width seeds and events are unchanged. Fresh
generation verifies the complete reconstructed lineage; these dictionaries
are lossless references, not truncated match lists or a new ranking policy.

Native events and transformations use transparent field tables: constant
fields are stored once, varying fields have an exact ordered column list, and
each row retains its original scalar values. Transformation seeds use tagged
canonical unsigned-64-bit decimal text. Decoding restores their original
integer type; it does not route them through floating point. The public
`stream` and `stream_json` views reconstruct the original native fields, bytes
and identities, including signed floating-point zero. Unknown fields,
overlapping constants/columns, malformed row widths, noncanonical table
encodings and invalid seeds refuse. This is storage representation only, not
event thinning, opaque compression or changed provenance.

These are research-native candidate artifacts, not fabricated legacy
reconstruction product manifests. Native event schemas do not establish hard
carving, broker conditioning, cross-currency/no-arbitrage certification or
production delivery eligibility. A finite positive noncrossed path is still a
modern-reference counterfactual, not recovered historical ticks or a posterior
sample from historical truth. One failed member makes the whole day unavailable
to the complete-member comparison.

## Fixed-model error radii and their limits

Evaluation uses 600 one-second probes at offsets 0–599. Each probe takes the
last quote at or before it, resolving ties by source order, with age at most
60 seconds inclusive. A missing or stale probe refuses the whole day; there is
no shifting, filling or replacement date.

For each member and symbol, the available scale is the maximum of one pip
(`0.0001`), mean candidate spread over the probes, and maximum deviation of
candidate midpoint from its own retained-anchor piecewise-linear midpoint.
This scale uses no application truth. A calibration day's joint score is the
maximum, over all members and symbols, of maximum probe midpoint error divided
by that available scale.

At least 30 fully replayed calibration units are required. With `n` units, use
the one-based ordered score at rank `ceil((n + 1) * 9 / 10)`, with no interpolated
or clipped rank. Each radius is that quantile times its available scale. A zero
quantile is possible; it does not establish path truth. Nonfinite results
refuse before allocation.

The nominal 90% interpretation is marginal over calibration and application
days under exchangeability conditional on the complete fixed model. It is not
conditional on a particular realized calibration sample, a particular day or
the subsequently admitted subset. This marginal/conditional distinction follows
the [conformal prediction reference, sections 1.1 and 3](https://arxiv.org/html/2107.07511v6).
Exchangeability is an assumption, not a property established by a successful
round trip or by having 30 units.

The confidence policy additionally abstains unless each member's per-symbol
scale lies within the calibration symbol's inclusive scale range and the exact
graph, epoch, session, horizon, generator, model, degradation and adapter stratum
matches. This filter does not inherit nominal coverage conditional on admission.
Unsupported confidence yields no confidence-policy allocation; other policy
results remain explicit. Where diagnostic radii exist, report joint coverage
before the scale-domain filter and admitted-subset coverage separately, including
unit IDs, numerators, denominators and refusal counts. Missing truth or an
undefined radius is unavailable, never counted as covered.

There is no fitted-model uncertainty guarantee, unseen-regime guarantee or
confidence claim for unknown missingness. Modern-to-historical transport,
serial dependence and regime drift may invalidate useful empirical coverage.

## Allocation before outcomes; paired historical-unit inference

The prescribed order is source-bound candidate replay, calibration on its
separate scheduled support, then persistence of all application allocations
before any application truth error or policy comparison. Application truth
does not choose members, weights, domains, thresholds or retained dates. Reading
source bytes to verify a degradation is not permission to inspect application
outcomes or feed hidden quotes into the model.

The fixed point loss is mean absolute midpoint error in pips over the identical
symbol/probe grid. Observed-only uses bounded-prior degraded quotes. Member
marginalization evaluates the actual mean of member losses; it must not be
replaced with loss of a mean path.

All comparisons are paired by the same complete historical triangle-day. The
protocol requires 20 common application units for inferential output. It uses
10,000 fixed-seed percentile bootstrap replicates, resampling complete days with
all policy results together, and a 95% interval with linear quantiles. The exact
two-sided paired sign test omits zero differences and reports its remaining
count. Its null requires independent unit signs with equal positive/negative
probability; ownership accounting alone does not prove that assumption.

Holm adjustment retains the exact seven-coordinate family: observed-only,
four one-member epochs, marginalized loss and calibrated radius, each minus
equal-unit mass. Equal-versus-itself and naive concatenation are excluded.
Unavailable coordinates retain a null reported p-value and participate as
`p = 1` only inside Holm arithmetic; the family is never shrunk after outcomes.

These are conditional descriptive diagnostics, not a live-profitability or
independent-day theorem. Cluster-aware inference motivates the historical-unit
choice; the registered percentile procedure is not the wild-bootstrap-t method
studied by [Cameron, Gelbach and Miller](https://faculty.econ.ucdavis.edu/faculty/cameron/research/papers.html).
Report failures and low support without selecting a winner or retuning.

## Complete artifacts and bounded replay

Candidate shards follow the frozen calendar, at most eight days and 32
triangle-member products. A day contains all twelve member/symbol components.
Fitting requires exactly six shards: `201001-1` through `201001-3` and
`201101-1` through `201101-3`. Application requires exactly `201102-1`,
`201102-2` and `201102-3`. Omitting a whole shard is not equivalent to retaining
explicit refusals for its scheduled days, even when the remaining fit count
would exceed the minimum.

The manifest retains the complete monthly source bridge and source-refusal
inventory as well as every scheduled shard admission/refusal. Neither a caller
ID nor a list of only successful days establishes completeness. Different
shards must reconcile to the same monthly source/ownership inventory and have
disjoint original-parent support; splitting a file does not create independence.

Candidate-shard persistence writes separately bounded, content-named model,
source, ownership, bridge and day components, publishing the root manifest last.
Writers reexecute the complete shard before publication. Readers check bounded
regular-file reads, exact bytes, hashes, canonical structure and full fresh
source/model/generator replay; there is no `verify=False` qualification path.
Changed source bytes, omitted refusals or re-sealed false lineage must fail.
An interrupted write may leave unreferenced components, not a completed root.

The frozen research limits include 8 MiB per artifact, 4,096 native events per
complete day/member, 4,096 collection items, depth 16 and 131,072 expanded nodes.
JSON embedded as text is parsed and charged to the node budget, including
retained table keys, scalar cells and shared query catalogs. Referenced content
is retained once rather than charged as duplicated copies; reconstruction has
separate closed native-field, row-count and native-byte checks. These limits
are simultaneous: meeting the event count does not guarantee the full artifact
fits. Denser inputs can still produce a retained deterministic resource refusal.
Protocol bounds are not raised after seeing which inputs fail.

Explicit, reproducible support/generation/output-budget refusals may be retained
as scheduled scientific exclusions. A wall-clock
timeout, interrupted process, transient I/O failure or unexpected implementation
exception is an operational failure of the attempt, not a deterministic
candidate refusal. It must not be silently retried until a favorable shard is
obtained or counted as a completed scientific campaign. The preregistered
120-second day and 10,800-second campaign budgets are enforced subprocess
deadlines, not a hard real-time completion guarantee on arbitrary hosts.

## Running one complete, explicitly identified attempt

`TrainingWeightCampaignPlanV1` and `run_training_weight_campaign` live in
`histdatacom.data_quality.training_weight_campaign`. A plan binds the exact
three typed monthly source plans, fixed model, evidence kind and attempt label.
The following illustrates the invocation after those inputs and an external
approval have been separately prepared and reviewed; it does not construct an
approval, discover real inputs or authorize their use:

```python
from histdatacom.data_quality.training_weight_campaign import (
    TrainingWeightCampaignPlanV1,
    run_training_weight_campaign,
)

plan = TrainingWeightCampaignPlanV1(
    sources=(calibration_201001, calibration_201101, application_201102),
    model=fixed_model,
    attempt_label="reviewed-attempt-001",
)
attempt = run_training_weight_campaign(
    plan,
    new_attempt_directory,
    approval=external_approval,
    cancelled=operator_cancelled,
)
```

Here `operator_cancelled` is an optional zero-argument callback returning a
Boolean. The directory must be new. Empirical execution requires all six
operation grants listed below before any output is created. No implicit resume,
automatic retry or selection of the fastest successful attempt exists. An
explicit `purpose="qualification_replay"` additionally requires
`prior_attempt_id`; such a replay is separately identified and does not add
historical evidence units or replace a failed preregistered attempt.

The complete runner is **POSIX-only** and refuses other platforms before
approval checks or filesystem side effects. Pure contracts, weighting math and
stage APIs do not depend on POSIX process groups. Candidate computation uses a
separate process with a per-day deadline; the POSIX campaign supervisor adds a
single whole-attempt deadline, process-group cleanup on cancellation, timeout
or abnormal exit, and direct-worker reaping. This supervises trusted numerical
workers, not hostile code that escapes its process group. Termination and
durability still rely on a responsive operating system and functioning storage;
neither an uninterruptible kernel operation nor coordinator/host destruction is
turned into a guaranteed on-time terminal record.

The runner retains the plan, exact supplied approval copy, started record,
source subsets, all nine candidate-shard roots, calibration, persisted
allocation, training-export summaries and final evaluation. Stage checkpoints
and an active-day checkpoint retain the last known stage/shard/day on failure.
Training-export summaries bind generated batch identities, counts and hashes;
they are not a persisted campaign-sized row table. The streaming row API
replays its source boundary once per export and divides oversized valid
requests into bounded deterministic row-key slices without renormalizing mass.
Opening a new export repeats verification; a single-batch request can refuse
when streaming is required.

The terminal record is published last without overwriting an existing attempt.
A claimed successful worker must also leave the canonical evaluation and
completed-stage linkage matching the plan's model, evidence kind and exact
source-plan scope. The parent performs this bounded consistency check rather
than repeating the complete empirical workflow. Thus
`execution_completed_not_qualification` describes operational completion, not
independent source authentication, an unforgeable execution certificate or
scientific admission. Failed or interrupted attempts may leave diagnostic
components but cannot supply a completed result.

## Real execution remains explicitly gated

There is no default approval. A coordinator records a separate external
`TrainingWeightExecutionApprovalV1` only after reviewing the final integrated
implementation. The receipt binds the exact policy commit/file digest, current
whole-package fingerprint, sorted allowed-operation subset, coordinator label
and canonical UTC time. The exact controlled operation names are:

- `source-window-decoding`
- `degraded-subset-materialization`
- `candidate-generation`
- `calibration-fit`
- `application-outcome-inspection`
- `policy-comparison`

The fingerprint includes relative-path and byte digests of shipped Python,
JSON and base64 computational assets, plus the Python implementation/version,
platform and declared numeric/IPC dependency versions. Reads are bounded and
reject symlinks and nonregular package inputs; derived caches and the external
approval receipt are not scientific code inputs. A stale fingerprint, changed
runtime, unknown operation or missing approval refuses. The preregistration's
`execution_approved` property remains false; an external workflow declaration
does not rewrite the policy.

The runtime-bearing fingerprint also participates in stochastic model identity.
Relocating identical code/model/source bytes need not change draws merely
because read locators moved; changing Python or the declared runtime is a
different method fingerprint. There is no promise of identical empirical paths
or replay across runtimes. Installed Python 3.10 synthetic API qualification is
distinct from empirical execution under the exact separately approved runtime.

Run from a fresh process on final reviewed package bytes. The fingerprint is an
on-disk code/asset and runtime checkpoint, not proof of already-loaded bytecode
under monkeypatching or hot edits. It is not a signed execution certificate,
source authentication, atomic filesystem snapshot or hostile same-user security
boundary. Fixture evidence remains explicitly nonempirical; renaming known real
inputs as fixtures is not an approval mechanism.

Repository-wide and installed-package qualification check implementation
compatibility; they do not supply empirical support or change the frozen
protocol's execution gate.

## First preregistered attempt: no supported empirical comparison

On 2026-09-20, attempt `issue607-preregistered-comparison-001` ran once under
the committed protocol, after full source/installed-code qualification and
separate raw-source admission. All nine monthly partitions passed the latter
checks: 2,989,245 rows, 83,714,359 bytes, exact schema and hashes, original
physical order, and no crossed quotes or timestamp regressions. Month-boundary
spill remained raw. These checks establish adapter structure/integrity only,
not source authenticity, scientific window support or production suitability.

All six calibration shards and three application shards were retained, with
every scheduled date accounted for exactly once:

| Candidate refusal | Calibration days | Application days |
| --- | ---: | ---: |
| Engine unsupported transform | 24 | 6 |
| Expanded artifact node budget | 1 | 0 |
| Missing boundary anchor within fixed support | 3 | 1 |
| Missing or stale fixed probe | 14 | 12 |
| Fewer than 64 core parent rows | 0 | 1 |
| Total refused | 42 | 20 |
| Accepted candidate days | 0 | 0 |

The source-replayed fit persisted `calibration=null`; the persisted allocation
contains no admitted days. Thus the frozen minimum of 30 calibration units was
not met. Confidence, coverage, useful widths, empirical policy comparisons,
correlation dimension and inferential significance are unavailable—not zero
error, successful coverage, or proof that one weighting policy is superior.

The original runner then called its nonempty row-export API with no requests.
Its retained terminal is `failed`, reason `worker_failed`, last stage
`training_export`, with no evaluation ID. Campaign elapsed time was
1,577.819 seconds; fixed-model preparation took a separate 2.581 seconds.
No final evaluation or training-export report was produced, and the attempt is
not reclassified as completed merely because its refusal inventory is complete.

The subsequent implementation fix skips that invalid call for an empty export,
publishes an empty batch list and continues the ordinary unavailable-result
evaluation. A separate all-refused synthetic IPC regression exercises all
42/20 dates, null calibration, zero exports, unavailable inference and a
completed-but-not-qualified terminal. The public iterator still rejects an
empty request list before reading sources. This fix is not a real-data rerun
or new empirical support.

Static inspection narrows the engine refusals to cadence/time-scale
compatibility among the frozen greedy planner's retrieved fragments at its
current cursor. It does not prove the entire reference library is unusable.
The saved refusal records omit the failing member, symbol, interval, cursor and
numeric scale comparisons; their direction and magnitude remain undiagnosed.
No thresholds, windows, model, seeds or budgets were retuned after these results.

The original attempt's reproducibility references are:

- Source/runtime fingerprint:
  `0a25bf83a2440af1671ffc199189e6535c26867fe139fab315f2576f9b4a5de5`.
- Prepared metadata SHA-256:
  `6618e12773fc6fbe4343f5b63a8dfe51ccc77ac3520d49396f62695a6328074c`.
- Attempt ID suffix:
  `d367516ceb5c10b1ee3ef76ce59542fc759aa61fd5061de8834e808f72165464`.
- Terminal file SHA-256:
  `f4d3e51cc348b01c9130ca7d0e2f28bbd3051448387c7a64b2acaad2479ed1c3`.

Any shipped-code change, including the empty-export fix, changes the approval
fingerprint and stochastic model identity. Another empirical execution would
therefore be a separately identified and authorized attempt, not continuation
or successful replay of this one. The original evidence remains retained.
Issue #607 remains open pending supported empirical qualification.
