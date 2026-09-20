# Bounded training-weight diagnostic tooling

Issue #751 implements the A/B portion of the
[separate follow-up design](training-weight-followup-study.md). The new protocol
is version **1.0.0**, additive to the original contracts; this is an unreleased
SemVer minor feature. Qualification uses **newly generated synthetic fixtures
only**. No historical diagnostic execution, approval, confirmation study,
calibration, policy loss, publication or new empirical acceptance is supplied.
The failed original attempt remains unchanged, and #607 remains open.

## Public Python surfaces

The modules live under `histdatacom.data_quality`:

| Module | Purpose |
| --- | --- |
| `training_weight_diagnostic_protocol` | Frozen installed protocol, metadata-only exact input plan, distinct operation-scoped approval |
| `training_weight_source_diagnostics` | Independent source/subset gates and probe census, physical-row witnesses, eight-day shards |
| `training_weight_generator_diagnostics` | Twelve member/symbol cells per selected day, native lineage, bounded interval planning traces |
| `training_weight_diagnostic_runner` | Fresh supervised attempt, runtime binding, complete 234-cell ledger, artifact-only inspection |

`run_training_weight_diagnostics(plan, new_directory, approval=..., cancelled=...)`
is the execution entry point. There is deliberately no default empirical plan,
approval factory, source discovery, automatic retry or resume. The new directory
must not exist; input paths are exact, canonical locators, not globs. Symlink
aliases are refused. The gate is a trusted-coordinator workflow boundary, **not
a hostile same-user security sandbox** or an atomic filesystem snapshot.

For development, `tests.fixtures.training_weight_diagnostics.fixture_diagnostic_plan`
creates all nine synthetic monthly files plus a tiny synthetic TRAIN model under
the explicit `training-weight-diagnostic-fixture` namespace. Fixtures reject
known historical hashes and cannot carry an execution approval. These generated
files share schedule labels, not historical observations.

For a future separately authorized empirical execution, the plan binds every
approved source/model file, protocol identity and attempt label. The externally
supplied `TrainingWeightDiagnosticApprovalV1` binds the plan, exact protocol file
and Git blob, coordinator-declared actual commit, runtime fingerprint and allowed
operations. The installed asset itself grants no permission and proves neither
Git ancestry nor source authenticity. The optional read-only Git verifier checks
the declared commit's exact protocol blob, not branch ancestry or authorization.
Original campaign approvals are not accepted. Verification replay needs its own
label, directory and separately recorded approval; it adds no independent history.

## What the results mean

A accounts for all **186** day/symbol cells. Parent and thinned-subset probe
support are diagnosed separately; missing prerequisites yield `not_evaluable`.
Independent gate counts and earliest physical-row witnesses do not disclose
hidden bid/ask quotes or reconstruction errors. The unchanged original admission
decision is reconciled separately. A real diagnostic must reproduce **17/8/6**
admitted days before B can start. Fixture counts are reported honestly and are
not forced to match those historical counts.

B covers exactly four frozen dates, four seeds and three symbols: **48** cells.
The first deterministic refusal ends that cell's intervals, not the other cells.
Each trace binds the canonical query, ordered retrieval, exact target times,
segment seed and selected fragment prefix. Planner-grid references and a greedy
dead-end counterexample diagnose behavior; they do not alter the selection
algorithm or establish exhaustive infeasibility. Opt-in instrumentation is
tested against unobserved native generation at identical inputs and seeds.
The added source files change the package fingerprint and resulting stochastic
identity, so a future diagnostic is not an exact replay of attempt 001.

Source-refused selected fixture days have explicit unavailable generator cells.
All 62 days retain `selected`, `source_refused`, or
`not_selected_by_diagnostic_design` labels. Deterministic refusals are valid
diagnostic results, not successful candidates or empirical acceptance.

## Bounds, persistence and replay limits

POSIX process supervision enforces 300 seconds per source month, 120 seconds per
generator day and 10,800 seconds for the complete attempt, including preparation
and final inspection. Cancellation or operational failure ends the attempt.
There is one initial attempt, with no seed search or implicit retry. Individual
JSON artifacts are limited to 8 MiB, 4,096 container items, depth 16 and 131,072
expanded nodes; the generator additionally accounts for embedded native payloads
and complete-day native event capacity. Capacity exhaustion is an explicitly
incomplete diagnostic, never a truncated success.

The runner retains canonical `plan.json`, `runtime.json`, `started.json`,
per-cell progress, source shards, bridge/model/day components and `terminal.json`.
The runtime identity is bound into both started and terminal records, including
failures before the first source cell. Components publish atomically without
overwriting existing content. A failed terminal retains finalized evidence and
distinguishes `not_attempted` from `attempted_result_unavailable`; an abandoned
temporary file is not evidence. Persistence itself can still fail if storage is
unavailable. No software can promise a durable terminal after physical storage
failure or abrupt host loss.

`inspect_training_weight_diagnostic_artifacts(directory, approval=...)` performs
bounded **artifact-only structural replay**. It checks canonical bytes, hashes,
closed schemas, runtime/input bindings, lineage and complete cell reconciliation.
It never follows the retained source/model locators or regenerates candidates.
It therefore proves neither fresh source replay nor authenticity, empirical
qualification or approval. A distinct operation grant is required for inspecting
nonfixture artifacts. The source/model data may be absent during fixture artifact
inspection; deleting synthetic inputs is explicitly tested.

## Synthetic qualification

The dedicated suites are `test_training_weight_diagnostic_protocol.py`,
`test_training_weight_source_diagnostics.py`,
`test_training_weight_generator_diagnostics.py` and
`test_training_weight_diagnostic_runner.py`. They cover exact approval scope,
raw/retained probe oracles, boundary/tie/order cases, native observer equivalence,
planner reference arithmetic, refusal/capacity paths, complete synthetic attempts,
cancellation, missing inputs, tampered artifacts and interrupted publication.
They exercise the installed package and supported Python floor as well as source
imports. Passing them establishes software contract behavior only.
