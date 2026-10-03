# Bounded origin-aware wide feature views

Issue #652 targets unreleased v3.0.0. These additive v1 contracts compose the
existing #606 event spine and #610 executable join API. They do not change those
wire formats, readers, resource limits, source verification or provider rights.
Synthetic fixtures establish implementation behavior, not completion of the
#605/#612/#681 corpus, historical availability, empirical independence, model
performance, official-source authenticity or real provider certification.

## Build and consume

Import the contracts from `histdatacom.data_quality.training_wide_contracts` and
the executable functions from `training_wide_views`:

```python
grain = TrainingWideGrainV1(WideRowGrain.EVENT)
plan = build_training_wide_plan(tuple(native_join_plans), grain=grain)
view = materialize_training_wide_view(plan)
records = project_training_wide_view(view, columns=("market.tick.EURUSD.bid",))
reports = diagnose_training_wide_view(view)
```

Each input is a complete `TrainingJoinPlanV1`. Partition **columns**, never slice
`TrainingBatchV1.rows`. For each native spine, every tile has the same entire
source inventory; tiles cover each logical column exactly once. Replayed source
definitions—not caller-supplied units—produce the sorted column registry. Every
public value consumer replays sources and checks current material-use/derivation
rights, including declared sources not selected by the projection.

The registry records exact native join columns (namespace, dependency parents,
entity, direction, age, field and coordinate), source family, scalar type, units,
timeframe, lag/warmup **step unit**, availability rule, information mode and all
native null states. Macro transforms retain their actual native specification;
standardized features are in standard deviations, not original raw units.
Unknown units stay null. CFTC percentages, concentration, trader counts and
futures-contract counts are distinct; none claims spot volume. All seven bar
widths (1m, 5m, 15m, 30m, 1h, 4h, 1d), admitted indicators, triangle state and
projection diagnostics, activity, vintage, forecast, calendar, positioning and
broker-style adapters keep their existing native evidence and refusal rules.
Reserved families without an emitter remain unavailable, never fabricated.

## Row grains

- `event_clock` keeps each actual native event once, including tied events in
  native physical order. No Cartesian multiplication occurs as columns grow.
- `member_scenario_panel` retains real product/member/scenario coordinates and
  shared evidence-unit identity. It does not average members or make repeated
  source events statistically independent.
- `fixed_decision_grid` requires one full source-replayed spine for every aligned
  cutoff. Its native request covers the complete declared bounded lookback.
- `anchor_bar_clock` uses the same explicit cutoff geometry at a standard UTC
  bar width. It does not manufacture a bar or market event.

At each grid/bar coordinate, the latest actual event per symbol is selected;
ties preserve native order. The true event time, availability and decision time
stay in the native row. A vacant coordinate has no native row and all values are
explicit `missing_spine_anchor`. Zero maximum anchor age means exact-at-cutoff.
Native join direction/age still applies: selecting a prior event does not turn
an EXACT feature into a forward-filled value. Incomplete-dependency or causal
availability refusals are unchanged. A source that cannot provide a legitimate
full native spine at a requested cutoff is refused, not sliced or backdated.

## Durable layout and real projection

`training_wide_artifacts.write_training_wide_artifact(view, directory)` publishes
ordinary canonical native join children before a value-free layout. One
zero-column control child per full spine retains the actual spine and all source
plans. Each value group is an unchanged native join batch. Every provider-derived
child in this new container requires its native policy receipt, including
children whose standalone legacy reader permits historical receipt-free input.
Native provenance companions remain mandatory. Fresh retention checks protect
each publication boundary. Atomic no-clobber publication never repairs missing receipts or
overwrites contradictory evidence; a failed layout write may leave valid native
children, not a completed wide artifact.

The layout contains only column/grain declarations, opaque row references,
control/group identities and hashes/counts. It does **not** embed full join plans,
spine values, source evidence or numeric diagnostics. It is not an alternative
policy receipt and cannot authorize access by itself.

`read_training_wide_artifact(path, information_mode=..., columns=...)` always
reads every native control and replays all declared source plans/current rights.
It reconstructs the exact schema, logical values and hashes. It opens only
persisted value groups intersecting selected columns; no projection opens all
groups, while `columns=()` opens none. Missing/corrupt unselected value files may
therefore coexist with a valid narrow projection, but source corruption or
revoked rights in any declared source still refuses. A narrow projection must
not be presented as verification of unopened files or their receipts. Returned
`verified_control_files` and `verified_value_files` make this scope explicit.
Source replay cost is **not** eliminated by narrower persisted-value I/O.

`schema_sha256` hashes the ordered column registry. `content_sha256` hashes the
schema, grain, exact native control identities and length-framed canonical rows
with every value/null/provenance cell. Changing tile partition does not change
logical content; changing origin, clocks, values, null state or source identity
does. The layout ID additionally binds its physical group inventory.

The returned projection's `storage_report()` measures actual canonical layout
and opened native-child bytes, projected/logical width and bytes per output row
(unavailable for no rows). It explicitly excludes provider sidecars, source replay
I/O, peak memory, wall time and hypothetical compression ratios. Full reads
measure all wide child bytes; narrow reads measure only the stated scope.

For per-group sizing, `result.manifest.groups` exposes each group's `columns`
(`len(group.columns)` gives its width), `spine_id` partition and
`child.byte_count`. An unopened group's byte count is a layout declaration, not
verification of that stored file. The storage report includes always-read
control-child bytes as well as opened value-group bytes; provider sidecars and
the separate source-plan replay I/O are excluded.

## Resource and diagnostic limits

Existing native limits remain 128 columns, 256 rows and 4096 cells **per join**.
The new composition admits at most 4096 logical columns, 16 complete spines,
128 value groups, 512 selected rows and 32768 selected cells, with at most 32 MiB
of canonical native-child bytes. The inherited canonical 8 MiB planning/layout
envelope and traversal bounds still apply. These limits intersect: they do not
promise every 4096-column combination will fit. No native row is split to evade
a bound, and no source byte/read limit is raised. Wider low-row synthetic alias
fixtures exercise storage width without claiming independent information.

Descriptive diagnostics run after fresh replay and DERIVE admission. Each block
has at most 32 columns, 256 rows and 4096 cells, with its exact column/row scope
reported. Every Pearson pair retains its own support and unavailable/constant
status. Participation-ratio effective dimension uses **one common complete-case
numeric matrix**; it never treats pairwise-missing correlations as a PSD matrix.
Boolean/categorical columns are excluded explicitly. Rank is within-block only,
not algebraic rank, independent evidence count or an additive global dimension.
The rational-bit cap is an input-representation budget, not a measured time or
memory bound. Finite scalar/dimension bounds separately limit arithmetic work.
No correlation calculation chooses retained features, performs ablation or
accesses a final holdout. Reports are process-local values, not durable layout
metadata or a new rights certificate.
