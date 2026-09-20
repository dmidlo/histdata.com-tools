# Overlapping research windows

`histdatacom.data_quality.training_overlap_*` adds SemVer **1.0.0** research
contracts for #656. It does not change the #606 ownership map, #608 temporal
contracts, native product identities, or #607's empirical status. Overlap is
analytical duplication, not newly observed history, confidence, or causal
availability.

## Executable public path

Construct `TrainingOverlapPlanV1` from a `TrainingSourceV1`, its **complete**
source-replayed `TrainingOwnershipV1`, a complete chronological
`TrainingTemporalSplitV1`, and `TrainingOverlapGeometryV1`. Then use:

```python
from histdatacom.data_quality.training_overlap_views import (
    materialize_training_overlap,
    training_overlap_records,
)
from histdatacom.data_quality.training_overlap_artifacts import (
    write_training_overlap_artifact,
    read_training_overlap_artifact,
)

batch = materialize_training_overlap(plan)
rows = training_overlap_records(batch)  # fresh executable source replay
path = write_training_overlap_artifact(batch, output_directory)
restored = read_training_overlap_artifact(path, allow_expost=True)
assert restored == batch
```

These APIs verify actual catalog/IPC, committed native products, immutable
observed anchors, original ownership, and all declared adjuncts. Arbitrary IDs,
constructed envelopes, and correctly re-sealed false results are not verification
tokens. Pure arithmetic helpers in `training_overlap_math` qualify no source.

## Geometry and source identity

The first grid start is **exactly** `start_ns`; no epoch snapping occurs.
Width and stride are positive integer nanoseconds with `stride <= width`.
Only full half-open `[start, end)` children are scheduled. There is no extra
right-anchored tail child. Thus `T >= W` gives
`1 + floor((T - W) / S)` windows, with maximum simultaneous membership bounded
by `ceil(W / S)` (and by the actual number of children). A 3600-second parent,
900-second width and 300-second stride produces ten windows and multiplicity
three. A shorter parent produces zero windows, not a fabricated partial child.

Every verified source coordinate remains in the complete ledger, including
coordinates before/after the geometry and uncovered tail coordinates with empty
memberships. Intersections are the coordinates containing both window indices;
the union is the coordinates with any membership. No symbol's asynchronous clock
is snapped to another leg. Physical duplicate timestamp/quote rows remain
distinct original coordinates. A native observed anchor aliases its exact
original dataset/series/period/physical-row coordinate, retaining its separate
native event/product receipt. Generated coordinates retain the exact committed
product and event identity. No equality-of-price/time deduplication is used.

`coordinate_id` excludes analytical membership and ownership-overlay IDs and is
stable across stride changes and verified same-source ownership coarsening.
The coordinate envelope still retains its exact currently verified owner.
Window identity binds complete ownership, full geometry/tail policy,
and child ordinal/bounds. Worker-result ordering cannot change these identities
or canonical output.

## Original-unit mass and split order

The frozen policy is
`complete-inventory-purge-then-components-unit-mass.1.0.0`:

1. Replay the entire source and original ownership/split inventory.
2. Derive every scheduled window, occurrence, actual conditioning/target span,
   and strictly prior carry state, before any output selection.
3. Exclude windows crossing protected core, conditioning or label ownership;
   derive purge/embargo from the maximum **full, unfiltered** dependency span.
   For a boundary `b` and span `d`, starts in `(b-d,b)` are purged and starts
   in `[b,b+d)` embargoed.
4. Form transitive components only among eligible windows sharing geometrical
   overlap or actual dependency units. Every component is partition-homogeneous.
   Complete excluded windows and their mass remain retained.
5. Independently allocate at most one unit of default mass for **each original
   #606 evidence unit**, not a summed composite-parent budget. Order is original
   unit → every scheduled touching window → all native members (plus explicit
   observed baseline) → each member's actual run/config scenarios → all complete
   rows owned by that original unit across **all graph symbols**.

Exact rational numerators/denominators are retained. Unequal scenario counts use
hierarchical normalization, not flat branch counts. Zero-row branches reserve
their share and emit zero. Excluded windows and projected-out rows never donate
mass to survivors. Filtering (`TrainingOverlapSelectionV1`) cannot alter the
source ledger, groups, allocation denominators, or exclusion decisions. Empty
selection still performs complete verification. `overlap_unit_mass` computes
actual emitted mass per original unit; it is not an effective sample size.

`parent_unit_ids` retain core ownership; `dependency_unit_ids` additionally bind
actual native conditioning and label/feature dependencies. They never rewrite
the old ownership map. Shared future target support connects otherwise disjoint
eligible cores. Strictly prior unscored warmup is a separate support plane, not
additional label ownership or independent data. Nontrivial chronological splits
remain possible because excluded boundary windows do not transitively merge the
whole corpus.

## State, labels and native analytics

The implemented carry initializer executes actual retained quote-prefix state:
last midpoint and arithmetic event mean over
`[child_start - lookback, child_start)`. Physical/native sequence evidence orders
ties; ambiguous same-variant positions refuse. Events at child start belong only
to the core. A previous overlapping child's terminal state is never reused.
Every generated prior event must have its **full conditioning end** no later
than child start. Otherwise the carry is explicitly unavailable and the window
excluded. `RESET` rejects cross-partition support; `WARMUP_ONLY` permits only
strictly prior observed, unscored support, not generated/fitted future state.
No carried-state source hash can override this recomputation.

Optional `TrainingOverlapTargetV1` binds actual #608 plans at the exact child
end. The native temporal consumer recomputes `(t,t+h]` labels from concrete
baseline and target observations, including all conditioning bounds, target
owners, and temporal exclusions. This is not a caller-supplied horizon or label
value masquerading as proof. Without target bindings, no label claim is made.

Each complete window/product pair executes `derive_reconstruction_bars` for the
declared native timeframe inventory (all seven defaults) and, when configured,
`synchronized_triangle_tuples` under an exact canonical `TriangleBarPolicyV1`.
The same half-open child bounds govern all three asynchronous legs. Native UTC
bars keep their native IDs, source provenance and partial flags; they are not
renamed arbitrary-width closed bars. Tuple output retains actual native events,
sequence, age and matching policy, not synthetic OHLC synchronization.

Native analytics are **completed-window ex-post** results. Their window-end
feature cutoff and full product dependency end are separate fields; original
event time remains unchanged. Public row records do not backfill a completed
window aggregate into an earlier event as a causal feature. Historical
availability remains `None`; generation context is not a publication clock.

## Boundaries and persistence

This first bounded envelope supports at most 256 windows, 4096 total verified
observed/native-alias source events, 4096 expanded memberships/descendants,
32 complete member/scenario variants, and 4096 allocation branches. Carry
inventory is capped at 4096. Native replay is capped at 128 window/product
pairs, 4096 projected event/timeframe cells, and 4 MiB aggregate retained native
evidence. The complete canonical artifact retains the inherited 8 MiB, depth16,
200,000-node and 4096-item collection bounds. Oversized operations refuse;
there is no silent clipping, member selection, tail deletion, chunk-dependent
normalization or arbitrary large-month streaming claim.

Reader/writer boundaries perform fresh complete replay, even with empty output.
Files must be canonical and content-addressed by actual bytes. Local bounded
regular-file reads reject parent/leaf symlinks, directories/FIFOs and changes
during read. Publication uses a flushed/fsynced temporary file and atomic
no-clobber hard link, verifies identical existing bytes, and fsyncs the directory
also on idempotent retry. A parsed envelope alone is never a source-authentic
artifact. Interrupted writes cannot publish a partially written target.

Tests use genuine generated IPC/catalogs and committed synthetic native
publications. They check geometry, source membership/aliases, independent exact
mass arithmetic, useful multi-split exclusions, target sharing, prior-state and
future-conditioning canaries, async triangle/timeframe execution, shuffled worker
results, tampering, empty-selection verification and bounded durable replay.
These are software acceptance tests, not empirical confidence calibration,
market skill, historical availability attestation, or holdout qualification.
