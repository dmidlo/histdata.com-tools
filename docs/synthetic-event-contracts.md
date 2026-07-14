# Synthetic event contracts

The version-one synthetic contracts define the narrow durable row boundary for
historical reconstruction. They do not replace the enriched ASCII tick frame.
The 521-column frame remains an in-memory computation surface; accepted output
uses the fields below.

## Contract boundaries

- `SyntheticEventV1` represents one immutable observation or one generated
  event.
- `SyntheticEventStreamV1` represents one symbol, run, and ensemble member.
- `SyntheticEnsembleManifestV1` records compact member counts, stream IDs, and
  content hashes without embedding event rows.
- `histdatacom.data_quality.synthetic_generation` remains the same-cardinality
  Stage-0 control. It does not emit these variable-cardinality events.
- Window planning/checkpoints belong to #432. The implemented empirical-motif
  candidate generator is documented in
  [`empirical-motif-generation-contracts.md`](empirical-motif-generation-contracts.md),
  while atomic final publication/partition layout remains #446.

## Event ordering and identity

`event_time_ns` is a signed 64-bit count of UTC nanoseconds since the Unix
epoch. `event_sequence` is a non-negative signed 64-bit integer assigned
stably within one timestamp. A symbol stream rejects duplicate
`(event_time_ns, event_sequence)` pairs and sorts by time, sequence, then event
ID. Duplicate timestamps therefore remain distinct and deterministic.

Observed event IDs are derived from schema, symbol, event position, source
version, `source_series_id`, `source_period`, and immutable `source_row_id`.
Run and ensemble member do not alter an observed event's identity.

Synthetic event IDs are derived from schema, event position, run/member,
ordered anchors, generator/version/configuration, source version, optional
reference/motif/feed/broker lineage, and the constraint set. Process count,
retry count, and window/partition placement are not identity inputs.

The source/configuration version is responsible for price-mark semantics.
Changing generator semantics without changing its version/configuration is a
contract violation.

## Origin-specific lineage

Observed events require:

- `source_version_id`
- `source_series_id`
- `source_period`
- positive `source_row_id`

They reject synthetic lineage so an observed row cannot be relabeled as an
invention.

Synthetic events require:

- `source_version_id`
- ordered left and right anchor event IDs and `anchor_interval_id`
- `generator_id`, `generator_version`, and `generator_config_id`
- `constraint_set_id`

Reference, motif, feed-epoch, and broker-profile IDs are nullable because not
every generator stage has used them yet. If used, they are row-aligned scalar
lineage. Confidence is nullable until a stage has a defined, versioned
calibration quantity and scope; supplied values must be finite and in
`[0, 1]`. Raw motif-match similarity is not such a quantity and is retained
only on empirical-motif transformation evidence. The #442 ensemble confidence
quantity is exact-stratum metric/horizon interval coverage and does not
populate per-event confidence. Synthetic events reject observed source-row
identity.

## Flat Arrow and Parquet schema

The Arrow schema contains 26 scalar columns:

```text
schema_version event_id origin symbol event_time_ns event_sequence bid ask
run_id ensemble_member_id source_version_id source_series_id source_period
source_row_id anchor_interval_id left_anchor_event_id right_anchor_event_id
generator_id generator_version generator_config_id reference_id motif_id
feed_epoch_id broker_profile_id constraint_set_id confidence
```

It intentionally contains no `dq_*`, `cm_*`, or same-row `synth_*` analytical
columns. Bid and ask are finite positive Float64 values; event time, sequence,
and source row ID use signed Int64. Stream identity/count metadata is bounded
Arrow schema metadata rather than a repeated nested report.

Arrow remains optional. Importing `histdatacom.synthetic` does not import
PyArrow; Arrow/Parquet helpers require the `histdatacom[arrow]` extra only when
called. Parquet helpers use a fixed Zstandard/version/data-page configuration
for stable output under the same pinned runtime. The file helper is
intentionally non-atomic: #446 owns temporary paths, validation, checksums, and
atomic publication.

## Schema evolution

Version-one class names, ID derivations, required fields, ordering rules, and
Arrow types are frozen. A semantic change requires a new schema version and
new contract class. Version-one readers reject other schema versions and
Arrow schema drift.

For JSON compatibility, missing nullable fields are treated as null and
unknown keys are ignored. This permits bounded envelope metadata to evolve
without silently changing the persisted version-one row. Deterministic IDs are
recomputed on every read; a supplied event, stream, or ensemble ID that does
not match its canonical identity fails closed.

## Streaming use

These classes are correctness contracts, not permission to place an entire
period in Temporal workflow history. #432 must move event batches through
artifact references and bounded checkpoints. `SyntheticEventStreamV1` is
suitable for a bounded window/partition or test artifact; final period-scale
storage must use the Arrow schema incrementally.

The implemented window, batch, carry, checkpoint, resource, and two-phase
publication protocol is documented in
[`reconstruction-streaming-contracts.md`](reconstruction-streaming-contracts.md).
Run-bound ex-post/ex-ante modes, artifact availability, chronological splits,
window-plan look-ahead, and the fail-closed leakage gate are documented in
[`reconstruction-information-modes.md`](reconstruction-information-modes.md).
