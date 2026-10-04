# Managed artifact retention and garbage collection

The `histdatacom.artifact_retention` API separates evidence that must survive
from replaceable caches and completed transaction scratch. It operates only on
an explicitly initialized, dedicated managed store. It does not adopt an existing
data directory, infer references in arbitrary external files, or authorize cleanup
of a backup, repository, runtime, historical dataset or another process's work.

This is a bounded, cooperative POSIX storage protocol, not an operating-system
sandbox. Required no-follow directory descriptors, exclusive locks and durable
filesystem operations must be available; unsupported platforms refuse mutation.
An operator with direct filesystem access can still damage the store. Detected
changes, incomplete transactions and unknown files block collection.

## What is protected

An edge `u -> v` means that retained object `u` requires the live bytes of `v`.
Protected roots include durable root registrations, explicit holds and every
independently permanent retention class. Their complete transitive closure is
kept. Deleting an unreferenced cache never permits deletion of its source.

| Retention class | Collection behavior |
| --- | --- |
| Immutable source evidence | Independently protected, including required dependencies |
| Release/certification evidence | Independently protected; classification is not scientific certification |
| Reproducibility dependency | Independently protected |
| Migration predecessor | Independently protected; a successor is not deletion permission |
| Published derived product | Independently protected |
| Restricted-provider evidence | Disposition unsupported; blocks collection |
| Replaceable cache | Requires actual exact regeneration and absence from protected closure |
| Scratch/temporary | Requires a closed real transaction, no protected reference and expired policy TTL |
| Quarantined-corrupt evidence | Independently protected; corruption is not deletion permission |
| Legal/policy hold | Independently protected; no automatic release or expiry |

Every live dependency relation has the same retention force, including migration,
packed-member and verification-evidence edges. Content hashes, file extensions,
age, a dead process, a status flag and the availability of backups do not grant
deletion rights. Unknown or opaque references block the **whole** plan, not only
the object containing them.

Roots and holds are additive. This version provides no API to remove a root,
release a hold, replace the immutable store policy, downgrade a classification
or import a caller-authored graph. Passive contract constructors and successful
JSON decoding do not establish authority to mutate a store.

## Closed native scope

The initial native producer supports bounded HistData-style ASCII/T tick CSV,
the actual enriched Arrow cache, its exact regeneration recipe/proof, and a new
observed dataset catalog pointing to managed cache paths. Original source bytes
are copied, never moved, linked or removed. Cache production runs with source
deletion disabled and no download fallback, in a fresh unmanaged workspace.

Verification executes the real producer again, compares exact output bytes and
performs native partition readback. Reusing an existing `.data` file is not a
regeneration proof. A recipe binds the actual selected implementation and backend
profile; a changed or unsupported profile refuses instead of silently relaxing
the comparison to approximate data equivalence.

A new catalog is constructed against final managed locations. An imported
immutable catalog is never silently rewritten. Native scientific IDs can be
unchanged when a path changes, so the store separately binds full canonical wire
bytes, physical payload references and store identity. Store relocation is not
supported in this version.

These catalog publications are explicitly **unqualified**. Physical replay is
not scientific validation or release certification. Qualified catalogs, aliases,
parent/composition chains, unknown evidence or metadata, other provider families,
and arbitrary JSON/ZIP inputs are not accepted as reference-free objects. They
require additional complete native adapters and corresponding evidence. In
particular, this feature does not qualify a reconstruction campaign or claim
all-family historical release replay.

Provider retention rules do not imply permission to destroy data. Current
restricted-provider disposal, finite-retention enforcement and compulsory erasure
are unsupported. Unknown, expired, revoked or conflicting authority must not be
turned into a successful GC decision. See [provider policy](broker-provider-policy.md).

## Public lifecycle

Import operations and contracts from `histdatacom.artifact_retention`. Package
import is lazy: it does not open a store or load a native producer.

1. `create_retention_store(root, policy)` initializes a new empty namespace.
2. `ingest_ascii_tick_source(root, source_path, symbol=..., period=...)` admits
   parsed source bytes and returns an admission receipt with `primary_object_id`.
3. `produce_histdata_cache(root, source_object_id)` produces and verifies a cache,
   retaining its recipe and regeneration evidence.
4. `publish_histdata_cache_catalog(root, cache_object_ids, dataset_id=...)`
   publishes a newly located native catalog.
5. `register_retention_root(root, object_id, reason=...)` and
   `add_retention_hold(root, object_ids, reason=...)` add durable protection.
6. `inspect_retention_store(root)` observes complete current inventory;
   `plan_artifact_collection(root, cutoff_ns=...)` derives a deterministic plan.
7. `apply_artifact_collection(root, plan)` revalidates and applies that exact
   retained plan, or refuses it.

Only the real lifecycle can complete managed scratch. Its scratch/work record is
consumed by a verified publication or a closed terminal abort. An uncertain
interruption remains pending and blocks apply. The TTL starts at the actual
store-owned completion time, not a caller timestamp, PID, Boolean or file mtime.
The default TTL is 24 hours; the policy is immutable after initialization.

Native producer workspaces outside the store are retained. Collecting a managed
transaction work record does **not** reclaim those external workspaces. This
version provides no implicit external-scratch deletion or recovery mechanism.
Inspection and planning may create verification workspaces; planning also retains
its plan/audit evidence. A dry run means no managed payload deletion, not zero
filesystem writes or zero native execution.

## Plan and apply commands

For an already initialized and populated store:

```console
histdatacom cleanup artifacts-inspect --store /absolute/path/to/managed-store
histdatacom cleanup artifacts-plan --store /absolute/path/to/managed-store --output plan.json
histdatacom cleanup artifacts-apply --store /absolute/path/to/managed-store --plan plan.json --output receipt.json
```

The commands are distinct from legacy `cleanup sources`, whose default remains a
dry run. Legacy `--apply` or an unscoped YAML `apply: true` cannot activate managed
collection. Artifact commands reject unrelated legacy and cross-command options.
Report output must be a new unmanaged file: existing files and managed namespace
destinations are refused, not overwritten.

The CLI emits canonical JSON. Plan files must contain exact canonical contract
bytes; unknown fields, duplicate keys, noncanonical whitespace, invalid types and
future schema versions are rejected. The plan command's `--output` produces the
exact wire accepted by apply. A `--cutoff-ns` value is allowed only for planning
and cannot exceed freshly observed runtime time. Apply samples time again; a
forward-dated plan cannot mature scratch early.

Example command-scoped YAML:

```yaml
histdatacom:
  cleanup:
    command: artifacts-plan
    store: /absolute/path/to/managed-store
    output: plan.json
```

## Revalidation, journal and failure semantics

Publication, root/hold registration and collection share one exclusive lock.
Apply verifies the complete physical/control inventory, current policy, native
dependencies, real regeneration proofs and clock before reproducing the exact
plan. New roots, holds, admissions, mutations or collection attempts invalidate
an old plan. A plan's own historical audit envelope does not become a new live
root or invalidate its own operative snapshot.

Each individual unlink requires durable intent tied to the exact object, plan
and file observation. There is no recursive managed deletion or destructive
rollback. Successful observation and its journal evidence establish a retained
tombstone; an unexplained missing file does not count as successful collection.
Descriptors and history remain after the payload is collected. An ordinary live
edge to a missing or collected payload still blocks apply.

Regeneration and collection receipts distinguish historical output identity from
live dependencies. The original source and recipe remain required; a historical
output hash does not require keeping the deleted cache bytes forever.

Multiple unlinks and final receipt publication are not one atomic filesystem
transaction. An error stops further deletion. Partial or indeterminate outcomes
report only actual observations; absence is never used to infer a completed
unlink, and ambiguous operations are not automatically resumed. If durable
receipt publication fails, or the clock regresses after an effect, the operation
raises an interrupted error with available observations rather than inventing a
successful receipt or timestamp. A report-export failure does not undo an already
performed operation; inspect the returned receipt and error.

Python callers can catch `CollectionInterruptedError` and inspect its versioned
`to_dict()` evidence. If receipt publication returned but subsequent validation
failed, `receipt_persisted` and `unaccepted_receipt_id` preserve the observed write
without exporting it as an accepted complete receipt. Do not automatically retry
an interrupted operation. `RetentionStoreError` reports that current store
authority could not be established; neither error authorizes repair or deletion.

Surviving protected native roots are replayed after collection. Reported reclaimed
bytes are the bytes actually unlinked, not measured free disk space. Backup
restoration is not part of normal authorization or rollback.

## Bounds and legacy compatibility

The managed inventory is deliberately finite: 1,024 historical objects, 8,192
edges, 128 explicit roots and a 256 MiB aggregate descriptor payload budget,
including retained historical descriptors. JSON authority is bounded to 16 MiB,
262,144 nodes, depth 32 and 8,192 members per collection. Graph depth is at most
64. Native CSV admission is bounded to 2 MiB/4,096 rows, each cache to 64 MiB,
and a supported catalog to 16 partitions. These are admission limits, not a
claim that arbitrary historical corpora fit in one store.

Native replay has an additional per-operation execution and projected workspace
budget: at most 16 producer executions and 1 GiB of projected cache bytes,
plus bounded auxiliary/control headroom. Its complete topology, including final
replay, must fit before work starts. This is not an OS disk reservation, an RSS limit, or a lifetime cleanup
policy; concurrent unrelated disk use can still cause a fail-closed interruption.

The marker `.histdatacom-retention.json` makes legacy package mutation helpers
refuse a managed root, descendant or containing recursive target. Even malformed
or incomplete marker bytes remain protective. Complete batch preflight prevents
an earlier ordinary target from being removed before a later protected target is
discovered. Newly generated campaign cleanup commands perform this check at
execution time. Historical exported shell scripts and arbitrary external writes
are outside the cooperative package guarantee.

Validation uses invented ticks, generated stores, independent graph oracles and
fault injection only. It does not require provider capture, real-data cleanup,
empirical parameter search or scientific qualification.
