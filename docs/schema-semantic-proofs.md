# Exact semantic migration proofs

Issue #634 adds an executable proof layer next to the metadata-only schema
compatibility registry. It does not change any existing native wire contract,
reader policy, event identity or compatibility-query result. The packaged
registry currently declares **no qualified production migrations**. That is a
nonclaim, not evidence that historical versions are equivalent.

The implementation is exercised on actual current native readers and synthetic
artifacts, including two executable JSON-encoding transformations and composed
paths. Synthetic graph descriptor versions are not invented native historical
schema generations. A future migration needs an explicitly reviewed profile,
executor, exact input corpus and recomputable evidence before the offline
bundled-registry gate will admit its lossless declaration. The all-family
cross-generation golden corpus remains separate work under #635.

## Reviewed domains

Nine concrete artifact profiles cover eight native-family groups:

| Profile | Meaning retained |
| --- | --- |
| `synthetic-event-v1`, `synthetic-stream-v1` | Origin, nanosecond time and sequence, exact binary64 quotes, source lineage, generator/scenario/member identities and ordered events |
| `evidence-projection-v1` | Source IDs and SHA, availability/as-of/support clocks, information mode, policy, records, limitations and readiness |
| `broker-metadata-v1` | Plugin and SDK versions, plugin identity, display metadata and namespaced extensions |
| `broker-event-v1` | Session/connection identity, exact decimal lexemes, source/receive clocks, quote/size/activity semantics, raw provenance, gaps and diagnostics |
| `broker-lifecycle-manifest-v1` | Complete inventory/capability/policy header, partition references, epochs, counters, source-loss limitations and terminal outcome |
| `economic-release-v1` | Official-source reference, schedule/release/revision/availability clocks, timezone evidence, numeric values and lexical/precision/unit meanings |
| `forecast-score-v1` | Forecast task/model/target/horizon, retained vintages, normalization and independently recomputed metrics |
| `training-temporal-batch-v1` | Source/product/member binding, exact cutoffs, feature/label definitions, ordered rows, target support, availability and nonclaims |

Each domain is the **complete current JSON writer envelope**, within this
layer's resource limits. Top-level fields are explicitly enumerated. Reviewed
native reader modules and shared contract modules are source-pinned; changing
those bytes requires review and a deliberate new pin. Pins are not silently
regenerated to make qualification pass.

The actual native reader verifies identities and its normal contract rules.
The raw tree must then match its complete writer tree exactly, recursively and
with scalar types distinguished. Consequently an unknown nested field, missing
default, normalized identifier, re-sorted event array or ignored derived field
cannot disappear into a semantic proof merely because a permissive native
reader accepts it. This new domain is deliberately narrower than some legacy
readers; legacy callers are unchanged.

The independent projector consumes that admitted **raw** tree, not the native
writer's identity payload. It excludes only the profile's explicitly named,
native-verified top-level self-derived identity. All nested and upstream
identities, hashes, references and policy fields remain semantic. It never
strips fields by suffix, globally deletes hashes, or assumes provenance is
representation-only. Dynamically shaped extension values remain retained,
not treated as undocumented ignorable fields.

## Exact meaning rules

- JSON object whitespace and key order are representation-only. Duplicate
  keys refuse. Unicode code points are not normalized.
- Arrays retain order. These initial profiles define no interchangeable
  unordered collection representation; even reader-normalized sets must
  arrive in their native writer's order. Future set rules need versioned,
  collision-safe ordering and their own review.
- Null, missing, booleans, integers, numbers and strings are distinct. There
  is no missing-to-default substitution in the proof domain.
- Binary64 values use exact IEEE-754 bits, including signed zero. JSON numeric
  spellings denote their parsed binary64 value; there is no tolerance or
  decimal-lexeme preservation claim for native binary64 fields. Nonfinite
  values refuse. Native decimal **strings** preserve their exact lexemes;
  `"1.00000"` and `"1.0"` are not equivalent.
- Integer clocks retain their exact units and values; booleans/floats cannot
  substitute for integer nanoseconds. Event, provider, receive, availability,
  as-of and decision clocks remain separate. Timezone/lexical evidence is
  retained; equivalent UTC instants do not erase source timezone claims.
- `exact_unit_conversion` permits only bounded integer-domain, exactly
  invertible rational conversions. A nanosecond value not divisible by the
  destination unit refuses rather than rounding. No shipped executor changes
  units or timestamps.

Canonical semantic trees use disjoint type tags and sorted object-key pairs,
so user objects cannot collide with the numeric-tag representation. The
semantic digest binds the reviewed profile domain and this exact tree. Byte
formatting can change the input SHA without changing that semantic digest.

## Executable evidence and registry gate

```python
from histdatacom.schema_semantics import (
    project_semantics,
    prove_semantic_migration,
    validate_lossless_evidence,
    verify_semantic_projection,
)

projection = project_semantics("synthetic-event-v1", native_event_json)
verify_semantic_projection(projection, native_event_json)

# registry is an explicitly constructed and reviewed compatibility graph.
proof = prove_semantic_migration(
    registry, edge_id, source_profile_id, destination_profile_id, source_json
)
receipt = validate_lossless_evidence(registry, (proof,))
```

The last example requires all referenced edges/compositions to be covered, not
just one selected path. The API does not invent evidence for a missing edge.

Two closed executors are admitted: canonical compact JSON and deterministic
indented JSON. Their exact implementation module and canonicalizer hashes,
version and qualified name must match the registry declaration. Evidence
locators are never executed or imported. Unknown implementations refuse.
Creation executes the selected code; verification re-executes it and compares
the **exact output bytes**, as well as independently recomputed before/after
semantic projections. Supplying an equivalent-looking after-payload is not
proof of what a named migration actually did.

The acyclic edge subject includes the named edge, exact endpoint IDs and
declared versions/families/wire labels, reviewed profiles, classification,
invariants and executor identity. It excludes proof references and the full
registry hash, avoiding a content-hash cycle. Evidence references bind the
resulting canonical proof bytes by SHA. Golden input evidence must separately
bind the actual source bytes. The final gate receipt binds the complete
registry identity after evidence publication.

Compositions retain ordered independently replayed steps, byte-identical
intermediate handoffs and explicit registry composition evidence. Graph
reachability alone never implies composition qualification. Native mutation
fixtures cover source hashes, clocks, quotes, origins, member/scenario and
plugin identities, availability, policy, support and terminal outcomes.
Correctly resealed false projections and substituted implementations refuse.

`scripts/generate_schema_compatibility.py` invokes the mandatory offline gate
before either checking or publishing a bundled registry. A newly marked
lossless edge without executable evidence fails this gate. Existing public
`can_read`, `can_migrate`, `migration_path` and `exact_semantics` remain
producer-free metadata queries. User-created compatibility graphs are still
**declarations**, not runtime scientific proof; callers use the public proof
validator explicitly when qualification is required.

## Persistence and limits

`write_semantic_proof(proof, directory, registry)` verifies before publishing a
content-named canonical JSON file, using exclusive temporary creation and
atomic no-clobber linking. Existing different bytes, symlinks and nonregular
files refuse. `read_semantic_proof(path, registry)` checks canonical bytes and
filename SHA, then re-executes all bound implementations and projections.
There is no `verify=False`. Contract `from_dict`/`from_json` methods check
structure only; they are not a substitute for this qualified reader or
`verify_semantic_proof`.

Inputs are at most 8 MiB; encoded proof envelopes at most 32 MiB. Trees have a
262,144 expanded-node bound and depth 64; tagged projections must fit those
bounds too. Collections, repeated aliases, UTF-8 input size and escaped output
cost are checked before potentially large expansions. A composition has at
most 16 steps; a catalog has at most 512 entries and 64 MiB of canonical proof
evidence. Source/proof reads are bounded regular-file reads with no-follow
checks and explicit identity stamps; concurrent changes fail closed. These
limits refuse unsupported larger inputs without declaring them lossy science.

Run qualification in a fresh process on reviewed installed/source bytes.
On-disk hashes are operational identities, not attestation of loaded bytecode,
all transitive dependencies, a hostile same-user runtime or Git ancestry. A
semantic proof is evidence about the meaning of the retained artifacts and
the admitted transformation. It is not source authenticity, fresh raw-data
replay, broker continuity, forecasting quality, scientific admission, or
release/campaign approval. A failed invariant requires repairing the
implementation or classifying the transition as lossy/semantic-successor;
weakening exactness to preserve a lossless label is not supported.
