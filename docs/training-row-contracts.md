# Origin-preserving training rows v1

This additive #606 foundation materializes bounded, source-replayed rows. It
does **not** certify the wide training corpus in #605/#612, assign train/test
splits or purge horizons (#608), invent labels, assign synthetic weights (#607),
or implement cross-family causal joins (#610). Frozen event, bar, forecast and
legacy enriched-tick schemas are unchanged.

## Public workflow

```python
from histdatacom.data_quality.training_contracts import (
    TrainingConsumerMode, TrainingRequestV1, TrainingSourceV1,
)
from histdatacom.data_quality.training_lineage import build_training_ownership
from histdatacom.data_quality.training_views import (
    materialize_training_rows, training_frame,
)
from histdatacom.data_quality.training_artifacts import (
    read_training_artifact, write_training_artifact,
)

# catalog is an existing DatasetCatalog with immutable observed partitions.
source = TrainingSourceV1(catalog.to_json(), dataset_version_id)
ownership = build_training_ownership(source)
request = TrainingRequestV1(
    TrainingConsumerMode.DESCRIPTIVE, start_ns, end_ns, ("EURUSD",),
)
batch = materialize_training_rows(source, ownership, request)
path = write_training_artifact(batch, output_directory)
verified = read_training_artifact(
    path, consumer_mode=TrainingConsumerMode.DESCRIPTIVE,
)
frame = training_frame(
    verified, consumer_mode=TrainingConsumerMode.DESCRIPTIVE,
    value_columns=("bid", "ask"),
)
```

Direct module imports are public. Strict source typing is checked; the legacy
`data_quality` namespace is not newly advertised as a fully PEP 561 typed
package. Installed-wheel runtime consumers exercise the same workflow.

The source also accepts sorted, unique committed `product_manifest_paths`,
`context_artifact_paths` and `derived_artifact_paths`. These are replay locators,
not verification tokens. Source construction and `from_json` validate bounded
structure; they do not perform I/O or certify authenticity. Ownership creation,
row materialization, frame projection, persistence and persisted reading verify
the actual upstream files. There is no public `verify=False` persistence path.

## Verification and admissibility

| Row source | Origin | Executable modes | Evidence meaning |
| --- | --- | --- | --- |
| Exact observed IPC/fixture source row | `observed` | Descriptive, reconstruction sensitivity, representation augmentation | Source-byte/normalized-value integrity; no historical availability receipt |
| Committed native observed anchor | `observed` | Same three modes | Product replay plus exact observed source-row reconciliation |
| Committed generated native event | `synthetic_reconstruction` | Same three modes | Ex-post descendant, never another historical observation |
| Native generated event retaining broker profile | `broker_conditioned_counterfactual` | Same three modes | Explicit counterfactual provenance, not observed broker history |
| Complete normalized macro feature-matrix cell | `official_context` | Descriptive only | Retained normalized context and recomputed cells; **not official raw-source authentication** |
| Complete feature-aware machine forecast | `machine_forecast` | Descriptive only | Replayed retained inputs/model/distribution, not an official release or empirically qualified forecast |

`synthetic_customer_flow` remains a reserved, unsupported materialization origin;
an origin string is never sufficient evidence. `ex_ante` and `mixed_forbidden`
are represented in the contract but no v1 source emitter admits them. In
particular, a caller's asserted availability clock or normalized calendar hash
cannot turn these legacy sources into historically knowable inputs. Causal
forecasting/trading mode refuses even an empty selection, before source replay.
The matrix is enforced by emitters, complete batch validation, frame projection
and persisted readers; changing a requested mode is not a metadata-only edit.

The official-context label describes the normalized artifact's macro-origin
category, not an authentication claim. Its roots retain
`derived_artifact_self_consistency_not_official_source`, and every row retains
`normalized_context_is_not_official_source_authenticity`. Feature matrices must
have macro definitions for their retained observations. Other normalized feature
kinds cannot be relabeled official by this emitter. Missing cells stay missing.
Score artifacts and future label products are not feature-row inputs.

## Frozen evidence-unit ownership

The policy is `utc-day-complete-graph-dependency-closure.v1`. Start with a
complete nonoverlapping UTC-day partition of the immutable observed version's
coverage, including any verified month-boundary spill. Use the complete source
symbol graph, never the output's selected symbols. Hash each unit's exact
normalized observed rows (including raw source fields), immutable observed
version, interval, graph and retained context roots.

Before choosing an output member or window, verify the **complete explicitly
admitted dependency inventory**. Conservatively merge every day touched by one
committed product's entire retained event/anchor support, not just its generated
event's nearest anchor pair. Every observed event must reconcile by immutable
version, series, period and one-based source row, timestamp and exact quote
values. The existing source-qualified quote-order projection is replayed when
declared; source values are not silently rewritten. Generated events must name
actual retained enclosing observed anchors for the same symbol. A cross-midnight
pair therefore merges both days, and a three-day product merges all three.

Context feature matrices merge their complete reference-period, observation,
schedule and cutoff support. Feature-aware forecasts conservatively include
both inference and training matrices, retained calendar coverage and generation
time; their dependency intervals also participate in closure. Dependencies
outside observed coverage refuse. Empty days remain explicit units; their
presence is not evidence of observations or complete source availability.

The **ownership-map ID** binds the complete admitted inventory. The **historical
unit ID** excludes run IDs, member IDs, product IDs and purely derived artifact
IDs. Adding a same-support sibling changes the map/row/batch identities but not
the historical unit. Different output windows or graph projections cannot change
unit ownership. Changed anchor/context identities change the affected unit.
Changed derived identities change row/batch identity; changed dependency support
may legitimately require a different interval partition and cannot be silently
substituted into an existing map. An undeclared or later incompatible product
refuses. Rebuild and explicitly adopt a new map instead of reusing old unit IDs.

This is dependency closure over a declared bounded inventory, not discovery of
all possible experiments in the world. A downstream split must bind one common
map for all admitted siblings. Multiple independently constructed maps cannot
be combined and assumed independent. Alias references, unverified parent IDs,
unsupported adapters and invented source row numbers refuse.

## Actual row grains, clocks and lineage

Observed requests select exact half-open event intervals `[start_ns,end_ns)` and
canonical symbols. The narrow quote spine preserves bid/ask/source-row identity;
it does not copy the legacy enriched frame's full-period medians or diagnostic
regime classifications. Raw HistData `vol` remains source evidence only, omitted
from model-facing values. `volume_state` is explicitly
`unavailable_pending_source_semantics` (#653); no traded-volume claim is made.

Select a native product using its exact `product_manifest_id`. Every selected
native event is emitted once, with all frozen native fields preserved in
`value_json`, plus separate origin/ownership/lineage. Observed anchors retain
observed origin, while generated events retain reconstruction or broker
counterfactual origin. Run/member IDs and exact generator/constraint configuration
IDs are retained; these configuration references are not newly invented
scientific scenario classifications. Native confidence is retained with an
explicit uncalibrated interpretation. There are no fabricated uncertainty bands.

Market event time is never overwritten. Observed decision time defaults to event
time as a row clock, **not proof of availability**. Native decision time defaults
to the end of its full ex-post dependency support; an explicit earlier decision
refuses. Market rows have unknown (`null`) historical `available_at_ns`.

Select a complete context/forecast artifact with `feature_artifact_id`. Context
rows are one recomputed matrix cell per requested reference period/column, with
the cell's period-end-minus-one reference clock. Forecast rows are one full
distribution per forecast artifact, located at its forecast cutoff. The original
distribution, point statistic, target and uncertainty identity are retained.
Context/forecast availability fields retain the normalized asserted clocks, with
the weaker verification level and ex-post admission; they do not certify those
clocks historically. No currency-to-FX join is invented: these rows have nullable
per-row symbol and require the complete observed graph in the request. All
dependency support must precede their declared decision time. Value materialization
is not a cross-source join or a label.

Every row binds unit and map IDs, source row/event or artifact-cell identity,
source version, clocks, origin, information mode, run/member/configuration where
applicable, exact upstream roots, feature schema, explicit `none.v1` label schema,
admissible modes and nonclaims. Root records specify verification strength rather
than a generic misleading `verified=True` flag.

`training_frame` provides scalar values and mandatory `lineage.*` columns. Value
column projection cannot remove origin, unit, map, schema or verification roots.
Empty and populated frames retain explicit compatible dtypes. A user can of
course manually drop columns from any DataFrame; that modified frame is no
longer a verified substrate artifact and cannot pass complete-batch replay.

## Integrity, limits and qualification

Canonical, finite, strict wire envelopes reject unknown fields, duplicate JSON
members, scalar coercion, changed IDs, non-finite numbers, oversized collections,
cycles and deeply nested or expanded repeated-reference structures. Frozen
objects cache immutable IDs/JSON to avoid repeated whole-map hashing per row.
Complete persisted batches are content-addressed, written atomically without
overwriting, and read only from bounded regular files. Readers check filename
hash, canonical bytes, consumer mode and full source replay. A re-sealed false
value, missing row or stripped provenance still fails against the real sources.
Source bytes are checked again after parsing to detect replacements during reads.

Bounds: 8 MiB canonical envelope, 4,096 emitted rows/collection members, 16 levels
of JSON nesting, 200,000 traversal nodes, 32 paths per source category, 2,000,000
total declared observed/product rows and 512 MiB declared source artifacts.
Source work is preflighted before catalog hashing or product replay. Feature
artifact files receive the stricter local bound before their existing complete
reader runs. Frames additionally cap 250,000 output cells. Use bounded row
slices and value projection; this is not a full-corpus materializer or a promise
of a particular memory/throughput profile. No shared environment, dependency,
campaign, holdout or release state is changed by these APIs.

Qualification includes real temporary IPC/catalog and committed V1/V2/V3 product
roundtrips, normalized macro and forecast consumers, cross-midnight/multiday
closure, sibling/member/window/graph invariance, changed source/derived/context
identity, mode/refusal matrices, source mutation during parsing, forged/re-sealed
rows, origin-preserving projection, byte/depth/count bounds and installed-wheel
runtime replay. Fixtures establish executable invariants, not empirical market
quality or broad scientific eligibility.
