# Reconstruction schema discovery and compatibility

The installed schema registry is the authoritative discovery surface for the
reconstruction substrate. It inventories current versioned contracts, field
semantics, legacy translations, internal-only artifacts, and reserved future
contracts without reading or publishing tick rows.

## Current executable boundary

The v2.4 executable data boundary is deliberately narrow:

- provider: `histdata.com`;
- source representation: local HistData ASCII cache data;
- grain: raw tick timeframe `T`;
- synchronized symbols: `EURGBP`, `EURUSD`, and `GBPUSD`; and
- delivery mode: `modern_reference`.

Provider-neutral dataset identity and event contracts are architectural
foundations. They prevent source assumptions from leaking into scientific
identity and lineage, but they do **not** qualify alternate datasets in this
milestone. OANDA, other providers, live broker capture, and broker-conditioned
delivery are later-milestone work and are refused before plan construction.

## Installed CLI

Discover every audited contract as deterministic JSON:

```sh
histdatacom reconstruction schemas --json
```

The human form is intentionally compact and publication-safe:

```sh
histdatacom reconstruction schemas
```

Audit a proposed plan and every referenced JSON evidence artifact before
construction:

```sh
histdatacom reconstruction compatibility --plan plan-spec.json --json
```

The compatibility command does not mutate caches, artifacts, or the plan. Its
exit status is success only for an executable result, refusal for a registered
research-only future contract, and invalid-plan for stale, unsupported, or
invalid input.

## Typed API

The CLI and Python API return the same contracts:

```python
from histdatacom.reconstruction import ReconstructionClient

client = ReconstructionClient()
registry = client.schemas()
report = client.compatibility("plan-spec.json")
if report.executable:
    plan_ref = client.construct_plan(
        # A validated ReconstructionPlanSpecV1 for the same JSON content.
        spec
    )
```

`construct_plan()` invokes the same compatibility engine before the scientific
planner. Contract-specific readers then hash-verify and validate the complete
artifact content; schema discovery is therefore an admission control, not a
replacement for strong artifact validation.

## Registry semantics

Every entry declares its schema version, family, grain, lifecycle status,
information modes, consumer stages, publication policy, owning implementation,
and audit note. Dataclass-backed entries also expose every field with:

- dtype and nullability;
- required, optional, deprecated, or reserved status;
- grain and identity role;
- source, derived, or declared basis and lineage role;
- availability/as-of semantics;
- publication safety; and
- information-mode and consumer-stage restrictions.

Version constants without a public dataclass are not silently omitted. They
are registered as `internal_only`, with their defining constant recorded. The
registry is bounded to 512 contracts, 1,024 fields per contract, and 8 MiB of
canonical JSON. Its `registry_id` is a SHA-256 identity over the complete
canonical payload.

## Cache compatibility

Two HistData cache shapes are recognized:

| Cache | Result | Identity rule |
| --- | --- | --- |
| Legacy `datetime,bid,ask[,vol]` Arrow IPC | `compatible_translation` | Provider + symbol + period + stable source-row ordinal; timestamp alone is never identity. |
| Complete `histdatacom.ascii-tick-training-features.v1` cache | `exact` at the cache boundary | `series_id + period + row_id + event_seq`, with explicit source/provider/format/timeframe fields. |

Observed timestamps, bids, asks, and source-row identities are immutable.
Nullable row-aligned `synth_*` columns are deprecated auxiliary training
placeholders. They are not the variable-cardinality `SyntheticEventV1`
reconstruction product and cannot substitute for its event sequence and
origin-specific lineage.

An incomplete enriched schema, unknown columns, OHLC/M1 data, a stale training
version, a non-HistData provider marker, a timestamp-only identity, or an
unreadable partition fails closed. Reports aggregate identical cache schemas
and partition counts; they never expose local paths or row values.

## Compatibility states

| State | Meaning |
| --- | --- |
| `exact` | The subject matches an installed contract without adaptation. |
| `compatible_translation` | A declared deterministic adapter is required, such as legacy cache row identity or the v2.3 empirical-motif plan boundary. |
| `deprecated` | Accepted for migration but no longer the preferred contract. |
| `research_only` | Registered for a later issue, but deliberately non-executable. |
| `stale` | Known version family, wrong installed version. |
| `unsupported` | Outside the qualified provider or contract boundary. |
| `invalid` | Malformed, unversioned, unknown-field, wrong-grain, or otherwise unsafe. |

The reserved v2 portfolio plan and proposal-engine descriptor document the
handoff to issue #489. Point-in-time evidence, synchronized cross-series
constraint windows, and experiment manifests similarly reserve the contract
seams for issues #483, #484, and #486 without falsely claiming that those
implementations already exist.
