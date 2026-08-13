# Catalog-bound reconstruction experiments

`ReconstructionExperimentManifestV1` is the immutable scientific identity that
joins a provider-neutral dataset selection to the HistData-only reconstruction
runtime. It does not introduce another data store and does not copy tick rows.
The dataset catalog, resolution receipt, specialized evidence/model manifests,
split assignments, policies, gates, and implementation hashes remain separate
artifacts joined by strong references.

## Current executable boundary

Version 2.4 executes only qualified, observed `histdata.com` ASCII/T cache
partitions for the complete EURGBP/EURUSD/GBPUSD triangle. Provider, dataset,
dataset version, origin, delivery profile, experiment role, and local
materialization are separate fields. The provider-neutral fields are durable
identity seams; they do not enable a second provider.

M1, bars, fixture/alternate providers, broker feeds, and OANDA fail closed.
OANDA-compatible API work in #77 and broker-specific adaptation are later
milestones blocked on an explicitly qualified feed and adapter.

## Identity graph

One experiment binds:

- the path-independent dataset catalog ID and exact alias revision/query
  resolution;
- partition IDs, source hashes, sizes, row counts, symbols, periods, coverage,
  provider, origin, format, and timeframe;
- explicit roles such as `historical_anchor`, `modern_reference_training`,
  `tuning`, `calibration`, `protected_holdout`, `negative_control`, and
  `product_input`;
- indivisible split units, selected/label fields, timestamp masking, retained
  row-identity policy, and a recomputable leakage audit;
- strong references to the authoritative feed-epoch, observation, context,
  CFTC, benchmark, motif, evidence-policy, cross-series-policy, configuration,
  and later engine-specific manifests;
- preprocessing, feature-schema, benchmark-gate, package, dependency, Python,
  and module-source identities; and
- limitations and requested Arrow/Parquet export metadata. Canonical cache
  files are never mutated to satisfy an export request.

Local paths remain in the private execution copy so artifacts can be opened,
but they do not participate in dataset-version, catalog, selection, or
experiment scientific identity. Relocating byte-identical caches therefore
retains those IDs. Strong-reference verification still rejects changed bytes,
sizes, catalog resolutions, bindings, split policy, or implementation code.

## Leakage policy

Split units keep shared partitions, adjacent/duplicate ticks, overlapping
windows, context events, and anchor neighborhoods together. Training/tuning/
calibration units cannot reuse partitions or cohesion groups, overlap, or
violate the declared neighbor guard against protected holdout, negative
control, or product-input units. Assignments are frozen before candidate
results. Timestamp masking is permitted only while a source-row identity
policy remains bound.

The current first-party plan freezes one selected HistData range with
`historical_anchor` and `product_input` roles. The qualification campaign and
proposal-engine portfolio add their distinct training/tuning/calibration/
holdout selections to the same contract rather than overloading this product
selection.

## Planner and product lineage

`ReconstructionPlanSpecV1` accepts `dataset_catalog_path` plus
`dataset_reference`. The planner resolves that reference to immutable
partitions before run or plan identity is computed. Every execution manifest
and relevant stage command carries three explicit artifacts:

- `dataset_catalog`;
- `dataset_resolution`; and
- `experiment_manifest`.

Stage loading verifies those artifacts, the catalog-to-inventory equality,
the leakage audit, specialized bindings, and current implementation identity.
The experiment ID participates in run configuration identity. A committed v2
product retains the same ID at `source.experiment_id`, completing the plan-to-
product lineage.

The v2.3 input remains a documented translation rather than an unaudited path:
when only `source_root` is supplied, the planner first verifies feed-epoch
hashes and compiles a local HistData catalog, then continues through the same
resolution and experiment path. New callers should use a catalog selector:

```json
{
  "schema_version": "histdatacom.reconstruction-plan-spec.v1",
  "source_root": null,
  "dataset_catalog_path": "work/histdata-dataset-catalog.json",
  "dataset_reference": "reconstruction-selected",
  "source_format": "ascii",
  "timeframe": "T",
  "symbols": ["eurgbp", "eurusd", "gbpusd"]
}
```

The remaining qualified artifact and root fields are the same as the ordinary
plan specification.

## Discovery and verification

The installed CLI exposes bounded, publication-safe summaries and full local
verification:

```sh
histdatacom reconstruction --json experiment-list --root work/plan-artifacts
histdatacom reconstruction --json experiment-inspect \
  --manifest work/plan-artifacts/reconstruction-experiment-<sha256>.json
histdatacom reconstruction --json experiment-verify \
  --manifest work/plan-artifacts/reconstruction-experiment-<sha256>.json
```

The corresponding Python methods are
`ReconstructionClient.experiments()`, `inspect_experiment()`, and
`verify_experiment()`. List and CLI inspect results omit all local paths and
never embed tick rows.
