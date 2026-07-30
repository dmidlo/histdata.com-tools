# Provider-neutral datasets and immutable catalog resolution

The dataset catalog separates five identities that must not be collapsed into
one free-form `source` value:

| Axis | Meaning |
| --- | --- |
| `source_provider_id` | Organization or feed that supplied an observed record. |
| `dataset_id` | Stable logical dataset selected by configuration. |
| `dataset_version_id` | Immutable content-bound version actually read. |
| `origin` | `observed`, `synthetic`, `derived`, or `composed`. |
| `delivery_profile_id` | Optional modern-reference or broker-conditioned output transformation. |

`synthetic` is an origin and is never a provider. A delivery or broker profile
is not the historical provider unless it supplied the observed record. The V1
enriched-training and `SyntheticEventV1` schemas remain unchanged; V2 dataset
lineage is an explicit companion projection.

## First-party adapters

`HistDataProviderAdapter` owns all HistData-specific behavior:

- cache layout: `ASCII/T/<symbol>/<year>/<month>/.data`;
- source clock: EST without daylight-saving adjustment, normalized to UTC by
  the existing cache builder;
- partition clock allowance: source month with a bounded UTC spill;
- observed identity: one-based cache-row ordinal;
- attribution and local-only redistribution policy.

It reads the existing Arrow IPC caches and preserves timestamps, bids, asks,
row order, row count, duplicate timestamps, and the existing
`ascii:T:<SYMBOL>:histdata.com` series identity.

`FixtureProviderAdapter` is a deterministic reference implementation with a
meaningfully different boundary:

- layout: `<SYMBOL>/<YYYY>-<MM>.csv`;
- required headers: `timestamp,bid,ask`, with optional `vol,native_id`;
- source clock: explicit RFC3339 UTC only;
- partition policy: strict UTC calendar month;
- observed identity: one-based CSV source row;
- default public fixture-only licensing.

The fixture adapter is proof of the interface, not a licensed market feed.

## Adapter authoring and qualification

A future adapter implements the public `ProviderAdapter` protocol:

1. Publish immutable `SourceProviderDescriptorV1` attribution and licensing.
2. Publish `ProviderAdapterDescriptorV1` with a SemVer adapter version and
   exact format, granularity, clock, partition, row-identity, and projection
   policies.
3. Implement bounded `discover`, exact `inspect_partition`, and
   `read_partition` operations.
4. Normalize only ASCII/T bid-ask events. Reject OHLC/M1 inputs, ambiguous
   clocks, malformed/non-finite quotes, timestamp regression, missing hashes,
   invalid symbols/periods, and incomplete requested coverage.
5. Return `CanonicalObservedPartitionV2` objects whose identity binds provider,
   adapter version, artifact and source hashes, coverage, row count, clock,
   partition, row identity, licensing, native partition, and series identity.
6. Produce independent strong qualification evidence before setting a dataset
   version to `qualified`.
7. Increment the adapter SemVer whenever normalization or identity behavior
   changes. Old manifests keep their original adapter version and remain
   replayable.

Credentials, tokens, raw licensed response bodies, and large tick/event
payloads are forbidden in provider metadata, catalog JSON, CLI output, logs,
and workflow histories. Catalog artifacts contain only strong references and
hashes. Licensing policy is descriptive enforcement metadata; it does not
grant acquisition or redistribution rights.

Absolute artifact paths are operational references retained in serialized
catalogs so local verification can reopen the bytes. They are deliberately
excluded from partition, dataset-version, and source-inventory hashes: moving
the same strong content and qualification evidence to another local data root
does not create a new immutable dataset identity.

## Manifest and alias example

The complete catalog schema is `histdatacom.dataset-catalog.v1`. A qualified
observed version contains a provider descriptor, adapter descriptor, logical
dataset descriptor, immutable version manifest, and optional aliases. The
essential version/alias shape is:

```json
{
  "dataset": {
    "schema_version": "histdatacom.dataset-descriptor.v1",
    "dataset_id": "histdata-observed-ticks",
    "display_name": "HistData observed ticks",
    "description": "Qualified canonical ASCII/T observed ticks.",
    "allowed_origins": ["observed"]
  },
  "version": {
    "schema_version": "histdatacom.dataset-version.v1",
    "dataset_id": "histdata-observed-ticks",
    "dataset_version_id": "dataset-version:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "manifest_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "origin": "observed",
    "normalization_policy_id": "histdata-arrow-cache@1.0.0:histdatacom.canonical-ascii-tick.v2",
    "qualification_status": "qualified",
    "source_provider_ids": ["histdata.com"],
    "partitions": [
      {
        "schema_version": "histdatacom.observed-partition.v2",
        "source_provider_id": "histdata.com",
        "adapter_id": "histdata-arrow-cache",
        "adapter_version": "1.0.0",
        "format": "ascii",
        "granularity": "T",
        "symbol": "EURUSD",
        "period": "202001",
        "series_id": "ascii:T:EURUSD:histdata.com",
        "clock_policy_id": "histdata-est-no-dst-to-utc-v1",
        "partition_policy_id": "histdata-source-month-with-utc-spill-v1",
        "row_identity_policy_id": "one-based-cache-row-ordinal-v1",
        "licensing_policy": "local-only",
        "artifact_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "source_artifact_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
      }
    ],
    "parents": [],
    "qualification_evidence": [
      {
        "kind": "dataset_qualification_v1",
        "sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "size_bytes": 128,
        "path": "/local/catalog/evidence/histdata-qualification.json",
        "metadata": {}
      }
    ],
    "delivery_profile_id": null
  },
  "alias": {
    "schema_version": "histdatacom.dataset-alias.v1",
    "alias": "latest-qualified",
    "dataset_id": "histdata-observed-ticks",
    "dataset_version_id": "dataset-version:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "revision": 7,
    "require_qualified": true
  }
}
```

IDs shown above illustrate the exact syntax; a real ID is derived from the
complete canonical manifest and must not be edited by hand.

Derived and synthetic versions contain no observed partitions and no synthetic
provider. They instead list exact `DatasetParentV1` entries. Their V2 event
lineage contains parent versions, ensemble member, event, anchor interval,
generator, and optional delivery profile.

## Resolution, replay, and cursor identity

Resolve a mutable alias exactly once. `DatasetResolutionV1` stores the alias
revision and ID, resolved dataset/version, manifest hash, and query scope. A
later alias move cannot mutate that receipt. Replay uses the receipt's exact
version and manifest hash and deliberately does not resolve the alias again.

Observed row identity is:

```text
(dataset_version_id, series_id, period, row_id)
```

Reconstructed event identity is:

```text
(dataset_version_id, ensemble_member_id, event_id)
```

`DatasetCursorV1` additionally binds `resolution_id` and `query_scope_id`.
Cursors fail closed if reused across datasets, versions, query scopes, origins,
series, periods, rows, or ensemble members. Timestamp is an ordering/range
field, not a unique row key. Symbol/period scopes validate requested matrix
cells rather than only independent symbol and period sets. Reconstruction
inventories require every selected series to cover the complete requested
half-open interval without a gap.

## CLI and typed API

Catalog commands are local and bounded:

```bash
histdatacom datasets --catalog catalog.json list
histdatacom datasets --catalog catalog.json describe latest-qualified
histdatacom datasets --catalog catalog.json resolve latest-qualified \
  --symbol EURUSD --period 202001 --receipt resolution.json
histdatacom datasets --catalog catalog.json verify latest-qualified
histdatacom datasets --catalog catalog.json replay resolution.json
```

The typed API exposes the same behavior:

```python
from histdatacom import DatasetCatalog, DatasetQueryScopeV1

catalog = DatasetCatalog.read("catalog.json")
scope = DatasetQueryScopeV1(symbols=("EURUSD",), periods=("202001",))
receipt = catalog.resolve("latest-qualified", query_scope=scope)
verification = catalog.verify(receipt)
replayed = catalog.replay(receipt)
```

`DatasetCatalog.reconstruction_inventory` and
`preflight_reconstruction_inventory` accept qualified versions produced by
either first-party adapter without importing private implementation helpers.

## Relationship to the OANDA-compatible API

GitHub issue #77 owns the future REST transport. That API must accept a logical
dataset reference, resolve it once, and report the immutable
`dataset_version_id` on every response, receipt, cursor, and replay. Its page
tokens must embed the V1 dataset cursor identity rather than relying on only
`series_id`, `period`, or timestamp. The catalog does not implement HTTP,
playback, live broker capture, broker credentials, or broker-specific
adaptation.
