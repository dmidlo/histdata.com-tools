# Reconstruction persistence contracts

`histdatacom.synthetic.persistence` is the final durable boundary for the
v2.1 reconstruction product. It accepts one fully applied
`BrokerRenderedGroupV1`, exact immutable observed anchors, and a successful
storage/retention preflight. It writes only the 26 fields in
`SyntheticEventV1`. The 521-column analytical frame, candidate surfaces,
individual rejected rows, and broker-render workspaces remain ephemeral.

## Transaction and layout

One all-symbol synchronization unit is one atomic transaction. Its layout is:

```text
reconstruction-products/
  schema=<product-schema>/
    run=<run-id>/
      broker=<fingerprint-id>/
        member=<ensemble-member-id>/
          group=<symbol-group-id>/
            .scratch/
              publication.tmp-*/
            commits/
              <logical-publication-id>/
                manifest.json
                symbol=eurusd/
                  event_date=2023-11-14/
                    part-00000.parquet
```

All path components are percent-encoded. The manifest stores paths relative to
its transaction directory and rejects absolute paths, traversal, alternate
separators, or paths that do not match the symbol/date partition evidence.

`stage_reconstruction_publication()` writes each Parquet file through a partial
name, fsyncs it, validates it, atomically publishes the compact manifest inside
the hidden transaction directory, and validates a clean replay. The final
commit revalidates every file and promotes the complete directory with one
same-filesystem `os.replace()`. Discovery scans only `commits`; a crash before
the rename therefore cannot advertise a partial synchronized group.

The logical publication ID does not depend on scratch paths, worker attempts,
or partition bytes. Repeating the same input under the same retention plan is
idempotent. A retry with different content or physical writer settings cannot
replace the committed publication.

## Exact rows and immutable anchors

Every Parquet footer is required to expose exactly
`SYNTHETIC_EVENT_ARROW_COLUMNS`, in order and with the version-one Arrow types.
Unexpected analytical or training columns fail publication. Files use Zstandard
compression, statistics, Parquet 2.6/data-page v2, page checksums, deterministic
schema metadata, and bounded row groups.

The caller must provide the immutable observed anchors independently of the
rendered output. Publication compares the complete serialized observed rows by
event ID. Equal IDs with different bid, ask, time, source identity, or lineage
still fail. The source manifest then records counts plus hashes of exact
observed content and IDs.

## Compact manifests

The top-level manifest embeds bounded summaries, never event rows:

- partition evidence: relative path, symbol/date, counts, time bounds, stream
  and source IDs, row groups, byte size, byte hash, and logical hash;
- source evidence: source versions/series/periods and exact observed-anchor
  counts/content/ID hashes;
- constraint evidence: generator, configuration, constraint, and feed-epoch
  IDs plus compact hashes of per-event motif/reference assignments;
- quality evidence: broker-transfer/fingerprint IDs, final validation and #331
  quality hashes/status, observed/synthetic/lineage counts, broker lineage hash,
  action counts, and optional benchmark comparisons;
- replay evidence: partition-independent logical hash, physical byte aggregate,
  hash algorithms, writer/runtime version, compression, row-group size, and any
  canonicalized metadata exclusions; and
- retention evidence: primary member, every retained member, conservative event
  counts, compressed-byte estimates, manifest overhead, and the exact #432
  storage policy ID.

Counts reconcile across partitions, source, constraints, broker transfer, and
the top-level manifest. Manifest IDs cover the compact physical evidence;
publication IDs cover the logical product identity.

## Preflight and retention

`estimate_reconstruction_retention()` is called before event generation with
the primary member, all retained members, conservative event counts, and
expected partition count. It estimates primary bytes, all-retained bytes, and
manifest overhead and refuses before work when output bytes, retained member
count, or partition count exceed policy. Publication additionally refuses if
actual rows or partitions exceed those conservative estimates or if staged and
final bytes exceed the bound policy.

This preflight is the final-storage complement to #442's all-member computation
and scratch estimate. Omitted ensemble members remain reproducible compact
artifacts; they are not silently materialized as final Parquet.

## Replay, reads, and corruption

`verify_reconstruction_publication()` checks manifest identity and location,
file sizes and SHA-256 values, exact schemas, row-group limits, origin counts,
time bounds, per-partition logical hashes, the aggregate byte hash, exact
observed-anchor hashes, and the clean partition-independent logical replay
hash. A truncated footer, changed row, extra column, moved manifest, or altered
derived count fails closed.

`iter_reconstruction_event_batches()` uses Arrow Dataset scanners with column
projection and event-time predicates. `scan_reconstruction_events_polars()`
returns the corresponding lazy Polars plan. `reconstruction_parquet_paths()`
first prunes whole files by manifest symbol and time bounds, which also gives
DuckDB a narrow file list. The committed files are ordinary Parquet, so both
engines retain footer-statistics and column pushdown.

`read_reconstruction_streams()` is the integrity-first replay path. It verifies
the complete publication, reconstructs exact symbol streams, and confirms the
logical hash again.

## Cleanup and orchestration handoff

`cleanup_reconstruction_scratch()` removes only transaction directories named
`publication.tmp-*` below a product `.scratch` directory. It rejects paths
outside the product root and never traverses into `commits`, raw caches, source
artifacts, or unrelated operator files.

Staged and committed manifests expose strong `ArtifactRef` values. Their bytes
are identical across promotion, so they plug directly into the #432
`running -> staged -> validated -> committed` checkpoint transitions. #447 owns
the Temporal activities/workflows that call these operations and must record a
committed checkpoint only after the atomic directory promotion succeeds.

## Query dependency

Arrow persistence is available through `histdatacom[arrow]`. Polars remains a
base dependency. DuckDB is an optional query/smoke dependency exposed through:

```sh
pip install "histdatacom[query]"
```

InfluxDB is not the canonical archive. A serving adapter may later project
selected committed columns into InfluxDB without changing these durable
contracts.
