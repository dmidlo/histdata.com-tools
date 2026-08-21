# Measured campaign resource envelopes

The resource audit is the admission gate between the independently verified
final support map and a complete reconstruction campaign. Planning estimates
remain useful for shaping windows, but they are not accepted as physical
storage or runtime evidence. The audit measures committed synthetic-delta v3
products, fits deterministic conservative envelopes, forecasts the exact
all-member support rectangle, and binds the result to the frozen release
candidate and qualified mounted filesystem.

## Measurement census

`ReconstructionResourceProbeV1` identifies one successful product or one
refusal, cancellation, or failure case. The complete
`ReconstructionResourceMeasurementCorpusV1` must include every terminal
outcome and cover all values of these declared axes:

- early sparse, feed-transition, crisis/high-activity, and modern-dense eras;
- low, median, and high inferred missingness;
- exact and bounded-nearest alignment;
- zero and positive synthetic deficit;
- deep-recursive and unsplit/shallow windows; and
- all retained scenarios and ensemble members.

A successful probe strongly references a v3 product manifest. Verification
rereads that publication and independently measures logical observed and
synthetic events, physically stored synthetic rows, Parquet and manifest
bytes, directory bytes and inodes, Parquet row groups and occupancy,
compression, observed-anchor verification reads, and verification throughput.
The probe's runtime telemetry retains wall and CPU time, peak RSS and scratch,
stage output, write and candidate amplification, Poisson work, Temporal
history, checkpoint bytes, and the terminal cleanup result. Non-success cases
cannot claim committed product bytes and must prove their outcome-specific
scratch cleanup. The corpus also publishes a derived aggregate workload; its
identity changes if any aggregate differs from its per-case measurements.

Operation receipts may supply counters already bound to a committed manifest.
An operator can add counters that the receipt does not expose, but cannot use a
receipt that ambiguously or weakly references the product.

## Fitted model and forecast

`fit_resource_envelopes()` uses deterministic nearest-rank quantiles, never
means. Each metric retains its sample count, minimum, selected high quantile,
maximum, high-quantile absolute residual, maximum absolute residual, maximum
positive residual, evidence basis, and extrapolation limit. Upper campaign
bounds use the selected high quantile plus the worst observed positive
residual. Verification duration instead uses the slowest observed verified
read throughput. These choices keep the model inspectable while preventing an
observed tail case from falling outside the frozen envelope.

The forecast rereads every final-support verification shard and counts every
executable member. It retains:

- logical, observed, synthetic, physical-output, candidate, and Poisson work;
- lower and upper output and verification-read bytes;
- per-worker RSS and scratch peaks;
- output inode, Temporal-history, and checkpoint upper bounds;
- verification, campaign wall-clock, and CPU ranges; and
- write and candidate amplification ceilings.

Average events per final product may not exceed the largest measured product
by more than the declared extrapolation factor. A changed support map cannot
reuse the forecast because the support identity participates in its
content-addressed ID.

## Packing and admission policy

Admission subtracts the configured recovery and verification reserve before
checking memory, scratch, output bytes, inodes, and elapsed-time capacity. It
then freezes worker concurrency, maximum immutable-container bytes, products
per shard, and the measured write/candidate amplification ceilings. Any failed
capacity check produces explicit refusal reasons; a qualified final audit
cannot contain a refused policy.

Small products do not automatically change logical granularity. The packing
review retains per-product publication unless independent
partition-sensitivity evidence is supplied. A bounded-container recommendation
still requires per-window identity and replay, atomic publication, bounded
corruption scope, indexed lookup, and non-duplicated observed anchors.

## Mounted-storage evidence

`ReconstructionStorageQualificationV1` requires two strong evidence
references. The measurement evidence is hash-bound to the filesystem and
device IDs, remounted identities, non-sparse byte count, measured write/read
throughput, sentinel hashes, same-filesystem result, remount result, and all
terminal cleanup checks. The disconnect evidence is independently bound to
the same volume and must state that the operation failed closed with no local
fallback.

The output and scratch roots must be distinct paths on the same filesystem and
must exactly match the frozen release candidate. The sustained non-sparse test
must be larger than the measured scratch peak. The audit rereads both evidence
files, the corpus, final support map, release candidate, and storage
qualification; it refits every envelope and recomputes the forecast, admission
policy, and packing decision. A byte, identity, or derived-field change fails
closed.

The library deliberately does not unmount storage. The operator performs the
disconnect and clean-remount drill under the campaign runbook and supplies the
resulting bounded JSON evidence as a strong `ArtifactRef`.

## Public command

Prepare a bounded JSON spec, then run:

```sh
histdatacom reconstruction --json resource-audit \
  --spec work/resource-audit-spec.json \
  --output-directory work/resource-audit
```

The spec schema is
`histdatacom.reconstruction-campaign-resource-audit-spec.v1` and contains:

```json
{
  "schema_version": "histdatacom.reconstruction-campaign-resource-audit-spec.v1",
  "final_support_map": "/absolute/work/final-support/index.json",
  "release_candidate": "/absolute/work/release/candidate.json",
  "storage_qualification": "/absolute/work/storage/qualification.json",
  "probes": [
    {
      "case_id": "modern-dense-high-missingness-member-01",
      "terminal_outcome": "success",
      "strata": {
        "era": "modern_dense",
        "missingness": "high",
        "alignment": "bounded_nearest",
        "deficit": "positive",
        "split": "deep_recursive",
        "member_scope": "all_retained"
      },
      "product_manifest": "/absolute/work/output/product/manifest.json",
      "operation_receipt": "/absolute/work/receipts/run.json",
      "telemetry": {
        "schema_version": "histdatacom.reconstruction-resource-runtime-telemetry.v1",
        "cpu_seconds": 12.5,
        "poisson_work_units": 900,
        "temporal_history_bytes": 32768,
        "checkpoint_bytes": 65536
      }
    }
  ],
  "capacity": {
    "available_memory_bytes": 68719476736,
    "available_scratch_bytes": 1099511627776,
    "available_output_bytes": 8796093022208,
    "available_inodes": 10000000,
    "maximum_campaign_seconds": 604800
  },
  "quantile": 0.95,
  "reserve_fraction": 0.25,
  "maximum_container_bytes": 4294967296,
  "maximum_products_per_container": 256,
  "partition_sensitivity_evidence_available": false
}
```

Receipt-derived values are loaded first and explicit telemetry values override
them. A probe without an operation receipt must provide the complete telemetry
contract. Refusal, cancellation, and failure probes omit `product_manifest`
and use their corresponding cleanup status.

The returned strong reference points to
`ReconstructionCampaignResourceAuditV1`. Keep the audit, measurement corpus,
storage qualification, disconnect/measurement evidence, final support map,
and release candidate together as campaign admission evidence. Re-run the
audit after any source range, member rectangle, compression/writer,
implementation, runtime, filesystem, capacity, or packing-policy change.
