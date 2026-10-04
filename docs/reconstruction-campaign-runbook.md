# Complete HistData triangle campaign runbook

This runbook is the supported operator path for constructing and publishing a
complete modern-reference reconstruction campaign. Its executable source
boundary is intentionally narrow: the immutable HistData.com ASCII/T caches
for `EURGBP`, `EURUSD`, and `GBPUSD`. Provider-neutral domain contracts remain
in the artifacts, but OANDA, alternate providers, live feeds, and
broker-conditioned delivery are later milestones.

The campaign is complete only when every half-open window in the frozen common
intersection has exactly one terminal support outcome and every executable
window has every retained-member product. A source-empty, expected-closure, or
scientifically unsupported interval is a real terminal outcome; it must never
be turned into invented liquidity. A refusal covering otherwise valid common
evidence is a defect to resolve before publication.

## Freeze the executable identity

Run from the exact installed version and source revision that will execute the
campaign. Record `histdatacom --version`, the Git commit, the dataset catalog
revision, experiment ID, powered qualification dossier ID, proposal engine,
configuration and fit IDs, and every plan-spec artifact digest. The qualified
v2 path selects
`histdatacom.marked-hawkes.diagonal_self_excitation`; motif-only translation or
another proposal engine is not an admissible fallback.

The `ReconstructionPlanSpecV2` must use `modern_reference`, the complete sorted
triangle, the exact frozen start/end bounds, and the selected powered evidence.
Keep the four storage bases explicit and non-overlapping:

- `artifact_root`: content-addressed control and evidence artifacts;
- `checkpoint_root`: durable manifests and status state;
- `output_root`: committed product transactions; and
- `scratch_root`: disposable per-window staging.

Plan-set construction preserves all four operator roots. Each shard receives a
stable `shards/<period-and-nanosecond-boundary>` child below its corresponding
base; shard output, checkpoint, or scratch data is never relocated below the
artifact tree.

## Qualify campaign storage

Output and scratch must be on the same mounted filesystem because publication
uses an atomic directory rename. Artifacts and checkpoints may live on another
durable filesystem. For a removable, network, or iSCSI volume, qualify the
mount before planning and again before run, status, resume, product indexing,
and dataset publication.

On macOS, this guard proves that a mounted campaign volume is not a fallback
directory on the boot filesystem:

```sh
CAMPAIGN_VOLUME=/Volumes/histdatacom-campaign
test -d "$CAMPAIGN_VOLUME"
mount | grep -F " on $CAMPAIGN_VOLUME "
test "$(stat -f %d "$CAMPAIGN_VOLUME")" != "$(stat -f %d /)"
mkdir -p "$CAMPAIGN_VOLUME/output" "$CAMPAIGN_VOLUME/scratch"
test "$(stat -f %d "$CAMPAIGN_VOLUME/output")" = \
  "$(stat -f %d "$CAMPAIGN_VOLUME/scratch")"
test -w "$CAMPAIGN_VOLUME/output"
test -w "$CAMPAIGN_VOLUME/scratch"
df -h "$CAMPAIGN_VOLUME"
```

Before the first real campaign, perform a sustained non-sparse write larger
than the measured peak scratch allocation, flush it, hash-read it twice, cleanly
unmount/remount, and verify the same hash. Then force one test disconnect while
a disposable bounded smoke is active. The expected result is an I/O failure or
paused/retryable activity, no directory recreated on the boot disk, and a
successful idempotent resume after remount. Preserve throughput, duration,
byte-count, digest, device identity, disconnect, remount, and resume evidence.
Do not begin or resume the complete campaign while the mount guard fails.

## Plan and prove complete support

Global reconstruction flags precede the subcommand. The file names below are
content-addressed outputs returned by the preceding operation:

```sh
histdatacom reconstruction --json compatibility \
  --plan full-range-plan-spec.json

histdatacom reconstruction --json plan-set \
  --spec full-range-plan-spec.json \
  --periods-per-shard 12

histdatacom reconstruction --json preflight-set \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json

histdatacom reconstruction --json support-map \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --output-directory work/support-map

histdatacom reconstruction --json support-verify \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --support-map work/support-map/reconstruction-plan-support-map-index-<sha256>.json \
  --release-candidate work/release/reconstruction-release-candidate-<sha256>.json \
  --output-directory work/final-support

histdatacom reconstruction --json support-inspect \
  --support-map work/support-map/reconstruction-plan-support-map-index-<sha256>.json \
  --limit 100
```

The plan set and support index must agree on plan-set identity, exact start/end
bounds, window size, shard count, source partition/event/byte totals, selected
proposal engine, and executable/empty/refused totals. Support shards must be in
strict order with no gap, overlap, or duplicate. Review every refusal reason
against source counts, triangle readiness, alignment mode, feed epoch,
session/closure state, and context/CFTC availability. The closure gate is zero
refused windows that contain valid common reconstruction evidence, not the
scientifically incorrect claim that every wall-clock interval contains
liquidity.

`support-verify` must complete before execution intent is bound. Its final
index rereads immutable Arrow rows, proves strict ownership and selected
alignment events, reconciles cardinality/resources, binds the frozen candidate,
and publishes the complete terminal census. See the
[final support verification contract](final-adaptive-support-verification.md).

## Measure and admit campaign resources

Run the representative success/refusal/cancellation/failure corpus and the
mounted-volume write, read, disconnect, and remount drills before creating the
request set. Keep their strong evidence references in the resource-audit spec,
then admit the exact independently verified support rectangle:

```sh
histdatacom reconstruction --json resource-audit \
  --spec work/resource-audit-spec.json \
  --output-directory work/resource-audit
```

Do not proceed unless the returned audit status is `qualified`. Review its
storage and inode forecast, wall-clock and CPU ranges, per-worker memory and
scratch peaks, amplification ceilings, reserve, concurrency, shard size,
packing decision, residuals, and extrapolation factor. A changed candidate,
support map, mounted device, runtime, writer/compression implementation, or
capacity requires a new audit. See the
[measured resource envelope contract](reconstruction-resource-envelopes.md).

## Bind intent and execute through Temporal

The request set binds the plan and support identities plus the information-mode
and scientific-nonclaim acknowledgement. The default allows declared terminal
refusals so no-op shards remain part of the gap-free campaign. Use
`--disallow-refusals` only when the frozen support contract genuinely requires
zero refusal outcomes of any kind.

```sh
histdatacom reconstruction --json request-set \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --support-map work/final-support/final-adaptive-support-map-index-<sha256>.json \
  --information-mode ex_post_reconstruction \
  --acknowledge-scientific-nonclaim \
  --output-directory work/request-set

histdatacom reconstruction --start-runtime --json run-set \
  --request-set work/request-set/reconstruction-plan-set-execution-request-<sha256>.json \
  --submit-only \
  --output-directory work/submitted

histdatacom reconstruction --json status-set \
  --receipt-index work/submitted/reconstruction-plan-set-receipt-index-<sha256>.json \
  --output-directory work/status
```

Production execution uses only the installed Temporal runtime and seven
first-party handlers. `--local` is for bounded parity/recovery tests, not a
fallback for a failed Temporal campaign. Keep the request set and every receipt
index: they are the durable control surface for status, cancellation, and
resume.

```sh
histdatacom reconstruction --start-runtime --json cancel-set \
  --receipt-index work/status/reconstruction-plan-set-receipt-index-<sha256>.json \
  --reason "operator-requested bounded recovery test" \
  --output-directory work/cancelled

histdatacom reconstruction --start-runtime --json resume-set \
  --receipt-index work/cancelled/reconstruction-plan-set-receipt-index-<sha256>.json \
  --submit-only \
  --output-directory work/resumed
```

For the crash/restart gate, terminate a worker only after recording its runtime
status and active window IDs, restart the same workspace runtime, and resume
from the latest receipt index. Reconcile the pre-crash and post-resume reports:
already committed window/member products must retain the same identities,
uncommitted scratch may be rebuilt, and no window/member may be absent or
duplicated.

## Reconcile products and publish the dataset

For in-progress exploration, use `product-inventory`. Its separate inventory
schema is always `unverified`, with publication and certification ineligible.
The old `product-index --manifest-only` spelling is a deprecated alias for that
inventory, **not** an index builder. Inventory V1 refuses rather than truncates
more than 5,000 candidates in total, 100,000 tree entries per output root,
16 directory levels, or 16 MiB of canonical metadata. Candidate metadata is
reserved before accumulation; these are finite work/wire limits, not a measured
whole-call memory guarantee. Select smaller plan sets for exploration.
Its stable no-follow manifest reads currently require POSIX support; unsupported
platforms refuse explicitly instead of implying equivalent protection.

`product-index` has no verification bypass. It replays committed Parquet,
observed anchors, source/plan/scenario bindings, and final native validation
against the exact support/member rectangle. Missing products make the index
incomplete. Empty/refused support stays explicit; out-of-plan products are
reported separately rather than admitted. A same-run product containing actual
events in planned empty/refused support is refused, not hidden in that report.

```sh
histdatacom reconstruction --json product-inventory \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --output-directory work/product-inventory

histdatacom reconstruction --json product-index \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --support-map work/support-map/reconstruction-plan-support-map-index-<sha256>.json \
  --output-directory work/product-index

histdatacom reconstruction --json product-inspect \
  --product-index work/product-index/reconstruction-campaign-product-index-<sha256>.json \
  --limit 100

histdatacom reconstruction --json product-verify \
  --product-index work/product-index/reconstruction-campaign-product-index-<sha256>.json

histdatacom reconstruction --json dataset-publish \
  --product-index work/product-index/reconstruction-campaign-product-index-<sha256>.json \
  --output-directory work/dataset
```

`product-inspect` is a **structural** read: actual shard identities/counts are
checked, but its serialized output explicitly says product bytes were not
verified. `product-verify` is a fresh deep read. Durable V1 index/manifest labels
alone are not current authority. Constructing or loading a deep receipt does
not grant authority either: this implementation always replays inputs afresh
and does not implement the reusable receipt-tree route under #523.

Older native product writers could hash caller-ordered evidence IDs and then
retain only sorted, deduplicated IDs. That lost ordering cannot be inferred
from the stored product. Fresh verification refuses such unreproducible quality
commitments with republishing guidance; existing structural readers remain
compatible. Reissue affected products through the corrected writer with their
actual validation/evidence inputs in a new output location. Do not edit old
hashes or overwrite retained evidence to make it appear verified.

Publication requires a freshly deep-verified complete product index and retains
the replay evidence in the dataset version's qualification evidence. Changing
any referenced bytes invalidates a subsequent verification/publication, even
if an older receipt says complete. It preserves explicit terminal
non-product outcomes and emits one provider-neutral synthetic dataset version;
it does not relabel the output as HistData observations or broker data. Use
`outputs`, `preview`, and `replay` for bounded per-request/product inspection.

These are product-integrity checks, not a rerun of model training or scientific
promotion qualification and not evidence that a historical campaign ran.
Final-local checks reproduce the native terminal invariants and retained
lineage; they do not prove a unique stochastic price path or recreate discarded
pre-cross-currency candidate quotes. Cross-currency validation is recomputed
from actual output and source anchors. A different plausible price path cannot
be ruled out merely by passing those terminal constraints.
Artifact trees must remain quiescent during verification/publication; bounded
reads and before/after byte checks are not a hostile-filesystem transaction.
The fresh verifier refuses control documents over 64 MiB, aggregate retained
plan controls over 256 MiB, more than 262,144 planned coordinates/tracked files,
or an over-limit discovered artifact tree. Native shard limits still apply.
Canonical receipt content is bounded to 4 MiB with at most 4,096 separately reported
out-of-plan products. Refusal is explicit, never a truncated complete claim;
larger campaigns require a separately designed scalable proof path.

## Closure evidence

Retain a machine-readable closeout dossier that proves:

- exact version, commit, experiment, qualification, engine/config/fit, plan,
  support-map, request-set, receipt-index, product-index, and dataset IDs;
- complete temporal cardinality with zero gaps, overlaps, or duplicates;
- every executable window/member committed and replay-verified;
- every empty/closed/refused outcome is explicit and scientifically justified;
- observed anchors reconcile byte-for-byte and synthetic rows retain complete
  origin, constraint, uncertainty, and neighboring-anchor lineage;
- full aggregate information, triangle, point-process, mark, spread, activity,
  session/epoch, context, negative-control, bar, and strategy-sensitivity
  audits;
- measured memory, scratch, output, runtime, amplification, storage, Wi-Fi/iSCSI
  integrity, and crash/resume behavior within the declared envelope; and
- the exact release artifact passes full hooks/tests, real integration,
  isolated installs, local-simple-registry TestPyPI preflight, dev-to-main
  coverage, TestPyPI, and PyPI promotion.

The nonclaims remain part of the product: this is not recovered historical
truth, broker-conditioned data, centralized FX volume, an investment
recommendation, or an automatically selected model winner.
