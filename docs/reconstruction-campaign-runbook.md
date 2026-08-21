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

An in-progress diagnostic index may use `--manifest-only`; it is not a release
artifact. Final indexing omits that flag and integrity-replays every committed
Parquet product against the support map and retained-member rectangle.

```sh
histdatacom reconstruction --json product-index \
  --plan-set work/artifacts/reconstruction-plan-set-<sha256>.json \
  --support-map work/support-map/reconstruction-plan-support-map-index-<sha256>.json \
  --output-directory work/product-index

histdatacom reconstruction --json product-inspect \
  --product-index work/product-index/reconstruction-campaign-product-index-<sha256>.json \
  --limit 100

histdatacom reconstruction --json dataset-publish \
  --product-index work/product-index/reconstruction-campaign-product-index-<sha256>.json \
  --output-directory work/dataset
```

Publication requires a complete product index. It preserves explicit terminal
non-product outcomes and emits one provider-neutral synthetic dataset version;
it does not relabel the output as HistData observations or broker data. Use
`outputs`, `preview`, and `replay` for bounded per-request/product inspection.

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
