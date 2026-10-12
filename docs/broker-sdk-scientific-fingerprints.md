# SDK-native scientific fingerprints

Unreleased v3.0.0 work for #764 and #614. This is a synthetic-only software path,
not a real-provider admission or empirical certification. No broker acquisition
is enabled by fitting a retained capture.

## Evidence and identity

`fit_broker_delivery_fingerprint()` accepts exact SDK lifecycle manifests and
their `BrokerSDKInvocationV1` requests. Native inputs produce an explicit
`BrokerDeliveryFingerprintV3`; historical capture inputs retain their existing
V1/V2 contracts. V3 does not fabricate a legacy session, server, account, source
batch, or provider protocol to call the historical fitter.

Each native parent retains the lifecycle manifest, exact SDK invocation,
independently replayed host-health audit, provenance header and seal, and the
sealed compatibility evidence when available. Historical environment V1 records
do not acquire invented compatibility evidence. The new scientific-lineage V2
sidecar retains V3 parents alongside derived publications; historical
scientific-lineage V1 keeps its exact legacy V2-parent meaning.

SDK compatibility is governed independently by
[the compatibility policy](broker-plugin-compatibility.md). Parsing an older
artifact, making a statistical projection, proving semantic equivalence, and
qualifying an empirical provider are different claims. A successful fit makes
only the declared bounded statistical projection claim.

## Supported measurements and eligibility

Fitting is bounded by an explicit `BrokerLifecycleFingerprintFitConfigV1` and
uses streaming native replay. It requires complete, stopped, reaped capture
evidence without lifecycle errors/loss and independently qualified host health.
Unknown upstream loss remains unknown; the retained health policy determines
whether that uncertainty is admissible at the host boundary.

Quotes supply lexical price precision and spread measurements. Cadence uses
host monotonic receive time; support intervals use host UTC receive time.
Missing source timestamps and source-batch semantics remain unsupported, not
host timestamps relabeled as broker time. Heterogeneous source timestamp
semantics cannot silently become one precision estimate. Calendar, session,
market-context, server and account facts are not inferred.

## Current rights and actual sources

Metadata, retained grants, a conformance report, and source paths grant no
current provider rights. Fitting and every material consumer require a current
provider-policy scope. Fingerprint reuse requires explicit native source roots
through `broker_fingerprint_sources(...)`, plus any consumer's required native
input scope. The source resolver accepts an exact native directory or its exact
canonical header-identity child; it does not search recursively or choose an
ambiguous match.

Reuse replays the actual source, reconciles its exact manifest, health and
anchored provenance, and recomputes the projection. Missing sources, tampering,
incompatible identities, denied/expired rights, unsupported evidence, or
declared resource-limit violations refuse material use. Plugin-free replay
needs retained native evidence and current rights, not an installed plugin or a
registry lookup. An offline replay does not authorize fresh live capture.

## Public interfaces

The public `histdatacom.broker_capture` API exports the V3 contracts, the fit,
eligibility, parse, read/write and comparison interfaces, and explicit source
scope. `BrokerQualifiedDeliveryFingerprint` denotes the qualified V2/V3 union.
Proposal conditioning preserves the actual fingerprint identity and can change
only supported measurements. Full broker rendering refuses native profiles
without source-batch or timestamp semantics; it does not substitute the legacy
renderer defaults. Scientific publications retain their exact native parents
and require provider-policy receipts. A valid proposal does not by itself
qualify a rendered product, derived bars, or a training publication.

The offline CLI is available as:

```console
python -m histdatacom.broker_capture.scientific_cli inspect FINGERPRINT.json
python -m histdatacom.broker_capture.scientific_cli fit NATIVE_DIRECTORY \
  --invocation INVOCATION.json --policy-context CURRENT_POLICY.json \
  --output FINGERPRINT.json
python -m histdatacom.broker_capture.scientific_cli verify FINGERPRINT.json \
  --source NATIVE_DIRECTORY --policy-context CURRENT_POLICY.json
```

`inspect` is metadata-only and reports that material admission was not checked.
`fit` and `verify` reread the supplied current policy at policy boundaries;
neither imports a plugin nor acquires provider data. JSON inputs must satisfy
the exact public contracts. `fit --config` selects an explicit bounded policy.

## Qualification boundaries

Acceptance tests use generated first-party and separately distributed plugin
fixtures. The installed-wheel acceptance driver checks actual native dispatch,
deterministic refit, an independent numerical oracle, nonzero proposal
conditioning, current-rights denials and retained consumption after plugin
uninstall. Driver implementation alone is not a passing installed run. These
checks do not constitute real-provider qualification or the all-family
historical golden corpus tracked in #635. Real-provider admission remains with
#488/#601.
