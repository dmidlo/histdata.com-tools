# Reconstruction Certification Contracts

## Matrix-bound successor and current evidence

The [current capability matrix](capability-matrix.md) distinguishes implemented
software from retained execution and independent verification. It does **not**
establish a complete 2002-to-cutoff campaign or certified dataset. The missing
certification-grade index, deep-verification root and era audit (#522–#524)
remain blockers; this governance implementation does not execute those programs.

`CapabilityCertificationSpecV1` freezes the complete matrix, its exact policy
and concrete evidence declarations. `CapabilityCertificationDossierV1` embeds
that specification and the freshly derived independent verification receipt.
The policy and matrix identities include the versioned catalog's semantic
membership, dependencies and release-critical requirements. Full v2.5 claims
cannot omit those requirements or substitute software-scoped evidence for
complete evidence. Explicitly exercised waivers always produce a limited
outcome, never the full-campaign label.

The separately versioned public route is:

```sh
histdatacom reconstruction --json certify-capabilities \
  --spec capability-certification-spec.json \
  --evidence-root /absolute/canonical/evidence-root \
  --output-directory retained-capability-dossier
```

The equivalent Python method is
`ReconstructionClient.certify_capabilities(spec_path, evidence_root=...,
output_directory=...)`. Both run the same fresh native checks, write the exact
specification and dossier JSON plus readable Markdown, preserve conflicting
existing outputs, and report `blocked` with refusal exit code 3 or a verified
exact claim with exit code 0. `verified_limited_claim` does not certify the
complete campaign or authorize publishing a package. These routes do not
publish packages, access protected holdouts or execute an entire campaign.

Evidence preparation uses `verify_capability_execution()` from
`histdatacom.synthetic.capability_verification`. It derives a release identity
from the current verifier implementation, immutable dataset-version identity,
closed profile versions and exact declared input graph. Its per-row execution
and independent-verification IDs can populate a truthful matrix. The final
certification call repeats verification; it never accepts the preparation
receipt, a caller callback, a `verified=True` flag, or recorded `executed_passed`
as authority. Relative evidence paths are rooted explicitly, byte-bounded and
hash-checked, including declared nested inputs and post-verification drift.

Each row's `implementation_commit` is declared Git attribution, not a Git
attestation produced by the native verifier. The current snapshot's source
anchors were separately inspected in Git. Fresh verification binds actual
package source bytes through `implementation_id`; it does not establish that
an arbitrary caller-supplied commit label contains those bytes.

Initial closed profiles cover actual reference-kernel recomputation and current
model-registry comparison at **software** scope, and native source-experiment
replay at **bounded** scope. Powered-eligibility, selection and publication
profiles are not admitted by this initial consumer: their real producer
qualification must precede adding successful verification paths. Existing
mocked planning controls or eligibility scalars are not substitute fixtures.
The admitted profiles do not supply complete-campaign, untouched-holdout,
external process-history or era-audit evidence. Missing profiles or evidence
block the relevant claim.
Thus the current implementation cannot honestly produce a full-campaign pass.

Reading a retained dossier only checks its structure and content identity.
Use `verify_capability_certification_dossier(dossier, evidence_root=...)` before
relying on it: this repeats native checks and requires the same complete outcome.
The matrix JSON is not a substitute for those retained execution inputs.

## Historical scalar-aggregation contracts

The native product checks in `certify-campaign` require an exact, freshly
reverified receipt-tree root in addition to the native index. Declare an artifact
of kind `campaign-verification-root`, with its root JSON path, byte SHA-256,
`/artifact_id` subject pointer and `histdatacom.campaign-receipt-root.v1` schema.
Include that evidence key in each protected product observation. The current
policy factory requires that evidence kind explicitly; retained older policies
keep their original wire contents and identities. Publication observations also
include the native dataset-publication evidence key: its dataset version must
bind exactly that root and the runner freshly checks the same graph.
An index alone, a sampled receipt,
caller scalars or a structurally valid historical root cannot pass these native
checks. This does not add a complete-campaign profile to the capability-bound
successor or turn synthetic fixtures into release evidence.

The V2 contracts and `certify` command below retain their original wire IDs,
readers and behavior for historical replay. Their scalar aggregation is not the
matrix-bound successor: a self-consistent report can carry caller-supplied
measurements. A V2 `certified` label alone therefore does not satisfy current
capability evidence or authorize a new complete-campaign release.

Certification is the fail-closed evidence boundary for the
EURUSD/GBPUSD/EURGBP reconstruction product. It aggregates compact reports; it
does not retain tick rows, analytical frames, model objects, candidate batches,
or rejected events and it does not certify output because it looks plausible.

## Current and legacy policy versions

`ReconstructionCertificationPolicyV2` is the modern-reference policy schema.
The current factory predeclares the issue #498 release contract. It
fixes:

- product version `2.5.0` (while embedded earlier V2 policies remain readable);
- the exact EURGBP/EURUSD/GBPUSD instrument set and ASCII tick scope;
- common source support beginning at `200203` and an execution-time end month;
- delivery mode `modern_reference` and claim `unconditioned_reference`;
- source-readiness, scientific, operational, reporting, repository, and release
  checks;
- the exact passing reconstruction math-verification report identity;
- explicit resource and candidate-amplification ceilings; and
- coverage exactly once at the `dev`-to-`main` promotion boundary.

Broker adaptation is excluded from V2. A V2 policy cannot contain a
broker-named check or required artifact, the evaluator rejects broker-named
evidence, and the dossier always records `broker_specific_claim: false`.
Broker capture, fingerprinting, and transfer remain optional, separately
qualified extensions.

`ReconstructionCertificationPolicyV1` is retained unchanged for replay of
previously published evidence. It requires a broker fingerprint and must not be
used for the modern-reference #449 campaign. V2 was introduced rather than
changing V1 in place because removing a broker field or changing evidence kinds
would silently change the meaning of old policy identities.

## Gate mapping

The V2 factory `modern_reference_triangle_certification_policy()` binds every
live #498 seam while retaining #449/#491 as predecessor evidence:

| Gate group | Required evidence and outcome |
| --- | --- |
| `identity-and-anchors` | Inventory readability, dimensions and hashes reconcile; the support map is gap-free; no duplicate dimension, raw-hash mismatch, observed-anchor change, missing synthetic lineage, valid-common-data refusal, or unclassified terminal outcome exists. |
| `information-safety` | The current scientific ledger, passing math-verification report, and plan/product/dataset lineage binding are valid; every market-context and CFTC input has an explicit completeness/information-mode state; generated origin is not misclassified; every ex-post product retains `invalid-for-backtest`; and ex-post/ex-ante uses have distinct zero-violation point-in-time audits. |
| `reverse-degradation` | The benchmark corpus predates candidate results, thresholds are predeclared, blocked holdouts pass, and negative controls fail as expected. |
| `conditioned-scorecards` | Feed epochs and observation operators are valid; eligibility and weights come from powered qualification, the marked-Hawkes product choice comes from its validation-only selection dossier, observation uncertainty has validation and untouched-holdout evidence, all required strata exist, and product/benchmark tolerances pass. |
| `cross-currency` | Triangle, inverse, synchronization, and stale-alignment checks pass before and after identity delivery; projection-burden reports and exact consumer receipts cover model selection, products, campaign shards, era audits, and certification; no excessive burden, hidden final-residual-only pass, or surviving synthetic residual is admitted. |
| `ensemble-evidence` | Calibration, diversity, refusal, unsupported-region, between-seed, and between-window uncertainty are reported. |
| `product-reconciliation` | Final ticks, activity, and bars reconcile; the nonclaim is published; full-range preflight and execution pass; every executable retained-member product exists; empty/closed/unsupported windows contain no invented liquidity; the complete product index and provider-neutral dataset publication verify; representative windows and CLI/API evidence-chain parity pass. |
| `failure-resume` | Mid-run failure and qualified storage disconnect resume with no missing/duplicate partition; cancellation publishes no partial partition. |
| `replay` | Logical product hashes agree across clean replay and supported concurrency. |
| `resources` | Peak memory, scratch, runtime, candidate amplification, storage, final-row evidence, and mounted-storage write/read/hash/remount/no-fallback qualification meet frozen bounds. |
| `negative-tests` | Corruption, stale artifacts, missing context, invalid information mode, quota overflow, and partial groups fail closed. |
| `strategy-sensitivity` | Uncertainty is reported and no automatic winner is selected. |
| `dossier-publication` | Human methodology/limitations, the machine evidence manifest, and all twelve coherent diagnostic families are published. |
| `repository-gates` | Test dependencies are installed, the full plain suite and hooks pass, and promotion coverage runs exactly once. |
| `testpypi-preflight` | The local simple-registry TestPyPI preflight passes before promotion. |

Changing the source range, evidence contract, threshold, or resource budget
changes the deterministic V2 policy ID.

The methodology and limitations are not free-floating dossier prose. The
campaign declares the same strong `reconstruction_scientific_ledger_v1`
artifact carried by the experiment, plan, product, and published dataset. See
[`reconstruction-scientific-ledger.md`](reconstruction-scientific-ledger.md).

## Evidence contracts

The bounded evaluator retains four layers:

1. `CertificationArtifactV1` records the frozen policy identity, artifact kind,
   subject identity and schema, content SHA-256, safe relative path, size,
   verification state, and bounded metadata.
2. `CertificationObservationV1` records one scalar measurement and the exact
   artifact evidence identities that support it.
3. `CertificationCheckResultV1` applies one small comparator: equality,
   less-than-or-equal, greater-than-or-equal, true, false, or zero.
4. `CertificationGateResultV1` and
   `ReconstructionCertificationDossierV2` aggregate results without weakening a
   failure or missing observation.

V2 requires the evidence-kind set for a check to match its policy exactly.
Unverified, foreign-policy, missing, extra, or broker-named evidence cannot
support a pass. Booleans are not numbers, nonfinite numbers are rejected, and
exact equality requires matching scalar types.

## Executable campaign

`ModernReferenceCertificationCampaignSpecV1` freezes policy budgets, artifacts,
scalar-extraction locations, methodology, limitations, and whether execution is
the explicit promotion boundary. Every artifact declaration supplies:

- a campaign-local evidence key;
- the producer artifact kind and path;
- the expected file SHA-256;
- the expected subject schema version;
- the expected subject identity and JSON pointer that contains it; and
- a safe dossier-relative path.

Every observation supplies a measurement evidence key, a JSON pointer, and the
exact supporting evidence keys. It does not contain an `actual` value.
`run_modern_reference_certification_campaign()` reads the file, verifies its
hash, schema and subject identity, resolves the JSON pointer, verifies that the
result is a scalar, and only then creates an ordinary certification observation.

Four product checks are an explicit exception: `campaign_product_index_valid`,
`campaign_dataset_publication_valid`,
`executable_retained_product_missing_count`, and
`fabricated_liquidity_terminal_outcome_count`. Their values come from fresh
native campaign replay, never their declared JSON scalar pointers. Native
index/publication artifacts are verified even if no protected observation
references them. Publication/index/support identities and bytes must describe
one graph; an unrelated valid index cannot certify another publication or
support map. An unverified inventory or resealed passing summary is rejected.
Publication checks also reconstruct the exact normalization, parent role/order,
scientific ledger and closed qualification-evidence graph. Material use requires
the retained deep receipt to equal current replay. Legacy publication envelopes
without that receipt remain structurally readable, but must be republished with
fresh evidence before certification; decoding alone is not qualification.

Hash checking alone proves the declared bytes, not the truth of a report.
Producer-specific contracts still own how ordinary reports calculate their
metrics; the campaign owns identity, extraction, aggregation, and publication.
The protected product path independently checks actual retained products and
native validation, rather than granting authority to a handwritten boolean.
The projection-burden producer contract is described in
[`projection-burden-diagnostics.md`](projection-burden-diagnostics.md); its
report and release-consumption receipts are mandatory cross-currency evidence.

Run the installed public surface with:

```sh
histdatacom reconstruction --json certify \
  --spec evidence/campaign.json \
  --output-directory evidence/dossier
```

The campaign automatically publishes and binds its frozen machine manifest and
methodology report. A normal `dev` campaign rejects
`coverage_promotion_run_count`; only a spec explicitly marked as a promotion
boundary can carry that observation.

## States and exit behavior

A V2 dossier has four states:

- `incomplete`: required evidence is missing or a blocking limitation remains;
- `failed`: a measured value violates policy;
- `ready-for-promotion`: every check except promotion-only coverage passes; and
- `certified`: every scientific, product, repository, coverage, and TestPyPI
  check passes with no blocking limitation.

A measured failure outranks missing work. A known limitation can narrow a claim
but cannot replace immutable-anchor, information-safety, benchmark,
operational, or release evidence.

The CLI returns `0` for `ready-for-promotion` or `certified`, `3` for an
incomplete campaign, and `5` for measured certification failure. Malformed or
changed campaign inputs return the existing invalid-plan category.

## Publication and replay

`write_modern_reference_reconstruction_certification_dossier()` atomically
writes canonical JSON and deterministic Markdown, immediately reads the JSON
back through `ReconstructionCertificationDossierV2`, and returns strong
`ArtifactRef` values. Campaign execution also writes:

- `evidence/campaign-spec.json`;
- `evidence/methodology.json`; and
- `campaign-result.json`.

The dossier identity covers policy, artifacts, results, methodology,
limitations, state, delivery claim, and fixed trust assertions. It always
states that event rows and analytical-frame columns are not inline, no broker
claim or historical-truth claim is made, no automatic winner is selected, and
no investment recommendation is made.

## Required release sequence

1. Re-inventory and hash the complete three-symbol source scope.
2. Freeze V2 policy, scientific thresholds, and resource budgets.
3. Verify all dependency artifacts and point-in-time coverage.
4. Execute ex-post reconstruction and each separately supported ex-ante view.
5. Produce real holdout, conditioned, cross-currency, ensemble, product,
   activity, bar, strategy, fault, replay, resource, negative-test, and public
   interface reports.
6. Run the full plain suite and repository hooks without coverage.
7. Publish to TestPyPI from `dev` and pass the local simple-registry preflight.
8. Execute the campaign and publish a `ready-for-promotion` dossier.
9. During explicit `dev`-to-`main` promotion, run coverage exactly once. Bind
   the complete current matrix and independently replay its concrete evidence
   through the matrix-bound successor before relying on a final dossier. A
   historical V2 scalar label is insufficient. Publish the same artifact to
   PyPI only after all required evidence and release gates actually pass.

Fixture dossiers and campaign tests prove contract, extraction, comparison,
serialization, and publication semantics. They cannot certify historical
output or replace a real reconstruction campaign.
