# Reconstruction Certification Contracts

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
| `information-safety` | Market-context and CFTC corpora are valid; ex-post and ex-ante uses have distinct zero-violation point-in-time audits. |
| `reverse-degradation` | The benchmark corpus predates candidate results, thresholds are predeclared, blocked holdouts pass, and negative controls fail as expected. |
| `conditioned-scorecards` | Feed epochs and observation operators are valid; selected engines and frozen weights come from the powered qualification dossier; all required strata exist and product/benchmark tolerances pass. |
| `cross-currency` | Triangle, inverse, synchronization, and stale-alignment checks pass before and after identity delivery. |
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
result is a scalar, and only then creates a certification observation.

This prevents a handwritten summary boolean from standing in for a producer
report. Producer-specific contracts still own how reports calculate their
metrics; the campaign owns identity, extraction, aggregation, and publication.

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
9. During explicit `dev`-to-`main` promotion, run coverage exactly once,
   publish the final `certified` dossier, and publish the same artifact to PyPI.

Fixture dossiers and campaign tests prove contract, extraction, comparison,
serialization, and publication semantics. They cannot certify historical
output or replace a real reconstruction campaign.
