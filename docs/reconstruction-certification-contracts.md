# Reconstruction Certification Contracts

The version-one certification layer is the final fail-closed boundary for the
EURUSD/GBPUSD/EURGBP reconstruction product. It aggregates compact evidence
from the implemented reconstruction modules; it does not rerun those modules
inside a workflow payload and does not retain tick rows, analytical frames,
model objects, candidate batches, or rejected events.

Certification is evidence aggregation, not a visual judgment that output
"looks plausible." Every measured claim is evaluated against a requirement
that was content-addressed before the observation was supplied.

## Fixed product scope

`ReconstructionCertificationPolicyV1` fixes:

- product version `2.1.0`;
- the exact EURGBP/EURUSD/GBPUSD instrument set;
- common source support beginning at `200203`;
- the explicitly selected broker-delivery fingerprint identity;
- resource ceilings for peak memory, scratch space, runtime, and final
  storage;
- all scientific, operational, reporting, repository, and release checks; and
- the rule that coverage runs once at the `dev`-to-`main` promotion boundary.

The common end month remains an explicit policy input because the source
inventory advances over time. Changing the broker identity, support interval,
threshold, budget, or required artifact changes the deterministic policy ID.

`eurusd_triangle_certification_policy()` creates the complete policy. A partial
policy is invalid: every issue #449 gate must have at least one requirement,
and check identifiers must be globally unique.

## Evidence pipeline

The pipeline has four bounded layers:

1. `CertificationArtifactV1` records the predeclared policy identity plus a
   verified artifact's kind, subject identity, schema version, content SHA-256,
   safe relative path, size, and bounded metadata. Evidence produced under a
   different policy cannot be repurposed after thresholds or scope change.
2. `CertificationObservationV1` records one scalar measurement and the exact
   artifact evidence identities that support it.
3. `CertificationCheckResultV1` applies the policy comparator to the observed
   and expected values. Required artifact kinds must be present and verified.
4. `CertificationGateResultV1` and
   `ReconstructionCertificationDossierV1` aggregate checks without weakening a
   failure or missing-evidence result.

`CertificationArtifactV1.from_payload()` binds a compact JSON contract to the
predeclared policy and hashes that contract. It is an adapter for existing
manifests and reports, not a substitute for their own validation. Callers must
load and verify the original contract before marking the certification artifact
as verified.

The selected broker artifact receives an extra identity check. A valid
fingerprint for another broker, server, account, or version cannot satisfy the
policy merely because its artifact kind is correct.

## Gate mapping

| Gate | Required evidence and outcome |
| --- | --- |
| `identity-and-anchors` | Raw-source inventory and final product manifests report zero source-hash or immutable-anchor mismatches. |
| `information-safety` | Every claimed use has an accepted information audit with zero leakage violations. |
| `reverse-degradation` | Thresholds were predeclared and untouched final holdouts have zero failures. |
| `conditioned-scorecards` | Required epoch/session/event strata are present and cadence, spread, timing, and path tolerances pass. |
| `cross-currency` | Post-broker triangle, inverse, and stale-join validation has zero failures and is bound to the selected broker fingerprint. |
| `ensemble-evidence` | Calibration, diversity, refusal, and unsupported-region rates are reported. |
| `product-reconciliation` | Final events, activity/volume semantics, and derived bars reconcile. |
| `failure-resume` | Injected mid-run failure resumes with zero duplicate or missing partitions. |
| `replay` | Clean replay has zero logical-content-hash mismatches. |
| `resources` | Measured peak memory, scratch, runtime, and final storage remain under predeclared ceilings. |
| `negative-tests` | Corruption, stale broker profile, unhealthy clock, missing context, and partial synchronized group all refuse. |
| `strategy-sensitivity` | Uncertainty is reported and automatic-winner selection remains false. |
| `dossier-publication` | Human methodology/limitations and the machine evidence manifest are published. |
| `repository-gates` | Full plain tests and hooks pass; coverage count is exactly one at promotion. |
| `testpypi-preflight` | The local simple-registry TestPyPI preflight passes before release promotion. |

Comparators are deliberately small and reviewable: exact equality,
less-than-or-equal, greater-than-or-equal, true, false, and zero. Booleans are
not accepted as numbers, nonfinite numbers are rejected, and exact equality
requires matching scalar types.

## Overall states

The dossier has four possible states:

- `incomplete`: required evidence is missing or a blocking limitation remains;
- `failed`: a measured value violates its predeclared requirement;
- `ready-for-promotion`: every check except the single promotion-boundary
  coverage observation passes;
- `certified`: every scientific, operational, repository, coverage, and
  TestPyPI-preflight check passes with no blocking limitation.

Missing evidence is never treated as a measured failure or a pass, and a known
measured failure outranks a simultaneous blocking limitation. This keeps
external prerequisites—most importantly a qualified live broker fingerprint
and actual reconstruction artifacts—visible. An accepted limitation may
describe scope or uncertainty but cannot replace evidence that is necessary
for the product claim. Contradictory limitations belong in
`blocking_limitations` and force `incomplete`.

Coverage does not run during ordinary issue implementation. If all other
evidence, including the local simple-registry TestPyPI preflight, passes, the
dossier becomes `ready-for-promotion`. The one coverage observation is added
only while moving `dev` to `main`; a second coverage run fails the exact-count
requirement.

## Publication and replay

`write_reconstruction_certification_dossier()` atomically writes:

- canonical JSON containing the machine-readable evidence manifest; and
- deterministic Markdown containing product scope, gate status, methodology,
  accepted limitations, blocking limitations, and the trust boundary.

Both outputs receive strong `ArtifactRef` values with size and SHA-256.
Machine JSON is immediately read back through
`ReconstructionCertificationDossierV1.from_json()` before publication returns.
The dossier ID covers the policy, artifacts, results, methodology, limitations,
state, and fixed trust claims.

The report always states:

- event rows are not inline;
- analytical-frame columns are not inline;
- no automatic winner is selected;
- no historical-truth claim is made;
- no investment recommendation is made; and
- release authorization is true only for `certified`.

## Required execution sequence

The release-grade run should proceed in this order:

1. Freeze the three-symbol source inventory and content hashes.
2. Select and verify one eligible, versioned broker fingerprint.
3. Freeze the policy, scientific thresholds, and resource budgets.
4. Execute ex-post reconstruction and each separately supported ex-ante view.
5. Produce final-holdout, conditioned, cross-currency, ensemble, product,
   activity, bar, strategy, fault-injection, replay, resource, and negative-test
   artifacts.
6. Run the full plain suite and repository hooks without coverage.
7. Run the TestPyPI preflight through the local simple registry.
8. Evaluate and publish the `ready-for-promotion` dossier.
9. During the explicit `dev`-to-`main` promotion, run coverage exactly once and
   publish the final `certified` dossier.

Fixture dossiers prove contract, comparison, serialization, resource-bound,
and fail-closed behavior. They cannot certify historical output or stand in for
the qualified broker and actual three-instrument reconstruction evidence.
