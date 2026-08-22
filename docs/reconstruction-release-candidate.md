# Exact reconstruction release candidates

Phase-two qualification and the complete reconstruction campaign must use one
immutable installable candidate. A commit, a scientific graph, or a wheel by
itself is not a candidate. The executable contract is
`histdatacom.synthetic.release_candidate.ReconstructionReleaseCandidateV1`.
It binds all of those surfaces and produces one content-derived
`candidate_id`.

The release-holdout `ReleaseCandidateFreezeV1` remains the scientific-stage
graph: it proves fitting, preprocessing, tuning, selection, scenario policy,
and adaptive policy did not use the protected holdout. The reconstruction
release candidate wraps that graph with the exact source, build, installation,
storage, validation, and GitHub-governance identities needed to execute it.

## Frozen identity graph

The candidate contains strong, local artifact references and stable IDs for:

| Surface | Frozen evidence |
| --- | --- |
| Source control | repository URL, full Git object IDs for commit and tree, clean-tree proof, and ref name |
| Distribution | SemVer package version plus the exact wheel and sdist byte hashes built from that commit |
| Runtime | Python implementation/version/ABI, OS release, architecture, machine class, critical dependency versions, and compression versions |
| Schema and source | reconstruction schema-registry ID, complete campaign dataset catalog/revision, exact executable-partition SHA-256 values, and source cutoff |
| Scientific inputs | scientific training/qualification experiment, ledger, raw feed epoch, raw observation operator, market context, raw CFTC, benchmark, proposal evaluation, powered qualification, and product-selection artifacts |
| Selected product | selected engine ID, configuration, fit, observation-scenario registry, and the sealed candidate graph |
| Policies | adaptive window, alignment, carving, reconciliation, storage, and certification artifacts |
| Protected evidence | a fresh still-sealed release-holdout manifest used by the candidate graph |
| Storage | distinct absolute artifact, output, checkpoint, and scratch roots with machine-bound filesystem qualification receipts |
| Execution | permitted CLI commands and the exact seven first-party runtime handler IDs |
| Governance | protected branch/tag evidence, required promotion checks, and signed merge/tag policy |

Every dependency is part of the canonical payload. A one-byte artifact change,
new dependency ID, new path, environment version, Git tree, gate receipt, or
policy creates another `candidate_id`.

Dependency roles also require the artifact kinds consumed by an executable v2
plan. Certification-report wrappers cannot replace the raw CFTC corpus, feed
epoch definition, observation operator, benchmark manifest, proposal config,
or proposal fit. The `reconciliation_policy` dependency—not the policy-registry
`alignment_policy` entry—binds the plan's
`cross_series_constraint_policy` role.

The candidate's `experiment_manifest` is the protected scientific experiment
used by powered qualification. Each campaign shard may have a separate
historical-anchor/product-input experiment, so those two experiment references
are linked transitively through qualification rather than required to be the
same artifact. In contrast, `dataset_catalog` is the complete executable
campaign catalog. Its selected revision must contain exactly one
HistData.com ASCII/T partition for every EURGBP/EURUSD/GBPUSD period from the
first common archive month (`200203`) through the month immediately before the
UTC month-boundary `source_cutoff_ns`. Every partition must end at or before
that cutoff, and
`source_partition_hashes` must equal the catalog's
`symbol.lower():period -> partition.artifact.sha256` projection.

## Freeze procedure

Freeze only after scientific decisions are complete and before phase-two
qualification or the release holdout is opened.

1. Build the wheel and sdist from a clean full commit. Attach
   `git_commit_sha` and `package_version` metadata to both strong references.
2. Capture `ReleaseCandidateGitIdentityV1` with
   `inspect_release_candidate_git_identity()` and capture the runtime with
   `capture_release_candidate_runtime_identity()`.
3. Bind every required dependency with `ReleaseCandidateDependencyV1`. The
   schema registry must equal the installed CLI/API registry, each dependency
   must use its executable artifact kind, the powered-qualification experiment
   must agree, the campaign catalog and source hashes must agree, and both
   selected-engine artifacts must name the selected engine.
4. Bind four non-overlapping absolute roots using
   `ReleaseCandidateFilesystemRootV1`. Each root requires a byte-verified
   qualification receipt for writable, durable, atomic-replace storage on the
   declared machine class.
5. Bind every required validation receipt and protected-ref receipt, all to
   the same commit.
6. Call `freeze_reconstruction_release_candidate()`. It deeply verifies every
   local reference, reads the scientific graph and protected manifest, checks
   their linkage and timestamps, verifies the source cutoff, and confirms the
   installed seven-handler registry.
7. Persist with `write_reconstruction_release_candidate()`. The filename
   includes the SHA-256 of the canonical manifest bytes.

The freeze fails if the working tree is dirty, the package version differs
from the installed API, the registry is stale, an artifact kind or strong
reference differs, the campaign catalog is incomplete or exceeds the cutoff,
the holdout graph points to another manifest, a root overlaps, or a gate is
missing.

## Required validation gates

The candidate is complete only when it carries passing, commit-bound receipts
for all of these names:

- `full_pre_commit`, `typing`, and `docs_warnings_as_errors`;
- `full_test_suite`, `critical_branch_coverage`,
  `critical_property_invariants`, and `critical_mutation_testing`;
- `wheel_sdist_build` and `build_metadata`;
- `isolated_install_linux`, `isolated_install_macos`, and
  `isolated_install_windows`;
- `temporal_extra_install` and `seven_stage_registration`;
- `local_simple_registry`;
- `cli_api_schema_discovery`; and
- `path_independence`.

The existing CI build and cross-platform wheel-smoke jobs, full local hook and
test batteries, Sphinx `-W`, coverage policy, and
`pypi.sh testpypi_preflight` are the evidence-producing surfaces. A skipped or
not-applicable claim is not a passing receipt. If a supported platform is
removed, that is a changed executable scope and therefore a new candidate.
The three critical-path reports and their measured module floors, generated
invariants, mutation profiles, and strong artifact kinds are documented in
[Reconstruction critical-path quality gates](critical-path-quality.md).

Installed schema discovery stays reproducible through both:

```console
histdatacom reconstruction schemas --json
```

and `ReconstructionClient().schemas()`. Both expose the same content-addressed
registry bound by the candidate.

## Branch and tag governance

`ReleaseCandidateBranchGovernanceV1` requires a full commit-bound receipt for
an immutable `refs/heads/...` or `refs/tags/...` ref. The receipt must prove:

- protection is enabled;
- required checks match the checks intended for promotion to `main`;
- the final merge or tag must be signed where the hosting/account policy
  supports signing;
- scientific commits are forbidden after qualification begins; and
- any operational fix creates a new candidate and reruns affected evidence.

A local declaration is not protection evidence. Capture the actual GitHub
branch/ruleset or protected-tag response and hash it as the `protection_ref`.
Do not start qualification while GitHub reports the candidate ref as
unprotected.

## Candidate-scoped evidence

Every phase-two qualification, campaign receipt, product index, and
certification artifact must include the exact `candidate_id` in strong-reference
metadata. `bind_release_candidate_artifact()` creates a
`ReleaseCandidateArtifactBindingV1` only when that metadata agrees. Replacing
the ID or trying to reuse evidence from another candidate fails closed.

Operational fixes never mutate the candidate. Build a new commit, new
distribution artifacts, new validation receipts, new protected ref, and new
candidate manifest. Evidence may be rerun selectively only where the governing
policy explicitly establishes that unaffected evidence remains valid; the old
candidate's binding can never be relabeled.

The manifest records phase-two and release child issues as blockers. A frozen
candidate makes their execution reproducible; it does not claim that those
qualifications or the release decision have already passed.
