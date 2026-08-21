# Reconstruction critical-path quality gates

The reconstruction release path does not use repository-wide coverage as a
proxy for critical scientific behavior. Three commit-bound reports are
required by `ReconstructionReleaseCandidateV1`:

- `critical_branch_coverage` measures branches separately for experiment and
  schema identity, observation/cardinality, adaptive partitioning, support
  planning and indexing, bounded alignment, carving, cross-currency,
  persistence, orchestration, campaign publication, and certification;
- `critical_property_invariants` runs the bounded generated-contract checks
  plus their direct integration witnesses; and
- `critical_mutation_testing` proves that the selected tests fail under exact
  release-target source mutations.

Each JSON report uses
`histdatacom.critical-path-gate-report.v1`, contains the full Git commit, hashes
of every measured source/test input, one result per check, and a content-derived
report ID. The writer returns a strong `ArtifactRef` whose kind, commit, gate
name, pass state, and report ID are validated when it is attached to a release
candidate. Failed reports remain publishable for diagnosis but cannot satisfy a
release-candidate gate.

## Measured branch floors

The initial floors were derived from the focused 2026-08-21 baseline and
rounded down enough to tolerate coverage.py/platform branch representation
without permitting a material regression. Compact leaf contracts have higher
floors than very large orchestration modules. Current floors range from 50% for
the independent final-support verifier to 77% for schema restoration; the
complete table and rationale live beside the executable policy in
`scripts/critical_path_quality.py`.

Run a fresh full-suite branch report before evaluating the floors:

```bash
python -m pytest \
  --cov=histdatacom \
  --cov-branch \
  --cov-report=json:coverage.json \
  --cov-fail-under=0
python scripts/critical_path_quality.py coverage \
  --coverage-json coverage.json \
  --output critical-branch-coverage.json
```

No critical module may borrow coverage from another module or from the global
average. A missing module, a coverage file without branch data, or a value
below its own floor fails the command.

## Property and mutation profiles

The property gate generates bounded valid and invalid partitions, alignments,
semantic identities, resource boundaries, spectral-radius evidence,
rejection-count contracts, and scratch layouts. It also runs direct witnesses
for anchor immutability, order/retry invariance, campaign coordinate
uniqueness, incomplete-index publication refusal, and content-hash tampering:

```bash
python scripts/critical_path_quality.py properties \
  --output critical-property-invariants.json
```

Mutation runs copy the installed source package to a temporary directory,
apply one exact replacement, put that copy first on `PYTHONPATH`, and run only
the named killing tests. The repository worktree is never mutated. A test exit
of 1 is a killed mutant; a pass, timeout, collection error, or internal pytest
error fails the report. The mutation set covers boundary changes, removed hash
and contiguity checks, future timestamps, bid/ask-side swaps, disabled anchor
comparison, changed quote age, ignored or unverified products, runtime refusal
handling, and count reconciliation.

```bash
# Fast dev-to-main release-candidate subset
python scripts/critical_path_quality.py mutations \
  --profile focused \
  --output critical-mutation-testing.json

# Bounded complete release set
python scripts/critical_path_quality.py mutations \
  --profile release \
  --output critical-mutation-testing-release.json
```

The dev-to-main CI job retains all three focused reports with coverage output.
A push to `main` runs and retains the bounded complete mutation report. Release
operators must attach the exact passing artifact references produced for the
candidate commit; reports from another commit are rejected.
