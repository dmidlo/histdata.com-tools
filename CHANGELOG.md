## Unreleased

### Added

- **reconstruction**: add a versioned broker-neutral v2.1 certification policy,
  hash/schema/subject-verified campaign runner, JSON-pointer observation
  extraction, atomic machine/human dossier publication, and installed
  `reconstruction certify` command while preserving legacy V1 evidence replay
  (#449).
- **reconstruction**: add exact paired nanosecond plan bounds for small
  representative-window campaigns while retaining complete touched-month
  source hashing, and report candidate amplification against aggregate
  estimated inputs instead of mixing all ensemble members with one raw-source
  denominator (#449).
- **reconstruction**: add content-addressed full-range plan sets and public
  `plan-set`/`preflight-set` operations so resource-safe daily windows can span
  the common history without exceeding the independent 64 MiB plan-artifact
  bound (#449).
- **reconstruction**: add atomic content-addressed write/readback verification
  for compact final-product activity manifests used by certification and
  downstream bar reconciliation (#449).

- **reconstruction**: expose the first-party reconstruction pipeline through
  an installed CLI family and typed Python facade, with explicit information
  mode/nonclaim requests, preflight resources and refusals, Temporal and local
  execution, aligned status/cancel/resume receipts, bounded lineage previews,
  replay verification, and stable exit codes (#467).
- **data-quality**: make weekend and expected-session-closure remediation
  guidance profile-aware with bounded calendar-policy context while preserving
  stable public weekend hint codes (#344).
- **data-quality**: preserve value-level quality-profile provenance during
  resolution across defaults, named profiles, files, YAML, API options, and CLI
  overrides, including previous source/value evidence (#367).

### Fixed

- **reconstruction**: preserve Arrow partition-row order for equal-timestamp
  source ticks, externalize proposal and carving batch evidence into bounded
  content-addressed ledgers, enforce live RSS limits, and truncate very large
  cross-currency refusal lists with a deterministic count and digest; inject
  one bounded, source-grid-aligned anchor from the sparsest declared leg into
  missing proposal legs so independently sampled modern streams have genuine
  exact-time triangle support without replacing immutable observations, and
  try every declared synthetic projection target before refusing a feasible
  cross-currency point (#449).
- **reconstruction**: resolve exact nanosecond plan bounds to source months with
  integer time conversion so the last nanosecond before a month boundary cannot
  round into the following partition (#449).
- **reconstruction**: compact high-cardinality activity provenance into bounded
  retained IDs plus explicit occurrence counts and ordered SHA-256 evidence
  instead of refusing ordinary reconstructed products at 256 IDs (#449).
- **reconstruction**: de-duplicate source partitions and strong artifact
  verification across adaptive full-range plan shards, with stat-identity hash
  and qualified-input caching plus compact streaming aggregation, so split
  months report unique raw rows and neither construction nor public preflight
  repeatedly materializes immutable corpora or retains every full shard graph
  in memory (#449).
- **reconstruction**: retain scientifically unsupported full-range spans as
  bounded refusal-only plan shards with zero workflow/output estimates, so
  public planning stays exactly contiguous without converting missing context
  into executable work (#449).
- **reconstruction**: reconcile end-to-end window runtime and peak stage
  resources into run reports instead of exposing atomic-commit runtime as if it
  represented the whole seven-stage execution (#449).
- **ci**: reserve coverage for one required `dev`-to-`main` production
  promotion job instead of running it during routine pushes, issue closure,
  and every Python/OS test-matrix job (#420).

## 1.3.2 (2026-07-03)

### Added

- **workflow**: add reusable issue closure/readiness tooling with acceptance
  coverage, pre-mutation gates, slow-phase summaries, no-mutation checks, and
  compact closeout reports.
- **data-quality**: add cache-scale preflight evidence, freshness policy,
  latest validation discovery, and operator-facing quality guidance.
- **orchestration**: add scheduled submission preflight, overlap guards,
  schedule identity filters, run request export, and scheduled run bundles.
- **instruments**: add individual triangle pair groups and group discovery CLI
  coverage.

### Changed

- **runtime**: keep Linux and macOS worker-starting bundled-runtime release
  smokes blocking while documenting the Windows install/CLI-only release gate
  until the native Temporal/Nexus worker startup blocker is resolved.

### Fixed

- **runtime**: improve Windows worker startup diagnostics, retry reporting,
  process isolation, startup cleanup, and Temporal SDK compatibility bounds.
- **release**: strengthen signing preflight, bundled runtime smoke gates, and
  Windows runner diagnostics.
- **data-quality**: make cache-scale quality checks viable and ensure quality
  preflight artifacts remain disposable.

### Added

- **data-quality**: add cache-scale preflight decisions, no-target diagnostics,
  saved evidence checks, and non-blocking warnings before large cache-backed
  quality runs.

## 0.79.0 (2026-06-24)

### Changed

- **temporal-orchestration**: make the Temporal orchestration the default CLI/API runtime.
  The foreground runtime remains available as a compatibility rollback through
  `--foreground` or `Options.use_orchestration = False`, and default orchestration runs
  start the bundled local orchestration when needed.

## 0.78.4 (2022-12-13)

### Fix

- **histdatacom**: address keyboard inturrupt for all but import to influx stages

## 0.78.3 (2022-12-05)

### Fix

- **package**: removed bs4 proxy dependency

## 0.78.2 (2022-12-05)

### Fix

- **cli.py**: sort arguments into logical groups

## 0.78.1 (2022-12-05)

### Fix

- **package**: add [pandas],[arrow], and [jupyter] pip install flags

## 0.78.0 (2022-12-05)

### Feat

- **package**: add --version arg to report version
