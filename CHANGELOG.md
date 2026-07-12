## Unreleased

### Added

- **data-quality**: make weekend and expected-session-closure remediation
  guidance profile-aware with bounded calendar-policy context while preserving
  stable public weekend hint codes (#344).
- **data-quality**: preserve value-level quality-profile provenance during
  resolution across defaults, named profiles, files, YAML, API options, and CLI
  overrides, including previous source/value evidence (#367).

### Fixed

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
