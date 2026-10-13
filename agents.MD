# Repository working instructions

## Project focus

Read [the delivery guide](docs/development-delivery.md) before selecting or
expanding work. The project provides reliable HistData ingestion, assessment,
and scientifically qualified reconstruction. The current executable scientific
milestone is the HistData ASCII/T EURGBP/EURUSD/GBPUSD campaign described in the
[scientific ledger](docs/reconstruction-scientific-ledger.md) and
[campaign runbook](docs/reconstruction-campaign-runbook.md). Preserve the broader
roadmap, but do not make later-provider or broker-ecosystem expansion an implicit
prerequisite for that milestone. Explicit user priorities take precedence.

## Required workflow

- Always create a GitHub issue before actionable work, including cleanup or
  maintenance; reuse an existing open issue when it already covers the work.
- GitHub issues and versioned changes follow SemVer. Do not bump the package
  version merely to record documentation or workflow maintenance.
- Work from `dev`; production release work goes through `main`.
- Use Commitizen `cz` for commit/version workflows and `gh` for GitHub workflows.
- Run the full battery of pre-commit hooks and test suites before release work
  is considered complete. Never bypass hooks or weaken acceptance to finish.
- Run the TestPyPI preflight with the local simple registry before publishing.
- Publish to TestPyPI from `dev` first; publish to PyPI from `main` only after
  the TestPyPI path passes.

## Delivery discipline

- Define a bounded deliverable, its original issue set, acceptance criteria,
  and validation cost before implementation. Keep this in the existing issue
  or batch discussion, not a new tracking system.
- Finish and deliver the active batch before opening another implementation
  lane, unless the user explicitly reprioritizes. Subagents get bounded,
  non-overlapping tasks and share one integration/acceptance boundary.
- Add supporting work to the batch only when it blocks a named acceptance
  criterion. Defer other discoveries; issue creation is not product progress.
- Audit once for the chosen baseline, then inspect relevant changes. Do not
  repeat a whole-repository, branch, or issue audit without a concrete reason.
- Reuse standard test/job tools and still-applicable results. Batch compatible
  tests; do not invent per-module supervisors, evidence schemas, or repeated
  independent reviews without a demonstrated need.
- Preserve required behavioral coverage, scientific inputs, and release gates.
  Document the scope and limitations of reused evidence; never call an unrun,
  skipped, failed, or historical-equivalence check a fresh pass.
- Keep long-running jobs intact. Prefer completion-aware waits and compact
  checkpoints; avoid goal-turn or chat churn that only repeats unchanged state.
  Time spent running tests is not a delivered capability.
- Report product outcomes delivered to remote `dev`, current work awaiting
  validation, and supporting maintenance separately. Close issues only against
  their actual acceptance criteria and verified delivery; release/publication
  and empirical qualification are separate claims.
