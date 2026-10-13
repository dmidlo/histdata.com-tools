# Project focus and delivery

This guide governs how work is selected and finished. It does not replace
scientific contracts, reduce acceptance criteria, or declare the roadmap done.
It exists to prevent support work, validation administration, and issue churn
from displacing delivery.

## Keep the product at the center

The product is a usable CLI/Python path from HistData archives through reliable
ingestion, quality assessment, and qualified reconstruction. For the current
scientific milestone, prioritize the complete HistData ASCII/T triangle
(EURGBP, EURUSD, GBPUSD), its genuine execution, and its usable verified outputs.

Use these existing authorities instead of inventing another roadmap:

- [Scientific ledger](reconstruction-scientific-ledger.md): estimand, assumptions,
  information boundaries, and the distinction between counterfactual ensembles
  and recovered historical truth.
- [Complete-campaign runbook](reconstruction-campaign-runbook.md): the executable
  source boundary, complete support accounting, publication, and verification.
- [Public interfaces](reconstruction-public-interfaces.md): the operator-facing
  CLI and Python path.
- The user-selected GitHub issue: the concrete outcome and acceptance criteria
  for the current change, within those scientific boundaries.

Prioritize defects that prevent this path from working, then unfinished core
capabilities and their qualification. SDK, portability, optimization, and other
support work must identify the core acceptance criterion they unblock to take
priority. Later providers and broker conditioning remain legitimate roadmap
work, not silently added prerequisites. An explicit user reprioritization can
select a different milestone without changing scientific validity claims.

## Bound the batch

At selection, record one short entry in the existing issue or batch discussion:

1. **Outcome:** what a user will be able to do after delivery.
2. **Original issue set:** a fixed denominator for this batch, including which
   acceptance criteria each issue owns.
3. **Validation:** required checks, applicable runtimes, expected duration and
   resource needs, and the conditions that invalidate earlier results.
4. **Delivery:** the integration branch and the evidence needed to commit,
   verify the push to `dev`, and close the issues.

Default to one deliverable. Combine complementary or non-overlapping issues
only when ownership and integration are clear. Give subagents bounded tasks;
their local edits are not delivered results. Keep one integration owner and one
final acceptance review rather than a review chain for every test module.

Audit the repository, branches, and issues at selection as requested, then use
delta checks against that baseline. Re-audit only what a new finding or changed
state makes relevant. Do not repeatedly inventory everything while tests run.

A discovery joins the active batch only if it demonstrably blocks an original
acceptance criterion. Explain that connection and the added cost. Otherwise
record it as deferred in the existing discussion; create its issue before any
later actionable work. Do not begin it just because an issue now exists.
Deliver the active batch before starting another, unless the user changes the
priority. Never hide scope growth by replacing the original issue denominator.

## Validate the change, not the administration

Required tests and scientific safeguards remain mandatory. Run cheap checks
early and the required complete validation against the final stable change.
Group compatible tests with ordinary repository tools; use existing CI/job
results where applicable. Do not add custom runners, receipt layers, or repeated
acceptance ceremonies when the existing tools provide the needed evidence.

Before an expensive run, use prior timings to state its expected cost and
coverage. If duration is unknown, say so and use a bounded timing check when
needed; do not promise an unsupported completion time. Reassess unexpectedly
expensive setup or repeated failures before automatically launching more work.
Never silently reduce coverage, replace genuine scientific paths with mocks,
or add skips to meet a cost estimate.

Reuse results only when the checked code, dependencies, inputs, environment,
and relevant integration behavior remain applicable. A change invalidates the
affected evidence, not automatically every unrelated result. Historical
equivalence remains explicitly historical, not a newly executed pass. Keep
failures and skips visible with their actual reasons. Documentation-only work
normally needs documentation/link checks and applicable hooks, not an unrelated
multi-hour scientific rerun; it must not weaken the separate release gates.

For long-running work, retain the process/job handle and use completion-aware
waiting. Checkpoints should capture how to resume and what action comes next,
not accumulate a new dossier on every poll. Report milestones, failures,
changed estimates, and decisions; avoid repeated unchanged-status goal turns
and unnecessary narration. A live process is a verified wait, not a blocker
or a delivered outcome. Observation timeouts are not permission to restart it.

## Use a delivery scoreboard

Keep progress reports short and separate these categories:

| Category | Evidence |
|---|---|
| Product delivered | Capability, original issue, verified remote `dev` commit |
| In progress | Local implementation, outstanding acceptance checks, next action |
| Supporting work | Defect/maintenance resolved and the product outcome it unblocks |
| Deferred | Remaining roadmap work, not included in the current denominator |

Counts of issues created, audits completed, tests started, or CPU hours used
are not product delivery metrics. Local code, passing synthetic tests, a pushed
development commit, a production release, and empirical qualification are
different states. State which one the evidence actually proves.

A batch is delivered only after its acceptance criteria pass, the normal
Commitizen workflow succeeds without hook bypasses, the intended commit is
verified on remote `dev`, and issue closure matches that evidence. Do not close
a campaign or empirical-study issue merely because supporting software ships.
Production release work still requires the full hooks/tests, local-simple-
registry TestPyPI preflight, TestPyPI from `dev`, then PyPI from `main` only after
that path passes. A documentation or development commit is not a release.
