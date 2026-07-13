# Reconstruction information modes

The version-one reconstruction information contracts make the distinction
between historically informed reconstruction and point-in-time simulation a
machine-checkable pre-generation gate. They do not source macro/news data,
fit models, generate events, or evaluate a strategy.

## Modes and trust boundary

`ex_post_reconstruction` may use explicitly labeled future anchors,
full-period summaries, global normalizations, or empirical motifs when both
the input and run policy bound the required look-ahead. A passing report is
valid for historically informed reconstruction and diagnostic
counterfactuals. It is not valid for prospective simulation or a strategy
usefulness claim.

`ex_ante_simulation` requires zero look-ahead. External and derived inputs
must have been point-in-time available when consumed, and pre-decision stages
cannot use future events, future anchors, full-period summaries, global
normalizations, or motifs selected from future observations. A passing report
opens the point-in-time simulation and strategy-usefulness gates, subject to
the declared chronological splits.

Any violation rejects the artifact graph. `require_reconstruction_information_audit()`
raises `InformationLeakageError` before generation and carries the complete
bounded report. A rejected report is invalid for generation, validation, and
strategy claims.

## Contract boundary

| Contract | Responsibility |
| --- | --- |
| `ReconstructionInformationPolicyV1` | One stable mode, maximum ex-post look-ahead, fail-closed behavior, and retained-finding limit. |
| `ReconstructionInformationInputV1` | One external or derived artifact use with event, observation, availability, consumption, vintage, revision, scope, stage, reason, parent, and split metadata. |
| `ReconstructionInformationSplitV1` | One half-open train, calibration, or validation interval. |
| `ReconstructionInformationManifestV1` | One run, policy, exact window-plan identity, complete artifact-use graph, and declared split sequence. |
| `InformationAuditFindingV1` | One deterministic rule ID with bounded evidence. |
| `InformationAuditReportV1` | Acceptance, total and retained violations, truncation state, and explicit valid/invalid downstream uses. |

The contracts are artifact-level sidecars. They do not add hundreds of
repeated fields to every synthetic event. Event rows retain `run_id`, source,
anchor, generator, motif, feed-epoch, broker-profile, and constraint lineage.
The run binds the information policy through `configuration_ids`; the audit
report then certifies the exact run, information manifest, and window plan.

## Construction order and streaming integration

The policy exists before the run, avoiding a circular identity between a run
and its completed input manifest:

```python
from histdatacom.synthetic import (
    InformationMode,
    ReconstructionInformationManifestV1,
    ReconstructionInformationPolicyV1,
    ReconstructionRunV1,
    audit_reconstruction_information,
    plan_reconstruction_windows,
    reconstruction_information_window_plan_id,
)

policy = ReconstructionInformationPolicyV1(
    information_mode=InformationMode.EX_ANTE_SIMULATION,
)
run = ReconstructionRunV1(
    symbols=("eurusd", "gbpusd", "eurgbp"),
    source_version_ids=("source:sha256:historical-v1",),
    configuration_ids=(policy.policy_id, "config:sha256:generator-v1"),
    ensemble_member_ids=("member-000",),
    base_seed=20260713,
)
windows = plan_reconstruction_windows(
    run,
    ensemble_member_id="member-000",
    start_ns=0,
    end_ns=300,
    window_size_ns=100,
    right_lookahead_ns=0,
)
manifest = ReconstructionInformationManifestV1(
    run_id=run.run_id,
    policy_id=policy.policy_id,
    information_mode=policy.information_mode,
    window_plan_id=reconstruction_information_window_plan_id(windows),
    inputs=declared_inputs,
    splits=chronological_splits,
)
report = audit_reconstruction_information(
    manifest,
    policy,
    run=run,
    windows=windows,
)
```

The retained-finding limit is serialized and enforced but intentionally
excluded from `policy_id`. Changing diagnostic retention therefore does not
change `run_id`, seeds, or scientific output. Mode, maximum allowed
look-ahead, fail-closed behavior, and ordered-split requirements remain
semantic policy identity.

`right_lookahead_ns` is an information-access channel, not merely transport
tuning. The audit hashes the complete window plan and checks every window:

- all windows must belong to the audited run;
- every ensemble member must have the same contiguous window boundaries;
- the supplied plan must match the manifest identity;
- ex-ante windows must have zero right look-ahead;
- ex-post windows cannot exceed the policy maximum.

Left halo remains historical context and does not require look-ahead
permission. Window ownership and batch transport remain governed by the
streaming contracts; the information audit only decides what those windows
may read and what downstream claims are valid.

## Input graph semantics

Every input requires:

- external or derived kind;
- consuming stage and temporal scope;
- semantic event time;
- inclusive observation start and end;
- point-in-time availability and actual use time;
- vintage ID and revision sequence;
- explicit allowed look-ahead;
- a human-readable reason for use;
- parent input IDs for derived artifacts;
- a split assignment where model fitting, calibration, validation, or
  strategy evaluation requires one.

Revisions name the superseded input. The audit checks sequence and availability
ordering and rejects a revised value used before its release in ex-ante mode.
Derived artifacts cannot become available before their parents. Missing
parents, orphan derived inputs, and graph cycles fail closed.

The three split contracts must appear in train, calibration, validation order
and must not overlap or regress. Model-fit inputs belong to train, calibration
inputs belong to calibration, and validation or strategy-evaluation inputs
belong to validation. Assigned observation intervals must stay inside their
declared split.

## Stable leakage rules and bounded reports

Rule IDs are stable `INFORMATION_*` values. The audit distinguishes, among
other failures:

- policy, run, manifest, input-mode, and window-plan mismatches;
- undeclared or excessive look-ahead;
- unavailable revisions and other not-yet-available inputs;
- future events and future observation windows;
- ex-ante future-anchor, full-period-summary, global-normalization, and motif
  selection leakage;
- missing parents and invalid revision chains;
- missing, duplicate, unordered, overlapping, or misassigned splits.

Findings are deduplicated and sorted deterministically before retention. The
report records the full violation count, retained count, and whether evidence
was truncated. Each finding also has a deterministic content-derived ID and a
hard evidence-size limit.

## Downstream obligations

Later reconstruction stages must add every artifact they consume or derive to
the manifest with its real availability and use times. In particular:

- #434 must declare feed-epoch summaries and their observation intervals;
- #435 must declare fitted observation-operator inputs and train split;
- #437 must retain macro/news vintages and release/revision times;
- #438 must distinguish point-in-time motif availability from an ex-post
  reference library;
- #439 and #440 must require a passing report before generation or carving;
- #448 must reject strategy claims unless the report is accepted in the
  appropriate mode.

Changing required fields, rule meaning, validity claims, or identity
derivation requires a new schema version and contract class.
