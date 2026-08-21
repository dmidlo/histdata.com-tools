# Adaptive-window partition invariance

The reconstruction campaign may recursively split one source span when its
modeled missing cardinality approaches an engine safety limit. Splitting is an
operational choice, but it can change recurrent history, censoring, cardinality
draws, seed scope, and anchor geometry. The campaign therefore cannot assume
that a merged set of child windows has the same law as a single window.

`histdatacom.synthetic.partition_invariance` provides the candidate-bound
qualification gate for that assumption. It compares the same source span under
three declared treatments:

1. the coarsest admissible partition;
2. the partition selected by the adaptive planner; and
3. one deterministic, strictly finer refinement of the planner result.

The gate never claims rowwise identity. It measures distributional and
metric-level sensitivity and permits the full campaign only after representative
coverage, matched semantic replicates, and every hard tolerance pass.

## Exact partition identity

`AdaptivePartitionSpecV1` is a contiguous, half-open cover of one exact parent
span. Every child owns `[start_ns, end_ns)`. Its content identity includes every
nanosecond boundary, so moving a seam by one nanosecond creates a new child and
partition identity.

`PartitionInvarianceCaseV1` binds all three treatments to one:

- reconstruction release candidate and byte-verified candidate artifact;
- source-content SHA-256;
- selected model fit;
- observation-uncertainty scenario; and
- representative split-depth, epoch, activity, context, and alignment strata.

The coarsest treatment has one interval. Planner boundaries must refine it, and
the deterministic-finer boundaries must strictly refine the planner. A
different parent span, source digest, fit, scenario, or candidate is not a
partition comparison.

## Ownership, history, and seed audits

Before a replicate can contribute evidence, three contracts establish its
scientific lineage.

`audit_partition_source_ownership()` binds the upstream canonical hash of the
full source rows, assigns their event identities under strict half-open
semantics, and reports lost rows, duplicate event identities, seam rows, and
missing anchors. A run fails if its full-source hash differs, any source event is
not assigned exactly once, or an anchor is absent.

`audit_partition_history()` checks the carry/history supplied to every child.
Every history row must be a known source event, occur strictly before the child
start, and lie within the predeclared history horizon. Future, unknown, or
out-of-bound rows fail the run.

`build_partition_seed_ledger()` derives one parent seed from the release
candidate, parent span, model fit, observation scenario, semantic member, and
base seed. Child seeds then derive from that parent seed and exact child bounds.
Worker count, retry attempt, and child ordinal are deliberately absent. Thus the
same semantic member keeps its parent identity across partition treatments, and
an operational retry cannot masquerade as an independent scientific replicate.

## Predeclared comparison policy

`PartitionInvariancePolicyV1` freezes minimum case and replicate counts,
coverage strata, feature dimension, target power, alpha, energy-distance limits,
and metric-specific absolute and relative tolerances before qualification.

For merged final-stream feature vectors, the empirical comparison is

\[
D_E^2(P,Q)=2E\lVert F_P-F_Q\rVert
-E\lVert F_P-F'_P\rVert
-E\lVert F_Q-F'_Q\rVert.
\]

Each case produces comparisons for coarsest versus planner, planner versus
finer, and coarsest versus finer. Required metric summaries cover:

- total and per-symbol synthetic counts;
- interarrival and duration dependence;
- mark and update transitions;
- spread and path variation;
- maximum excursion and reversal count;
- pre- and post-projection triangle residuals;
- projection burden and boundary discontinuity; and
- runtime and resource work.

Strategy sensitivity may be included when the case supports it. A metric
tolerance is breached only when both its absolute and relative limits are
exceeded. Energy distance has separate advisory and hard limits.

## Fail-closed decision

`qualify_partition_invariance()` returns one of three statuses:

- `pass`: coverage and replicate power are complete and no hard limit fails;
- `fail`: evidence is complete but an ownership, history, energy, or hard
  metric check fails; or
- `insufficient_evidence`: cases, required strata, matched replicates, or
  feature dimension are incomplete.

Only `pass` sets `full_campaign_permitted` to true. Advisory findings remain in
the report without blocking execution. When a hard partition effect is real,
the allowed response is explicit: redesign state/cardinality carry, make the
partition policy a first-class scientific configuration, or narrow and publish
the product claim. Relabeling a failure as advisory after observing it requires
a new policy and qualification identity.

Qualification artifacts are written by
`write_partition_invariance_qualification()`. Their filenames contain the
SHA-256 of the canonical bytes, their metadata names the exact candidate and
decision, and reads recompute the scientific decision from the bound cases and
runs. A changed candidate file, source lineage, stored status, or report byte
fails verification.

The resulting
`histdatacom.partition-invariance-qualification.v1` reference is required
downstream evidence for the frozen candidate. It qualifies the planner-selected
partition policy for the declared scope; it does not generalize to arbitrary
boundaries, generators, fits, scenarios, or future candidates.
