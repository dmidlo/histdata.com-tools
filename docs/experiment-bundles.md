# Public experiment bundles

`histdatacom.experiments` implements the **public interoperability** scope of
[#718](https://github.com/dmidlo/histdata.com-tools/issues/718), as clarified by
the [owner's classification](https://github.com/dmidlo/histdata.com-tools/issues/718#issuecomment-5363601666).
It does not run training, search, protected evaluation, deployment or private
model registries. The package and its `py.typed` surface are additive v1 APIs.
The separate HistData-specific `reconstruction_experiment.py` is unchanged.

## Identity and immutable state

All durable classes use a canonical envelope with `schema_version`,
`artifact_id`, and `payload`. Sorted JSON keys, compact separators, ASCII JSON
escaping and finite exact Python scalar types define v1 serialization. Readers
reject duplicate/unknown fields, noncanonical text and mismatched IDs. Artifact
hashes include the schema version. Ordered inventories must be sorted/unique;
they are not silently reordered. Decimal metric JSON must retain its float
type (`1.0`, not `1`); no numerical tolerance is introduced.

`ExperimentBundleV1.experiment_id` hashes the versioned scientific input object.
Its separate `artifact_id` protects the whole envelope, including display title
and descriptive branch. Display changes therefore preserve scientific identity
without overwriting the original artifact. `ScientificInputsV1` explicitly binds:

- hypothesis and fit policy;
- all 20 named components: dataset products and lineage, split policy and
  artifact, feature registry and projection, preprocessing, label, uncertainty,
  transport, model architecture/configuration/weights, calibration, decision
  policy, transaction cost, financing, margin, execution and account policy;
- exact Git commit/tree, runtime, lock and SBOM references;
- stochastic semantic namespace, seeds and declared replay class/policy;
- metric-registry identity, reporting strata, research cycle, immutable search
  plan and protected-evaluation roots.

The complete component inventory is mandatory. Inapplicability needs an explicit
reason; core dataset/split/feature/preprocessing/label/model-definition components
cannot be omitted. Trading-action evaluation also requires every decision,
execution, cost and risk policy. A planned non-trained model can explicitly mark
weights inapplicable; that is not evidence of a fitted or qualified model.
Scientifically material batch size, worker-dependent arithmetic or training
configuration belongs in the fit/model/runtime references, not display metadata.

`ArtifactReferenceV1` supports positive int64 byte lengths, including large
external weights/corpora, without reading or allocating them. Every reference
declares `synthetic_fixture_not_scientific_qualification` or
`declared_external_unverified`. Digests prove neither authenticity, rights,
historical availability, empirical validity nor completeness of external work.
External schemas and native IDs are declarations. The registry never upgrades
them to source-verified or scientifically-passed evidence.

Synthetic fixtures retain exact small canonical payloads. Generic
`experiment-fixture-spec.v1` values are labelled test specifications, not model
receipts. The native `synthetic-event-stream.v1` fixture bridge calls the actual
`SyntheticEventStreamV1` reader, checks its native ID, and requires byte-identical
canonical reconstruction, including rejection of unknown fields tolerated by
an older reader. This integration runs no generator and opens no source data.

## Attempts, results and lifecycle

`ExperimentAttemptV1.attempt_id` identifies a logical attempt by scientific
experiment and attempt label. Each running/terminal state has a distinct
immutable `artifact_id`. `previous_state_id` binds the retained start to its
terminal state; original runtime, inputs, resources, worker, retry ancestry and
already-declared outputs cannot change. A terminal-only imported receipt is
permitted, with declared start/end clocks. Its active interval is `[start,end)`.

Retries use a new attempt label and reference a terminated predecessor state.
`retry_reason` is independent of a terminal failure `reason`, so a running retry
is representable. Changed scientific inputs produce a successor scientific ID,
not a retry of the old experiment. Wall/memory/worker resource envelopes here
are declarations, not an execution supervisor or measurements.

`MetricDefinitionV1` binds name/version/formula/direction/units, evidence-mass
weighting, missing/censored handling, aggregation and uncertainty method.
`MetricRegistryV1` is itself versioned and content-addressed. Semantic changes
cannot reuse the old metric ID. These are semantic declarations, not an
implementation of arbitrary user-supplied formulas.

`ExperimentResultV1.result_id` binds experiment, metric, stratum and canonical
payload hash. A payload has either a finite value with positive unit support,
or an explicit unavailable reason. Optional uncertainty endpoints must be
complete and ordered; they do not manufacture statistical coverage.
`ResultReceiptV1` links exact result bytes to the matching completed attempt's
produced-artifact inventory. Multiple execution receipts can cite the same
replay-invariant result without changing its identity.

`ExperimentLifecycleV1` is an append-only, non-forking chain beginning `planned`.
It represents `running`, `failed`, `completed`, `invalidated` and `superseded`.
Running checks resolve terminal attempt states, not obsolete start receipts.
Completion requires matching completed execution and an exact result producer
no later than the completion clock. Supersession requires a successor with
retained candidate ancestry. An invalidation may omit a successor if none exists;
superseded evidence can subsequently be invalidated. Original bundles, attempts
and results are never rewritten. Registry snapshots bind the complete inputs,
produced results, lifecycle and promotion/deployment references together.

## Frozen search plans and protected exposure

`ExperimentSearchPlanV1` binds the pre-result candidate keys, search budget,
declaration clock, predecessor-plan IDs and exact protected content hashes.
It contains no candidate experiment IDs, avoiding a circular identity.
Scientific inputs bind this exact plan ID. `ResearchCycleV1` then binds every
candidate key to an experiment and its parent experiments. Its mapping must
match the frozen inventory. Relabelling or narrowing a plan under the same
human cycle name cannot preserve the experiment's original plan binding.

Every declared candidate has a `CandidateDispositionV1`, including unexecuted,
failed, insufficient, unfavorable and declared favorable outcomes. Every retained
attempt/result appears in that ledger, including failures. All candidate and
cycle ancestry is resolved and acyclic. This checks **declared completeness**;
it cannot prove nobody omitted work before declaring a plan or fabricated clocks.

`ProtectedExposureV1` names actual retained result IDs, a bound protected root,
an inspection clock and redesigned descendants. Inspection cannot precede
the exact result producer. Redesign requires a later successor cycle and
retained ancestry. Reuse checks compare the exposed root and exact split-content
hashes against descendant dataset/split references, not just human labels or
native-ID spelling. The rule is conservative: reusing the same opaque split
artifact does not establish an independent holdout. Supply a distinct exact
split/evidence artifact when a genuinely new scope is declared; the public
registry does not inspect private rows to adjudicate disjointness.

## Comparisons, reports and promotion bindings

`scientific_differences` returns the exact changed coordinates. Components are
atomic reference inventories; runtime/code/RNG subfields are separately named.
`ExperimentComparisonV1` requires declared differences to equal observed ones.
Feature-plus-strategy and downstream model changes are both visible; threshold
changes are decision-policy changes; transported products change both dataset
and transport; member/scenario policies are explicit uncertainty differences.
This controls declared inputs, not causal attribution of market outcomes.

Comparison results must share the exact metric and stratum and match their
respective experiments. `ExperimentReportV1` contains citations, not editable
metric-value copies. `resolve_report` and `render_report_markdown` resolve values,
support, uncertainty, current status, evidence kinds and frozen plan IDs from
the retained registry. Right-minus-left differences are computed from exact
binary-float rationals before one output rounding; unrepresentable results
refuse. Titles are descriptive, not validated scientific claims. Reports do not
silently convert differences to percentages or claim an improvement direction.

`PromotionBindingV1` is a neutral interoperability reference, **not a deployment
operation or certification**. It binds the exact experiment/result/comparison
context, promotion policy, deployment artifact reference and limitations. A
generic model digest alone is insufficient. The declared search ancestry must
be resolved and terminal, with qualifying results available at the original
promotion clock. Historical validation is as-of that clock; a later exact retry
does not retroactively become an earlier search attempt. Current
`assert_promotion_admissible` additionally refuses invalidated current ancestry.
It requires the exact `PromotionBindingV1` already retained in
`registry.promotions`; a separately supplied candidate cannot bypass the
registry's fixture/reference verification.
Private policy thresholds, claims of favorable utility and operating decisions
remain external and unverified.

Historical promotions and reports remain readable after later invalidation.
`reverse_dependencies` exposes affected experiments, result receipts, reports,
comparisons and promotion/deployment bindings through retained graph edges.
It does not infer undeclared causal relationships from a defect's prose reason.
Consumers must load the intended latest registry snapshot to learn later events;
an old immutable snapshot cannot know future audits.

## Public API and storage

```python
from histdatacom.experiments import (
    ExperimentRegistryV1,
    read_experiment_artifact,
    render_report_markdown,
    write_experiment_artifact,
)

# `path` is the exact content-named registry file supplied by the caller.
registry = read_experiment_artifact(path, ExperimentRegistryV1)
table = render_report_markdown(registry, registry.reports[0])
saved = write_experiment_artifact(registry, existing_output_directory)
```

Every durable `Artifact` subclass has `to_dict`/`from_dict` and
`to_json`/`from_json`. Nested `Record` values use `to_payload`/`from_payload`;
payload readers are not alternate versioned-envelope readers. Report rows and
resolved reports are immutable **process-local projections**, not authoritative
durable copies or private registry schemas. Wire, graph, metric and storage APIs
are stdlib-only. The optional native fixture bridge lazily imports the existing
native contracts; it does not duplicate their implementation.

Per-artifact limits: 8 MiB canonical bytes, 131,072 expanded nodes, depth 20,
4,096 collection entries, 65,536 characters per string and signed int64 scalar
integers. Embedded native fixture payloads count after parsing, including in the
complete registry. Repeated aliases, cycles and repeated strings are charged
before large expansion. These are reference/interoperability bounds, not a
promise to embed full models or corpora. Use external content references for
large artifacts; never a compressed/unchecked JSON bypass.

Files must be canonical ASCII JSON without a trailing newline and named
`<kind>-<content-digest>.json`. Reads check regular files, symlink rejection,
nonblocking no-follow leaf open, size bounds and before/after metadata. Writes
replay validation, fsync a temporary file and publish with an exclusive hard
link. Exact repeats are idempotent; conflicting bytes refuse. Temporary files
are removed after interruption; prior artifacts are preserved. POSIX directory
fsync provides the additional directory-durability step; Windows publication
has no directory-fsync claim. This is not a hostile same-user filesystem sandbox.

## Scoped requirements and maturity

The shipped `assets/requirements_v1.json` maps every public #718 atom to its
API and synthetic test. It records public implementation/conformance separately
from private execution and empirical qualification. The suite includes actual
native fixture integration, independent hash math, component changes, exact
differences, missing candidates, protected-root relabelling, clock/refusal
canaries, append-only invalidation and real filesystem publication.

These are scoped #691/#525 evidence records, **not completion of either global
issue**. All major private campaign instances, trained weights, protected search
ledgers, empirical metric values, operating promotions and result archives remain
private execution obligations. No public test fixture stands in for those data.
