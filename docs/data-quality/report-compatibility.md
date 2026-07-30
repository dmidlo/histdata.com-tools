# Data Quality Report Compatibility

The public data-quality report schema is `histdatacom.quality-report.v1`.
Automation can rely on the top-level report payload shape, bounded runtime
payload shape, severity/status vocabulary, target summaries, rule-result layout,
finding context, and quality-report artifact metadata.

## Path Publication Safety

Quality report JSON is publish-safe by default. Path-like fields are serialized
as relative project/report paths so generated reports can be reviewed or shared
without exposing local home directories, temporary directories, or sidecar
workspace roots. Callers that need exact local paths for debugging can opt into
raw payloads through the reporting API.

## Compatibility Contract

Compatible v1 changes:

- adding optional metadata fields inside existing `metadata` objects;
- adding new finding codes or rule IDs;
- adding new quality check groups without changing existing payload keys;
- adding new target kinds only when older consumers can safely ignore them.

Schema-version changes are required when a change removes, renames, or changes
the meaning of existing top-level keys, summary keys, target fields, rule-result
fields, finding fields, bounded runtime payload keys, or artifact metadata
fields. A schema-version change is also required when severity or status values
change meaning.

## Intentional Rule Skips

Reports with intentionally skipped target-rule evaluations include optional
`metadata.quality_engine` metadata using
`histdatacom.quality-engine.v1`. Its planned, executed, and skipped target-rule
counts satisfy:

```text
planned_target_rule_evaluation_count
  = target_rule_evaluation_count + skipped_rule_evaluation_count
```

`quality_engine.skip_events` uses
`histdatacom.quality-skip-events.v1`. Each event contains a stable reason code,
rule ID, target kind, and publish-safe data format, timeframe, symbol, and
period axis. Events never contain absolute paths or row samples. The event list
is deterministically limited to 128 entries; reason, rule, and target-kind
count maps are limited to 64 entries and expose complete truncation accounting
under `limit_metadata`.

The legacy
`quality_engine.skipped_duplicate_archive_rule_evaluation_count` remains
available. Full report consumers can use the structured events from report
metadata, and bounded runtime consumers receive the same contract at the
top-level `quality_engine` key. Reports without intentional rule skips do not
add `quality_engine`, preserving the prior optional-metadata behavior.

## Fingerprint Topology Inspection Context

Fingerprint topology-attention target summaries may include optional
`inspection_context` using
`histdatacom.timestamp-topology-inspection.v1`. Each evidence section contains
`total_count`, `included_count`, `omitted_count`, `truncated`, a bounded
`samples` list, and complete sample `limit_metadata`. Actionable sections link
to the same stable remediation identity used by `quality_next_actions` through
`code`, `action_kind`, `rule_id`, `flag`, and the copied target axis. Expected
session closures are
marked non-actionable and only provide context for suspicious gaps.

This is a compatible optional v1 metadata addition. The default payload remains
publish-safe: timestamp and row-position evidence is allowed, while absolute
paths, credentials, complete quote rows, and raw row excerpts are not.

## Cross-Series Fingerprint

Fingerprint runs may include optional `metadata.cross_series_fingerprint`
using `histdatacom.cross-series-fingerprint.v1`. The bounded runtime equivalent
is `fingerprint_cross_series`. Group and pair lists are deterministic and expose
complete limit/truncation metadata. Group rows contain publish-safe target axes,
symbol membership, timestamp-grid and coverage-range summaries, topology and
cache provenance counts, and bounded return-correlation results. Consistency
samples may include `series_id`, `period`, `row_id`, `source_row_number`, and
`event_seq`, but never absolute paths or raw quote rows.

This is a compatible optional v1 report addition. Consumers that do not use
run-scoped fingerprints can ignore both keys.

## Exponential-Smoothing Fingerprint

Fingerprint findings may include the opt-in
`time_series_fingerprint.exponential_smoothing` section using
`histdatacom.exponential-smoothing.v1`. Full reports summarize it under
`metadata.time_series_fingerprint_exponential_smoothing_summary`; the bounded
runtime equivalent is `fingerprint_exponential_smoothing`, and the text summary
heading is `Exponential-smoothing models`.

Configuration, fit, forecast, evaluation, training-projection, and run-summary
objects carry their own `histdatacom.exponential-smoothing-*.v1` schema
versions. Model specifications and target summaries are deterministic and
bounded. Scalar fitted parameters may be present in bounded fit samples, but
fitted backend objects, residual vectors, exception text, and measured
wall-clock durations are intentionally absent. Consumers must treat model
status, convergence, errors, and baseline comparisons as advisory; no field
selects an automatic winner or changes the data-quality exit decision.

The same annotation engine projects a configured specification and horizon as
nullable `cm_ets_*` scalar columns on enriched tick rows. Forecast values are
point-in-time available at the origin-bin close. Realized actual/error values
are post-observation diagnostics with separate availability and eligibility
flags. Durable identity remains `series_id`, `period`, and `row_id`; timestamp
is never the sole join key.

This is a compatible optional v1 report and enriched-column addition. Core
installs and profiles that do not enable the family omit these report keys and
do not import the optional numerical backend.

## Autoregressive-Family Fingerprint

Fingerprint findings may also include the opt-in
`time_series_fingerprint.autoregressive` section using
`histdatacom.autoregressive.v1`. Full reports summarize it under
`metadata.time_series_fingerprint_autoregressive_summary`; the bounded runtime
equivalent is `fingerprint_autoregressive`, and the text heading is
`Autoregressive models`.

Configuration, fit, forecast, evaluation, training-projection, and summary
objects use stable `histdatacom.autoregressive-*.v1` schemas. AR, ARMA, and
ARIMA remain explicit families with explicit orders. Automatic order search and
winner selection are absent. Backend failures, convergence, roots,
conditioning, parameters, fold errors, baseline references, resource limits,
and Statsmodels version are bounded advisory metadata.

Configured projections add nullable scalars under `cm_ar_*`, `cm_arma_*`, and
`cm_arima_*` on enriched tick rows. Forecast fields become available at their
origin; realized errors are separately marked post-observation diagnostics.
Durable identity remains `series_id`, `period`, and `row_id`, and consumers do
not need a side-table join. Older consumers may ignore the optional report keys
and the additive enriched columns.

## Seasonal/Exogenous-Family Fingerprint

Fingerprint findings may include the opt-in
`time_series_fingerprint.seasonal_exogenous` section using
`histdatacom.seasonal-exogenous.v1`. Full reports summarize it under
`metadata.time_series_fingerprint_seasonal_exogenous_summary`; the bounded
runtime equivalent is `fingerprint_seasonal_exogenous`, and the text heading is
`Seasonal and exogenous models`.

Configuration, regressor, fit, forecast, evaluation, training-projection, and
summary objects use stable `histdatacom.seasonal-exogenous-*.v1` schemas.
SARIMA, ARIMAX, and SARIMAX have explicit nonseasonal/seasonal orders and an
explicit seasonal cycle tied to the model-input sampling frequency. The flat
regressor contract records deterministic column order, vocabulary,
known-in-advance availability, missingness, and calendar-profile provenance.
Observed future market values, automatic order/regressor search, and automatic
winner selection are absent.

Configured projections add 123 nullable scalar columns under `cm_sarima_*`,
`cm_arimax_*`, and `cm_sarimax_*` on enriched tick rows. Forecast fields become
available at their origin; realized errors remain separately flagged
post-observation diagnostics. Durable identity remains `series_id`, `period`,
and `row_id`. Consumers may ignore the optional additive report keys and
columns.

## Volatility-Family Fingerprint

Fingerprint findings may include the opt-in
`time_series_fingerprint.volatility` section using
`histdatacom.volatility.v1`. Full reports summarize it under
`metadata.time_series_fingerprint_volatility_summary`; the bounded runtime
equivalent is `fingerprint_volatility`, and the text heading is
`ARCH and GARCH volatility models`.

Configuration, fit, forecast, evaluation, training-projection, and summary
objects use stable `histdatacom.volatility-*.v1` schemas. Symmetric ARCH and
GARCH orders, return input, mean model or preceding residual reference,
innovation distribution, scale, variance initialization, covariance type,
parameter bounds, and resource limits are explicit. Conditional-mean,
conditional-variance, and volatility metrics are separate; the realized
variance proxy is named, and comparison remains descriptive with no automatic
winner. Asymmetric models are registry-only in this contract.

Configured projections add 78 nullable scalar columns under `cm_arch_*` and
`cm_garch_*` on enriched tick rows. Forecast fields become available at their
origin; realized-return and variance diagnostics remain separately flagged
post-observation fields. Durable identity remains `series_id`, `period`, and
`row_id`. Consumers may ignore these optional additive report keys and columns.

## Classical-Model Comparison

Fingerprint findings may include the opt-in
`time_series_fingerprint.classical_model_comparison` section using
`histdatacom.classical-model-comparison.v1`. It is generated only from saved
bounded evaluation artifacts and cannot trigger fits. Full reports summarize it
under `metadata.time_series_fingerprint_classical_model_comparison_summary`;
the bounded runtime key is `fingerprint_classical_model_comparison`, and the
text heading is `Classical model comparison`.

Compatibility is explicit across dataset/fingerprint, regularization contract,
fold set, target metric, scale, transform, frequency, missingness, horizon, and
period. Incompatible or incomplete evidence stays visible but ineligible. Skill
uses the configured mean or variance reference without silently substituting a
baseline; negative skill, missing references, near-zero reference errors, and
incomplete folds have stable reason codes. Conditional-mean,
conditional-variance, and volatility metrics are never pooled.

Fit accounting preserves attempted, fitted, converged, limited, skipped,
timed-out, numerically invalid, dependency-unavailable, failed, and
resource-limited counts. Stability is advisory and distinguishes insufficient
folds, stable behavior, structural parameter shifts, isolated failures, and
persistent error degradation. Fingerprint regime/stationarity/decomposition
signals are context only, not causal claims. Diagnostics are bounded and omit
fitted objects, raw rows, residual vectors, paths, and backend exception text.

The augmented row contract adds 43 nullable scalar columns under
`cm_comparison_*`, `cm_skill_*`, and `cm_stability_*`. They are retrospective,
target-time gated, explicitly not training-eligible, and joined only by durable
`series_id`, `period`, and `row_id`. No winner, best-model field, production
recommendation, or normative automatic selection is part of this schema.

## Synthetic Tick Generation

`histdatacom.synthetic-tick-generation.v1` is a bounded diagnostic artifact,
not a new quality-report top level. Its configuration and automatic candidate
validation use `histdatacom.synthetic-tick-generation-configuration.v1` and
`histdatacom.synthetic-tick-generation-validation.v1`. The generated candidate
report remains an ordinary `histdatacom.quality-report.v1` containing the same
`fingerprint.series` payload and `histdatacom.synthetic-fingerprint-validation.v1`
comparison semantics used for external candidates.

The row schema is additive: the seven existing nullable `synth_*` columns are
populated on the enriched ASCII tick row, while observed bid/ask, duplicate
timestamps, and `series_id`/`period`/`row_id` identity are preserved. Consumers
that do not use synthetic values may continue to ignore those columns. The
generator never adds a separate table, changes timestamp identity, or restores
M1 as an independent base grain.

Generation diagnostics are deterministic for the same reference fingerprint,
configuration, and reference rows. Bounded evidence includes only counts,
stable IDs, configuration, and a limited transition-index sample; it excludes
raw quote rows, fitted objects, absolute report paths, and exception text.
Statistical fingerprint mismatch remains advisory and cannot change the
candidate quality status.

## Golden Fixtures

Representative payload fixtures live under
`tests/fixtures/data_quality_reports/` and are checked by
`tests/unit/test_data_quality_report_goldens.py`.

The golden suite covers:

- clean CSV detailed report;
- dirty CSV detailed report;
- corrupt ZIP detailed report;
- coverage-manifest failure detailed report;
- canonical cache target detailed report;
- deterministic synthetic tick generation diagnostics and candidate-validation evidence;
- duplicate ZIP/CSV quality-engine skip detailed report;
- bounded runtime payload with structured quality-engine skips;
- fingerprint detailed and bounded reports with action-linked topology
  inspection context;
- run-scoped finding detailed report;
- bounded runtime payload with quality-report artifact metadata.

The fixtures intentionally use stable `quality-fixtures/...` paths instead of
machine-local absolute paths.

Remediation catalog audit payloads may add bounded attribution evidence while
remaining compatible with `histdatacom.quality-remediation-catalog-audit.v1`.
Current attribution fields include `attribution_status`,
`attribution_reason`, status/reason counts, source-helper counts, and
finding-code-prefix counts. Consumers must treat `exact`, `inferred`, and
`unresolved` as advisory audit states; they do not replace `rule_id`, alter
finding severity, or change quality exit decisions. Runtime-only report gaps
use `runtime_report` because their rule ID comes from the saved report rather
than static source discovery.

Remediation coverage and catalog-audit groups also include deterministic
`actionability` and `actionability_reason` fields. The stable actionability
vocabulary is `remediable_defect`, `policy_or_profile_decision`,
`unsupported_format_or_capability`, `expected_artifact_or_context`,
`needs_rule_attribution`, `needs_diagnostic_context`, `unsafe_to_automate`, and
`informational_only`. Boundary-aware summary counts are advisory additions:
ordinary mapped/unmapped and warning/error counts remain unchanged, while
actionable, intentional-boundary, attribution-blocked, and
diagnostic-context-blocked counts explain which gaps should be worked first.
Unknown warning/error codes default to `remediable_defect`; only deterministic
rule, finding-code, severity, mapping, or attribution evidence can move a gap
behind actionable defects. These fields do not change finding severity, quality
status, or exit policy.

Catalog audits may also add a `remediation_plan` using
`histdatacom.quality-remediation-plan.v1`. The section is derived from
`ranked_gaps` and has independent bounded-sequence metadata under
`payload_limits.remediation_plan`. Consumers should use `items`,
`plan_item_count`, `included_plan_item_count`, `omitted_plan_item_count`, and
`truncated` together rather than assuming every candidate is embedded. Each
item preserves its `catalog_gap_rank` while adding a fixability-oriented `rank`,
suggested selector/action/hint-code metadata, explicit `missing_fields`, and
bounded `evidence`. Fixability scores and proposals are deterministic advisory
planning aids; they are not applied remediation mappings and do not mutate
reports, catalogs, files, or repository state.

Standalone repair-plan output uses
`histdatacom.quality-repair-plan.v1`. It is derived from a saved
`histdatacom.quality-report.v1` and does not alter or replace the source report.
Consumers should use `plan_item_count`, `included_plan_item_count`,
`omitted_plan_item_count`, `truncated`, and `payload_limits.items` together.
Each item preserves the finding code, rule ID, severity, mapped hint and action
kind, publish-safe target identity, proposed operation, preconditions,
evidence requirements, bounded evidence, missing context, and confidence
basis. Stable proposal states are `proposed`, `needs_context`, and
`unsupported`; operation execution remains `manual_only` or `unsupported`.
The top-level `mode`, `apply_supported`, `mutating_operations_performed`, and
`safety` fields are normative: version 1 is advisory and performs no file,
archive, permission, network, report, or catalog mutation.

## Update Workflow

Do not update golden fixtures as a side effect of routine test runs. When a
report shape intentionally changes, first decide whether the change is
compatible with `histdatacom.quality-report.v1`. If it is compatible, regenerate
the fixtures explicitly:

```bash
HISTDATACOM_UPDATE_QUALITY_GOLDENS=1 \
  venv/bin/python -m pytest tests/unit/test_data_quality_report_goldens.py
```

Then review the fixture diff directly:

```bash
git diff -- tests/fixtures/data_quality_reports
```

If the diff removes or renames public keys, changes severity/status meanings, or
breaks the bounded runtime payload contract, update the schema version and
document the migration path instead of silently refreshing v1 fixtures.
