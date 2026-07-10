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
- duplicate ZIP/CSV quality-engine skip detailed report;
- bounded runtime payload with structured quality-engine skips;
- fingerprint detailed and bounded reports with action-linked topology
  inspection context;
- run-scoped finding detailed report;
- bounded runtime payload with quality-report artifact metadata.

The fixtures intentionally use stable `quality-fixtures/...` paths instead of
machine-local absolute paths.

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
