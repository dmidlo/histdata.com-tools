# Data Quality Report Compatibility

The public data-quality report schema is `histdatacom.quality-report.v1`.
Automation can rely on the top-level report payload shape, bounded runtime
payload shape, severity/status vocabulary, target summaries, rule-result layout,
finding context, and quality-report artifact metadata.

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
- run-scoped finding detailed report;
- bounded runtime payload with quality-report artifact metadata.

The fixtures intentionally use stable `/quality-fixtures/...` paths instead of
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
