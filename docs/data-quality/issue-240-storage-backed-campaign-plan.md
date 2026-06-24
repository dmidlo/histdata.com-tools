# Issue 240 Storage-Backed Campaign Plan

Parent: #93. Related: #233, #234, #235, #236, #237, #238, #239.

## Result

Issue #240 is the operational bridge between the completed data-quality rule
work and the remaining full-dataset campaign batches. The remaining batch
issues should not be run as one unbounded local scrape. They should run as
bounded symbol/format/timeframe slices from an installed platform wheel or from
a source checkout whose sidecar has an explicit Temporal executable.

The `.repo` helper is part of the campaign contract. Every completed slice must
run `histdatacom --repo-quality` so `.repo` keeps bounded quality findings and
the detailed JSON report path. Cleanup is not the normal default; it is an
explicit disk-pressure tactic for these full-dataset campaign batches.

## Local Audit

- Current source checkout sidecar mode: metadata-only fallback for
  `macos-arm64`; `histdatacom-sidecar doctor --json` reports
  `platform.executable_bundled=false`.
- Current local data root: `data`.
- Current local `.repo`: present and already carries bounded quality metadata
  for prior issue #233/#241 work.
- Current local disk is still below the full-surface operator floor used by the
  campaign preflight, so full batch accumulation is not the right execution
  model.

## Reproducible Environment

Use one of these environments:

1. Installed bundled platform wheel:

```sh
python -m venv .campaign-venv
. .campaign-venv/bin/activate
pip install histdatacom
histdatacom-sidecar doctor --json
```

The doctor payload must show `platform.executable_bundled=true`.

2. Source checkout with explicit Temporal executable:

```sh
venv/bin/histdatacom-sidecar start --executable /path/to/temporal
venv/bin/histdatacom-sidecar doctor --json
```

This is acceptable for development, but it must not be confused with a packaged
platform-wheel proof.

## Slice Contract

Each slice follows this order:

1. Refresh or verify `.repo` availability metadata.
2. Download and extract only a bounded symbol/format/timeframe slice.
3. Run data quality with `--repo-quality`.
4. Confirm the detailed JSON quality report exists.
5. Confirm `data/.repo` has bounded quality metadata for the slice.
6. If disk pressure requires it, clean the slice according to the selected
   cleanup mode.

The normal cleanup mode is `none`, which preserves generated artifacts. The
issue-240 disk-pressure cleanup mode removes only canonical Polars cache files:

```sh
find <data-root>/<FORMAT>/<TIMEFRAME>/<symbol> -name .data -type f -delete
```

For tighter low-disk campaign execution, use working-artifact cleanup after
`--repo-quality` has written the report and `.repo` summary:

```sh
rm -rf <data-root>/<FORMAT>/<TIMEFRAME>/<symbol>
```

Do not remove:

- `<data-root>/.repo`
- `<data-root>/.quality`
- campaign reports under the selected reports directory

## Command Shape

For one slice:

```sh
histdatacom -D -X \
  -p etxeur \
  -f ascii \
  -t M1 \
  --data-directory /Volumes/histdata/data

histdatacom --repo-quality \
  --quality-target /Volumes/histdata/data/ASCII/M1/etxeur \
  --quality-checks all \
  --quality-report /Volumes/histdata/reports/issue-234-001-ascii-m1-etxeur-quality.json \
  --data-directory /Volumes/histdata/data

find /Volumes/histdata/data/ASCII/M1/etxeur -name .data -type f -delete
```

Use `rm -rf /Volumes/histdata/data/ASCII/M1/etxeur` only after the
`--repo-quality` command has succeeded and the detailed report path has been
recorded.

## Plan Helper

The code-level helper generates the repeatable plan used by #234-#239:

```python
from histdatacom.activity_stages import read_repository_data_file
from histdatacom.data_quality import build_storage_backed_campaign_plan

repo = read_repository_data_file("/Volumes/histdata/data/.repo")
plan = build_storage_backed_campaign_plan(
    issue_number=234,
    repo_data=repo,
    symbols=("etxeur", "euraud", "eurcad", "eurchf", "eurczk"),
    data_directory="/Volumes/histdata/data",
    reports_directory="/Volumes/histdata/reports",
    cleanup_mode="cache",  # issue-240 disk-pressure mode
    platform_executable_bundled=True,
)
```

Omit `cleanup_mode` for normal artifact-preserving behavior. Set
`cleanup_mode="working-artifacts"` only when disk is the limiting factor.

## Batch Resume Rule

Before rerunning a slice, check the detailed report path and `.repo` quality
metadata. If both exist for the same symbol/format/timeframe/period surface,
skip that slice and move to the next one. If either is missing, rerun the slice
because `.repo` is the operator index and the JSON report is the audit artifact.

## Remaining Campaign Order

Run the remaining batches in order after this plan is in place:

- #234: `etxeur` through `eurnok`
- #235: `eurnzd` through `gbpjpy`
- #236: `gbpnzd` through `nzdusd`
- #237: `sgdjpy` through `usdhuf`
- #238: `usdjpy` through `xagusd`
- #239: `xauaud` through `zarjpy`
