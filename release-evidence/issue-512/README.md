# Issue #512 successor release-holdout evidence

This directory freezes the row-free scientific evidence for the first
successor to the retired, ineligible 2026-02 through 2026-05 holdout. It must
be committed before the successor candidate is fitted or frozen.

The corpus uses calibration period `202512`, validation period `202601`, and
fresh final-holdout period `202606`. Its four ten-minute final windows cover
Asia, London, New York, and overlap/closure; ordinary and event context;
all three observation-uncertainty scenarios; exact and bounded-nearest
alignment; and low, median, and high deficit strata. The June 1 month-opening
interval lacked synchronized triangle support, so the deterministic Asia
support window begins on June 2. Selection used source support only and no
candidate was fitted or evaluated.

The manifest is sealed and row-free. Its exact-window hashes, partition
identities, counts, signatures, and 64-bit neighbor sketches replay against
the local source corpus. Its leakage and coverage audits pass. The companion
evaluation policy is bound to the predeclared issue-#463 benchmark gate policy
and commit. Its February 1, 2026 source cutoff is the first exact month
boundary after the January validation split, binding the candidate catalog to
every complete monthly triangle through January without exposing June rows.

Reproduce the artifacts with:

```console
uv run python scripts/freeze_release_holdout_evidence.py \
  --source-root data/ASCII/T \
  --feed-epoch-definition data/.histdatacom/analytics/feed-epochs-v2-issue-460/feed-epochs-v2-definition.json \
  --observation-campaign data/.histdatacom/analytics/observation-calibration-v2-issue-462/observation-calibration-v2-campaign.json \
  --market-context-corpus .histdatacom/market-context-461-final-v4/market-context-corpus-9255f8c39f999b7a54e41a59a6f1d96f02e897af8383795e464a2f8738b08e00.json \
  --cftc-positioning-corpus data/.histdatacom/analytics/cftc-positioning-issue-468-final/cftc-positioning-corpus-887a47840090cdab1982fe910a4bdf8c1fcc9af256ab687bceae1b8dd1cbd3e0.json \
  --selection-dossier .histdatacom/issue-519-requalification/selection-v3/hawkes-product-selection-dossier-758c1fba9f6cd24b0d1790083c2d5db5ba4836d67ca8678138ed7903303128bc.json \
  --output-directory .histdatacom/issue-512-successor-evidence \
  --frozen-at-utc 2026-08-26T04:44:56Z
```

Do not authorize evaluation from this commit. The selected Marked Hawkes fit,
candidate graph, certification registry, and release candidate must be
created in a later descendant commit before the one-time authorization.
