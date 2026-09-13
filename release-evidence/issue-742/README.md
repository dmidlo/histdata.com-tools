# Issue #742 RC22-independent successor holdout

This directory freezes the row-free scientific evidence for the third v2.5
release-decision successor. It was created after RC22 consumed the July 2026
holdout, without using RC22 rows, metrics, failure details, or gate results to
select a window, model, engine, configuration, preprocessing, smoothing,
support, or observation scenario. No successor candidate existed during this
freeze.

The final split is August 2026. Its four ten-minute windows cover Asia,
London, New York, and overlap/closure; one exact ONS event window and three
ordinary windows; all three observation scenarios; exact and bounded-nearest
alignment; and low, median, and high deficit strata. Each internal neighbor
gap exceeds seven days. The earliest August window is also more than seven
days after the latest RC22 window.

## Frozen identities

- projection manifest:
  `benchmark-source-projection-manifest:sha256:d1a4b8e8720772a7a8813ac4500ac94a34514c14ccd6b347a821c7389327029e`;
- feed-epoch definition:
  `feed-epoch-definition-v2:sha256:b93c0f80968a0c2e3ab7ca62fc7270f10a14826f8596252b86017b95531e7d82`;
- observation campaign:
  `observation-calibration-campaign-v2:sha256:405dded75e1189ec9506ab61bc422392d615458d42fd143b726ee8c609a02138`;
- observation operator:
  `observation-operator:sha256:e8bb1b906f26c0fc0a9bf59a7220d0ba42d5445a940303fa31397802ac5c4d27`;
- market-context corpus:
  `market-context-corpus:sha256:f14fbde7511c12a61624362b9c6457140511a1c5e4b3006c7512fe1ce3222897`;
- benchmark corpus:
  `reverse-degradation-corpus:sha256:d2a5f052570896b28febbcbe1247249a9cc86934ceabed7408e1cfb7b031c7fa`;
- unchanged access policy:
  `release-holdout-access-policy:sha256:6300fff28d28c15adaf966e04a9767ad7ab6dfe10924b3f989a768ab752f3825`;
- unchanged evaluation policy:
  `release-holdout-evaluation-policy:sha256:9db3e29a617daa257c5b658ec775e8f615278d13f8048512aa6fc18794c10730`;
- protected manifest:
  `protected-release-holdout-manifest:sha256:2685b1e38d707c8c96cdf84dfd94b6e3721660214155fb24cb95bb7dfdf18c6a`.

The manifest is sealed, `results_opened=false`, and contains no event rows or
candidate identity. Its built-in leakage audit
`release-holdout-leakage-audit:sha256:e22cf0a3b86d4cd128eef240c400cec4a8ab02ccaba4c9db65930fa88eb46534`
and coverage audit
`release-holdout-coverage-audit:sha256:67f05e04361a339f7cd6949d30b3253a56a2b1c610b4cde79e661fef161ec4bd`
both pass without weakened rules.

## Source acquisition and projections

The installed Temporal cache workflow acquired the three August parents in
job `histdatacom-run-114a8031e1404294857b4ebf66a8683f`: three work items,
four child workflows, seven artifact receipts, and a successful first attempt.
The transient ZIPs were removed after the immutable caches were published.

| Symbol | Parent bytes | Rows | Parent SHA-256 | Projection bytes | Projection SHA-256 | Regressions |
| --- | ---: | ---: | --- | ---: | --- | ---: |
| EURGBP | 3,185,305,375 | 869,045 | `dcf2d0a4e5f71c3d88b26530497eb310464799a934cf219c89c9c427f6c39b34` | 24,336,058 | `cde4a86b127067d4006f789115a0cb4e52a0ab7e038a17579730376620a75b13` | 113 |
| EURUSD | 3,858,946,927 | 1,052,845 | `9918af3e6329462039f7bd8c7a1bbe7b7bd6e25b25c44e9acad79e525fb4b3a6` | 29,483,090 | `012e9d137f5ecc6f67a2fc845809098cfeb68556b4b4a538488f12b1519fbf0f` | 58 |
| GBPUSD | 5,924,416,975 | 1,616,415 | `a172718df5f952c38ef66e6a063b4aba91eec4b16f095dd167c765dfff41800d` | 45,264,290 | `dc30bfb70617df2d6b8f80aace4869f4a7437609cdecdd1ee8499202593fe9c4` | 141 |

Every regression is exactly 1,000 ms and is retained in source order. The
projection builder verified the full primitive-value digests. Independent
direct reads of the 12 declared symbol-windows also found identical parent and
projection rows:

| Start UTC | EURGBP rows / SHA-256 | EURUSD rows / SHA-256 | GBPUSD rows / SHA-256 |
| --- | --- | --- | --- |
| 2026-08-06 08:00 | 350 / `2437ea8d4668909dc818ae9d199f9a2674b727e0fa1baf759f446dda4a651e8f` | 357 / `7a778d22b6c9852935dce9a1cb53d57ec5c8e981d126de440071e49505a08997` | 623 / `205cb2283e3379e24c903b11e9353c6f011b2b80f7c44116b8f2d83f88696246` |
| 2026-08-13 14:00 | 547 / `b64da48bf0c0e5ef31b1f6b587cab02dc8e6fc2a941046629b752fca8758a195` | 813 / `be277e24afc7e69e4ff0bf2baf79c635848096fd1b3bdaa67bab205453c59dfb` | 1,143 / `773a92d1109edd993440ff7dd063383072366f9bb3d19020b190474f962db786` |
| 2026-08-21 06:00 | 292 / `14e631497b6de7680d12a7b3a5618697c686af3c55f037d7d33a391c70be2aae` | 232 / `94385b60dbde32fe21a814859a45276bb2555c0e9d7044cea8c50f6d72ddba10` | 213 / `b78d7c10c32fe2314764d4b372ba4e832d7549ca82ec68fffc7ea3cbbab762e4` |
| 2026-08-28 16:00 | 1,076 / `098a8bc873eb34211dcbbb4079039cd3d3515cc04aad86f7a9f332c952a9b99c` | 1,491 / `613e35cabe4ecb6b7e5f338d701d42943d4ef4e03e8bc4f9fa20b7e6dbf9ef69` | 1,532 / `de89666e33f26168f5e479f1ed7a1c38426efc459ac8aa91dfe9139f61c82826` |

The benchmark's frozen per-symbol cap retains at most 256 rows in its compact
window identity. The full-row counts above are independent equivalence checks,
not protected manifest payloads. All source and compact files reside on the
qualified APFS volume `61B748DC-6111-44A5-A0E7-5ABFE23BFDF1`.

Build the compact projections with:

```console
uv run python scripts/build_benchmark_source_projection.py \
  --parent-root .histdatacom/issue-521-campaign-storage-rc20/mount/source/ASCII/T \
  --projection-root .histdatacom/issue-521-campaign-storage-rc20/mount/source-projection-v1/ASCII/T \
  --manifest-directory release-evidence/issue-742 \
  --frozen-at-utc 2026-09-13T13:46:00Z \
  --partition EURGBP:202608:dcf2d0a4e5f71c3d88b26530497eb310464799a934cf219c89c9c427f6c39b34 \
  --partition EURUSD:202608:9918af3e6329462039f7bd8c7a1bbe7b7bd6e25b25c44e9acad79e525fb4b3a6 \
  --partition GBPUSD:202608:a172718df5f952c38ef66e6a063b4aba91eec4b16f095dd167c765dfff41800d
```

## Frozen context and observation dependencies

The live official-source builder retained 29 immutable snapshots and replayed
1,044 events through August 31. The exact August 21 window contains the ONS
public-finance timeseries, public-finance release, and retail-sales release
events at 06:00 UTC. The corpus artifact SHA-256 is
`6bcae55fd79438329eecc03548db1229548532ea139deb8980403ebddadbe2d5`.

```console
uv run histdatacom analytics market-context-corpus \
  --artifact-dir .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/successor/rc23/dependencies/market-context \
  --start-date 2002-03-01 \
  --end-date 2026-08-31 \
  --json
```

The existing feed-epoch evidence was extended only with the three projected
August source summaries and refit under the unchanged config
`feed-epoch-fit-config-v2:sha256:5806706fff7f097a6a10cdc7cd0b2e3e45d33530e52c1d9947b4d24fa33e8006`.
All three historical boundaries retained full support and no new transition
appeared; `technology_epoch_04` now ends at `202608`. The definition artifact
SHA-256 is `584b9d6ff9bd7e54cde712346f3016c5f311b1dd7e3a4ba4a6fdfd99080f17f7`.

The observation campaign was then rerun with its unchanged profile and fixed
splits: calibration `202011`, validation `202401`, and final holdout `202510`.
None of the August evidence IDs enters those 108 windows. The profile and all
fitted target parameters match the prior ready campaign; the refreshed
campaign is also ready. Its artifact SHA-256 is
`3d4c6f5bdb3c2637c0ae06485af4096113bd1a3ebc61291b896cb3739c971049`.

```console
uv run histdatacom analytics observation-calibrate-v2 \
  --definition .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/successor/rc23/dependencies/feed-epochs-v2/feed-epochs-v2-definition.json \
  --evidence .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/successor/rc23/dependencies/feed-epochs-v2/feed-epochs-v2-evidence.json \
  --artifact-dir .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/successor/rc23/dependencies/observation-calibration-v2 \
  --sessions asia london new_york \
  --calibration-period 202011 \
  --validation-period 202401 \
  --final-holdout-period 202510 \
  --max-events-per-window 4096 \
  --minimum-events-per-window 512 \
  --max-source-bytes 2147483648 \
  --max-runtime-seconds 600 \
  --max-peak-memory-bytes 2147483648 \
  --json
```

## Freeze and independent audit

Reproduce the sealed repository artifacts with:

```console
uv run python scripts/freeze_release_holdout_evidence.py \
  --source-root .histdatacom/issue-521-campaign-storage-rc20/mount/source-projection-v1/ASCII/T \
  --source-projection-manifest release-evidence/issue-742/benchmark-source-projection-manifest-e11243ea890f05bd5ff2066e86ea2fe9ac287e525632df6668635d56f3b40d57.json \
  --feed-epoch-definition .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/successor/rc23/dependencies/feed-epochs-v2/feed-epochs-v2-definition.json \
  --observation-campaign .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/successor/rc23/dependencies/observation-calibration-v2/observation-calibration-v2-campaign.json \
  --market-context-corpus .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/successor/rc23/dependencies/market-context/market-context-corpus-6bcae55fd79438329eecc03548db1229548532ea139deb8980403ebddadbe2d5.json \
  --cftc-positioning-corpus .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/dependencies/cftc-positioning/cftc-positioning-corpus-0bf25abfa3e98b85e00a4c2fb7d7907d43353795660373e3debf87a3d2a7d0cb.json \
  --selection-dossier .histdatacom/issue-519-requalification/selection-v3/hawkes-product-selection-dossier-758c1fba9f6cd24b0d1790083c2d5db5ba4836d67ca8678138ed7903303128bc.json \
  --declaration-file release-evidence/issue-742/release-holdout-declaration-v2.json \
  --output-directory release-evidence/issue-742 \
  --frozen-at-utc 2026-09-13T13:55:15Z
```

Replay from a separate temporary working directory verified all 9 source
partitions and all 36 window partitions with zero hash mismatches. Treating
the four June RC19 and four July RC22 protected windows as prior-holdout units
produced cross-retirement audit
`release-holdout-leakage-audit:sha256:7d878bd29018aea539a07bf9c8e1ee8fc9733c144a8547389ee28b28d3bdb1cd`:
38 checked pairs, no findings, and `rows_inspected=false`.

Do not authorize or fit a successor candidate from this working tree. The
manifest, policies, corpus, declaration, and freezer implementation must be
committed and pushed first. RC22 retirement is recorded only after that commit
exists and binds this exact successor manifest identity.
