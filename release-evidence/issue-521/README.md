# Issue #521 successor release-holdout evidence

This directory freezes the row-free scientific evidence for the second
successor to the retired RC19 holdout. It is committed before any successor
candidate is fitted, frozen, authorized, or evaluated.

The corpus uses calibration period `202512`, validation period `202601`, and
fresh final-holdout period `202607`. Its four ten-minute final windows cover
Asia, London, New York, and overlap/closure; ordinary and FOMC event context;
all three observation scenarios; exact and bounded-nearest alignment; and
low, median, and high deficit strata. Window selection used only synchronized
source support and the already frozen context, observation, feed-epoch, CFTC,
and engine-selection artifacts. No successor candidate existed during
selection.

The final identities are:

- projection manifest:
  `benchmark-source-projection-manifest:sha256:f7151bb11cf43efb3baf99398a6c19c72fe2fc01d2635a58cb44cf77067f32bf`;
- benchmark corpus:
  `reverse-degradation-corpus:sha256:5982972be6853178eacd07a5774cf354695f9ea9e0b743f6a97e33af44fab7e5`;
- access policy:
  `release-holdout-access-policy:sha256:6300fff28d28c15adaf966e04a9767ad7ab6dfe10924b3f989a768ab752f3825`;
- protected manifest:
  `protected-release-holdout-manifest:sha256:b3d285d61c204ef5dd3c36375f30dcff5b4ea7780172189931ad8d163b98243d`;
- evaluation policy:
  `release-holdout-evaluation-policy:sha256:9db3e29a617daa257c5b658ec775e8f615278d13f8048512aa6fc18794c10730`.

The July HistData acquisition caches are 521-column enriched Arrow files.
The benchmark consumes the canonical `datetime`, `bid`, `ask`, and `vol`
columns, so the projection manifest binds deterministic four-column artifacts
to the exact enriched-parent SHA-256, byte size, row count, relative partition,
first/last timestamp, source-order regression evidence, projected SHA-256,
and batch-independent primitive-value digest. It contains no event rows.

| Symbol | Parent bytes | Rows | Regressions | Projection bytes | Projection SHA-256 |
| --- | ---: | ---: | ---: | ---: | --- |
| EURGBP | 5,028,599,679 | 1,371,981 | 103 | 38,419,514 | `e517d3ac02036e3fc89458735749b298379c081ca669b2df69ac9f07b63c8eb2` |
| EURUSD | 5,061,028,863 | 1,380,788 | 203 | 38,666,098 | `a977ee0ae8bf05578aefdb11e0a006f4a0ec4bdc77f07489e5c47a7cb54393cb` |
| GBPUSD | 8,116,894,999 | 2,214,539 | 319 | 62,013,306 | `875500e9d63010effbc0efa518e4f879fbe0c4c7e03b204ce35b1baca95061df` |

Every observed regression is exactly 1,000 ms and remains in source order.
Independent direct reads of all 12 declared symbol-windows produced identical
row IDs, timestamps, bids, asks, counts, and quote-window hashes from parent
and projection files. Full projection replay also passes. The nine compact
corpus partitions total 383,112,160 bytes. The qualified storage volume is
APFS device `16777244`, UUID `61B748DC-6111-44A5-A0E7-5ABFE23BFDF1`.

The exact parent verification and projection build took 1,488.10 seconds and
229,228,544 bytes peak RSS. That one-time evidence construction is outside the
campaign. The final repository-bound freeze took 5.69 seconds and 573,308,928
bytes peak RSS. The corpus retains its 3,600-second, 2 GiB, and 4 GiB source
construction bounds; the frozen benchmark policy retains its 900-second
campaign threshold. Raising a frozen resource gate was not required.

The manifest is sealed and row-free. Its source hashes, exact-window hashes,
counts, signatures, and neighbor sketches replay from the qualified compact
root. Neighbor leakage is zero. Leakage and coverage audits pass without
weakened split rules or missing strata. The February 1, 2026 source cutoff is
the first month boundary after the January validation split; July rows cannot
enter candidate fitting or policy selection.

Build the July projections with:

```console
uv run python scripts/build_benchmark_source_projection.py \
  --parent-root .histdatacom/issue-521-campaign-storage-rc20/mount/source/ASCII/T \
  --projection-root .histdatacom/issue-521-campaign-storage-rc20/mount/source-projection-v1/ASCII/T \
  --manifest-directory release-evidence/issue-521 \
  --frozen-at-utc 2026-09-13T03:39:39Z \
  --partition EURGBP:202607:a9872256e6ad73fb6be42834bbaa91f732cd973f5a11017dd510f14f9a72dfd2 \
  --partition EURUSD:202607:ec4af6a2ba0bbaf662df9eb7d268d327419ddf328b1e586e41075adcd1d80531 \
  --partition GBPUSD:202607:80c2d92364f383ff8a2e02cad4de010216496fd473341c1e9308c4fe3d6c400d
```

After colocating hash-identical December and January sources at the compact
root, reproduce the sealed artifacts with:

```console
uv run python scripts/freeze_release_holdout_evidence.py \
  --source-root .histdatacom/issue-521-campaign-storage-rc20/mount/source-projection-v1/ASCII/T \
  --source-projection-manifest release-evidence/issue-521/benchmark-source-projection-manifest-565871662140a5642f556f4216711b57904b16c8770f6d57a93adec80f0c73b8.json \
  --feed-epoch-definition .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/dependencies/feed-epochs-v2/feed-epochs-v2-definition.json \
  --observation-campaign .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/dependencies/observation-calibration-v2/observation-calibration-v2-campaign.json \
  --market-context-corpus .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/dependencies/market-context/market-context-corpus-523ea8240eac429865f601ba1efd2360a1608d3a101df7f7a4b505315473b0cb.json \
  --cftc-positioning-corpus .histdatacom/issue-521-campaign-storage-rc20/mount/evidence/dependencies/cftc-positioning/cftc-positioning-corpus-0bf25abfa3e98b85e00a4c2fb7d7907d43353795660373e3debf87a3d2a7d0cb.json \
  --selection-dossier .histdatacom/issue-519-requalification/selection-v3/hawkes-product-selection-dossier-758c1fba9f6cd24b0d1790083c2d5db5ba4836d67ca8678138ed7903303128bc.json \
  --declaration-file release-evidence/issue-521/release-holdout-declaration-v1.json \
  --output-directory release-evidence/issue-521 \
  --frozen-at-utc 2026-09-13T03:39:39Z
```

Do not authorize evaluation from this commit. The selected Marked Hawkes fit,
candidate graph, certification registry, and storage-bound release candidate
must be created and validated in a later descendant commit before the one-time
authorization.
