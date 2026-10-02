# Integrated source: preparation status

Point-in-time record: **2026-09-27**, issues #621 and #754. This note records
completed offline build/install preparation, not a software certification,
physical conformance report, provider qualification or release approval.
The projects remain unpublished at plugin version `0.1.0`, implementing public
SDK `1.0.0`. The reviewed development host has distribution metadata `2.5.0`
on the unreleased-v3 track; that version string alone is not compatibility
evidence.

## Reviewed artifacts and completed preparation

The exact reviewed host wheel is `histdatacom-2.5.0-py3-none-any.whl`, SHA256
`a383a49830dc9cdc5c761aa0dc6ec6dd15f034de2b40dac6056a0e1cb7b8297e`.
It exposes catalog `2.0.0`, whose artifact ID is
`broker-conformance-catalog:sha256:7c4965dad269a282167e08b1b04a7caaa6feaf4fc78da4d87b6229aef008b705`.
The following independently built wheel archives were retained:

| Project / build interpreter | Wheel SHA256 |
| --- | --- |
| reference / Python 3.13.5 | `ce5ff674f3d5ba3c7fe6671f2c355978e919cbcb37eb8d0bfac31d4c2570402e` |
| reference / Python 3.10.19 | `d9031afea3d1c190b892c8ca79fea04e170ab3beccab7c0f62c0d2cc5bfc1d7d` |
| starter / Python 3.13.5 | `046a215ebf4adf656381125496fe50c89fb7569b71929f3af33e7dec1b60afc6` |
| starter / Python 3.10.19 | `a019869a6301cd255b8f1b853328567ff52ca16e2508fa79359344e72fbfd982` |

Each project built offline through an ordinary isolated PEP 517 sdist-to-wheel
path. Per-project wheel member bytes match across interpreters; ZIP archive
hashes differ. Four fresh environments, one per project/interpreter pair,
installed the reviewed host and both plugins with normal dependency resolution.
Dependency, RECORD hash/length/inventory, installed-file and import-origin
checks passed before and after the unit suites. Discovery inspected metadata
without importing either plugin entry module.

Each of the four complete installed project test collections passed all **58
nodes**: **232 lane-node results**, with no failures, errors, skips or excluded
tests. These are standalone-project results, not additions to the host suite
denominator. Direct generated plugin calls are unit tests; operator unit tests
stub native calls and do not prove isolation, capture or worker cleanup.

This note and its source-packaging/metadata-lock changes were added after those
archive builds. The hashes above identify the original retained archives, not
a rebuilt sdist or newly qualified archive. README, runtime code, registration,
permissions and driver bytes are intentionally unchanged. A later source
archive must be checked independently before claiming archive equivalence.

## Prepared plans and remaining acceptance

Complete current-catalog plans were generated, not executed as part of this
preparation:

| Subject / profile | Cases | Capabilities |
| --- | ---: | ---: |
| reference / trusted | 28 | 7 |
| reference / dual | 42 | 8 |
| starter / dual | 38 | 6 |
| host-reference selftest / isolated | 21 | 1 |

The candidate plans include `session.freshness`. Full physical runs on the
supported interpreters and independent fresh-process verification remain
pending in this preparation record. The host-reference selftest is explicitly
noncertifying even if every case passes; it cannot certify a plugin candidate.

Four read-only installed operator preflights passed. Actual public capture,
pinned replay, typed tamper/rights refusal, exact-plugin uninstall and
post-uninstall plugin-free replay remains pending; preflight does not satisfy
those gates. Prepared operator plans contain no conformance-report claim.

Earlier catalog1 physical reports remain historical evidence only. They do not
qualify catalog2. The unstarted catalog1 starter plan is **not executed** and
is superseded by the complete catalog2 starter plan, not relabeled as a pass.
No publishing, version bump or empirical provider study is authorized by this
note. Follow [the developer guide](DEVELOPER_GUIDE.md) and [operator guide](OPERATOR.md)
for the public workflow and its separate software/scientific boundaries.
