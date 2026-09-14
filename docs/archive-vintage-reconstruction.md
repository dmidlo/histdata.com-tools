# Archive-first economic vintage reconstruction

`histdatacom.market_context.archive_vintages` binds retained official source
bytes to immutable economic release-vintage chains. It governs the evidence
produced by source-specific discovery and parsers; it does not fetch archives
or interpret a source format itself. The bounded fetch/replay layer remains in
the [official-source registry](official-source-registry-and-fetch.md), and the
temporal mutation rules remain in the
[release-vintage engine](release-vintage-reconstruction.md).

The governing rule is fail closed. For reference period `r`, retain the
observed publication vintages

`V_r = {(x_(r,k), tau_k, s_k, h_k)}_(k=1..K_r)`,

where `tau_k` is the first supported availability time, `s_k` is the official
source identity, and `h_k` is the exact source or normalized-record hash. A
query at cutoff `t` may select only the last member whose `tau_k <= t`. If no
retained artifact supports a past value, that value is unknown. A current API
response cannot be projected backward to fill the gap.

## Recovery declarations

Every one of the 21 scoped economies and 12 event families has exactly one
`EconomicVintageCoverageDeclarationV1`. An adapter must declare one recovery
mode and separate qualification for initial actuals and previous-as-known
values:

| Mode | Permitted historical evidence | Meaning |
| --- | --- | --- |
| `api-vintage-complete` | A current API that exposes prior versions | Requires a registered historical-vintage capability and empirical source verification. |
| `archive-reconstructed` | Contemporaneous releases, archived tables, or official mirrors | Values are reconstructed from occurrence-specific official artifacts. |
| `snapshot-dependent` | A retained value-bearing snapshot | Coverage cannot begin before the snapshot was captured. |
| `historically-incomplete` | Partial artifacts or explicit gaps | One or both historical claims remain unqualified. |

`conservative_official_vintage_coverage()` materializes the complete 252-cell
matrix without inferring empirical coverage from a reviewed endpoint. In the
packaged registry, 250 primary cells are explicitly latest-only and the other
two advertise historical-vintage capability but are not empirically verified;
all 252 therefore begin as historically incomplete. Concrete adapters must
replace only the declarations their retained evidence qualifies.

Qualified declarations carry a half-open coverage interval, content-addressed
artifact evidence, qualification notes, and limitations. A corpus marked
complete is rejected while any declaration is incomplete, either historical
claim is unqualified, or any explicit gap remains.

## Evidence graph

| Contract | Purpose |
| --- | --- |
| `EconomicArchiveArtifactV1` | Projects an immutable `OfficialRawSnapshotV1` into a typed current API, contemporaneous release, archived table, retained snapshot, official mirror, or archive-index artifact while retaining request, source, format, content hash, retrieval time, parser version, locator, and limitations. |
| `EconomicArchiveVintageBindingV1` | Binds one parsed record locator and normalized-record hash to one exact release ID and revision sequence. |
| `EconomicArchiveMirrorResolutionV1` | Partitions all candidate bindings into the selected record, hash-equivalent mirrors, and lower-precedence conflicts. |
| `EconomicArchiveSourceEraV1` | Declares a non-overlapping half-open source/parser interval for an indicator, including migrations between archive systems. |
| `EconomicVintageGapV1` | Records missing artifacts, latest-only limitations, parser gaps, source-migration gaps, or unresolved mirror conflicts instead of inventing values. |
| `EconomicLatestValueCrossCheckV1` | Compares a terminal historical release with one located record in a current API artifact without changing the release. |
| `EconomicArchiveVintageCorpusV1` | Stores the complete sorted evidence graph, all 252 coverage declarations, release chains, gaps, limitations, and a deterministic corpus identity. |
| `EconomicArchiveVintageAuditV1` | Reports recovery modes, incomplete cells, gaps, current-value differences, mirror conflicts, and multi-period revision integrity. |

The corpus validates the registry ID, source identity, declared format and
capabilities, parser identity/version, economy/family ownership, and every
cross-reference. Every release vintage has exactly one mirror resolution;
every candidate must name that exact release. Every selected record is covered
by exactly one matching source era, and every retained artifact must be used by
coverage, a binding, an era, a gap, or a current-value check. JSON restoration
recomputes all nested identities and rejects tampering.

## Adapter workflow

1. Use the registered official API for series discovery, metadata, and a
   current-value cross-check.
2. Enumerate official publication indexes, releases, RSS/ICS records, and
   archived CSV, spreadsheet, HTML, or PDF artifacts by occurrence.
3. Fetch each resource through the bounded official-source request layer and
   retain the exact raw snapshot before parsing.
4. Project each snapshot to an archive artifact, parse an exact record, and
   bind its normalized hash and locator to an immutable release vintage.
5. Reconcile duplicate official locations with
   `resolve_economic_archive_mirrors()`. Lowest numeric precedence wins;
   equal-precedence disagreement is an error, while lower-precedence
   disagreement remains an audit diagnostic.
6. Declare source/parser eras explicitly, build the corpus with
   `build_economic_archive_vintage_corpus()`, and require
   `audit_economic_archive_vintages().passed` before downstream use.
7. Persist the canonical bounded corpus with
   `write_economic_archive_vintage_corpus()` and replay it with the matching
   official-source registry.

Current cross-checks are deliberately one-way. They must use a `current-api`
artifact, identify a concrete record locator, occur after that artifact was
retrieved, and compare against the selected terminal historical binding. A
hash difference is reported but never mutates the historical chain.

## Difficult cases

- **Simultaneous previous revision:** the prior-period mutation must name the
  triggering later event in the same series, use the same publication time,
  and include the prior reference period among its affected periods.
- **Benchmark or rebase batch:** every affected period for a series must be
  represented by a benchmark mutation selected from the same official
  artifact. Missing members fail the audit.
- **Source migration:** each release must fall in exactly one non-overlapping
  source/parser era. The era itself must cite evidence from that source.
- **Missing historical artifact:** retain the attempted archive/index evidence
  and an explicit gap. A current value is not a substitute.
- **Retained snapshots:** a value is usable only from capture onward. Backdated
  snapshot-dependent availability is rejected.
- **Duplicate mirrors:** identical normalized hashes are equivalent. A
  conflicting top-precedence record is ambiguous and rejected; a conflicting
  lower-precedence record remains visible in the audit.

The reusable format parsers and concrete source adapters are downstream work.
They must populate this evidence graph rather than weakening its temporal,
coverage, or provenance rules.
