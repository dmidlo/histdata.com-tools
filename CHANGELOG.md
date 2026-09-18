## Unreleased

### Added

- **market context**: qualify the uninterrupted 2001-Q3 through 2026-Q2
  Eurostat quarterly labour-cost publication lineage, preserve source-era
  euro-area composition, signed headline movements, the 2012 business- to
  whole-economy activity-scope break, total/wages/other table values for the
  euro area, Germany, and France, and scope-safe revision comparisons; retain
  the bounded Atom, landing, and discovered
  English-document corpus; and bind an independent `lc_lci_r2_q`
  revised-series cross-check without substituting current EA21 values for
  historical publication vintages (#539).
- **market context**: qualify the uninterrupted April 2012 through July 2026
  Eurostat monthly international-trade-in-goods publication lineage, preserve
  source-era EA17 through EA21 composition, monthly versus year-to-date
  headline scope, eight source-authored flow-table measures, repeated-month
  revision comparisons, and every broad-search exclusion; retain the bounded
  Atom, landing, and discovered English-document corpus; and bind an
  independent six-measure `ext_st_easitc` revised-series cross-check without
  deriving balances or substituting current EA21 values for historical
  publication vintages (#539).
- **market context**: qualify 298 source-dated Eurostat retail-trade
  publications from April 2000 through September 2026, preserve the explicit
  searchable-release gaps, source-era headline measure and euro-area
  composition, 9,393 supported EA/Germany/France table values, and revision
  comparisons; retain all 298 landings and 267 discovered English documents;
  and bind an independent `sts_trtu_m` revised-series cross-check without
  substituting current values for historical publication vintages (#539).
- **market context**: qualify the complete March 2002 through September 2026
  Eurostat construction-output publication lineage across its quarterly and
  monthly eras, preserve duplicate and gap semantics, source-era euro-area
  composition, signed headline movements, and supported EA/Germany/France
  publication-table values, retain all 257 landings and 225 discovered English
  documents, and bind the independent revised `sts_copr_m` monthly-series
  cross-check without projecting current values into historical vintages
  (#539).
- **market context**: qualify the complete January 2002 through September 2026
  Eurostat monthly-industrial-production publication lineage, preserve
  source-era euro-area composition, signed headline movements, and supported
  EA/Germany/France monthly and annual table values, retain the bounded Atom,
  landing, and English-document corpus with explicit duplicate and page-date
  evidence, and bind an independent `sts_inpr_m` revised-series cross-check
  without substituting current values for historical publication vintages
  (#539).
- **market context**: qualify 296 source-dated Eurostat monthly-unemployment
  publications from January 2002 through September 2026, preserve the
  source-era euro-area composition, headline direction and rate, supported
  EA/Germany/France table values, and repeated-month revision comparisons;
  retain the complete 383-result Atom selection, explicit exclusions, all 296
  landings, and all 264 discovered English documents; and bind a 2000-01
  onward `une_rt_m` revised-series cross-check without treating it as
  historical-vintage evidence (#539).
- **market context**: qualify 279 source-dated Eurostat quarterly-GDP
  publications from January 2002 through September 2026, preserve distinct
  preliminary-flash, flash, first-, second-, third-, and regular-estimate
  stages, exact modern publication clocks, 248 English release documents, and
  source-era EA/Germany/France q/q and y/y values with revision comparisons;
  retain the complete 932-result Atom selection and explicit false positive;
  and bind a 2000-Q1 onward revised-series cross-check without treating it as
  historical-vintage evidence (#539).
- **market context**: qualify all 319 published PPI headline releases from
  January 2000 through August 2026 with 320 distinct current/predecessor
  artifacts, exact 08:30 publication times, legacy text and semantic/fallback
  HTML parsing, the 2014 finished-goods/final-demand lineage break, 59 observed
  prior-period revisions, and the explicit October 2025 publication gap
  (#538).
- **market context**: qualify all 318 published CPI-U all-items releases from
  January 2000 through July 2026 with current/predecessor artifact hashes,
  exact 08:30 publication times, legacy text and HTML parser eras, an official
  PDF substitution for the empty May 2016 HTML response, explicit 2025
  appropriations-lapse semantics, and a complete inflation coverage slice
  (#538).
- **market context**: qualify all 319 Federal Reserve G.17 releases from
  January 2000 through August 2026 with a retained official release index,
  unique raw and normalized hashes, independently replayed headline triplets,
  explicit historical encoding/header eras, and a complete quantified
  industrial-production coverage slice (#538).
- **market context**: add a 22-program, 12-family U.S. official macro backfill
  profile; DOL/ETA and Philadelphia Fed source entries; bounded plain-text
  replay; a retained real 2002 Federal Reserve G.17 artifact with independently
  recomputable actual/previous/revised triplet; and fail-closed quantified
  2000-present coverage audits (#538).
- **market context**: add versioned retail, production, capital-goods, trade,
  housing, confidence, and official-survey concept, window, observation,
  triplet, release-package, revision, and unsupported-gap semantics; preserve
  exact units and published transformations and audit legal-producer ownership
  and difficult cases across all 21 economy profiles (#578).
- **market context**: add versioned GDP/national-accounts concept, stage,
  observation, growth derivation, triplet, component reconciliation, release
  sequence, benchmark, and methodology semantics; preserve first-published
  stage actuals and audit difficult cases across 21 economy profiles (#577).
- **market context**: add versioned labour concept, observation, derivation,
  expectation, triplet, release-package, and methodology semantics; preserve
  exact survey identity and published changes, refuse current-level historical
  fabrication, and audit difficult cases across all 21 economy profiles (#576).
- **market context**: add versioned inflation/index concept, observation,
  derivation, expectation, triplet, component, release-stage, and methodology
  semantics; prohibit latest-current rates from masquerading as historical
  initial actuals; and audit legal-producer ownership and difficult cases for
  all 21 economy profiles (#575).
- **market context**: add versioned monetary-policy meeting, phase, setting,
  decision, expectation, vote, and surprise semantics; preserve ranges, rate
  sets, yield targets, currency-board and exchange-rate frameworks; refuse
  retrospective emergency scheduling and scalarized text surprise; and audit
  all 21 official monetary-source profiles (#574).
- **market context**: add exact-version reusable SDMX 2.1/3.0, JSON-stat,
  JSON/CSV/TSV, XLS/XLSX, HTML, RSS/Atom/ICS, PDF, static-archive, and
  data-catalog adapter packs with raw-to-record provenance, explicit schema
  drift/reacquisition failures, header versioning, and registry/fixture
  qualification audits (#584).
- **market context**: add archive-first historical vintage governance with a
  complete 21-economy/event-family recovery matrix, content-addressed official
  artifact/record bindings, deterministic mirror reconciliation, explicit
  source eras and gaps, immutable latest-value cross-checks, and benchmark/
  simultaneous-revision audits (#581).
- **market context**: add content-addressed historical release-schedule
  corpora, catalog-qualified occurrence binding, immutable reschedule/delay/
  cancellation/emergency evidence, explicit timezone/holiday/DST versions,
  scheduled-time point-in-time queries, and no-future replay audits (#579).
- **market context**: add a content-addressed 21-economy indicator catalog,
  versioned methodology and recurrence rules, evidence-backed rename/rebase/
  split/merge lineage, explicit expected-occurrence states, and fail-closed
  structural coverage and gap audits (#572).
- **market context**: add deterministic official-archive release-time
  hierarchy, immutable schedule/publication/revision mutation chains,
  first-actual deltas, fixed previous-as-known cutoffs, difficult-case
  diagnostics, and machine-verifiable point-in-time reconstruction audits
  (#537).
- **market context**: add a reviewed authoritative-source matrix covering all
  21 scoped economies and event families, plus source-bound bounded HTTP,
  conditional fetches, credential-safe manifests, immutable raw snapshots,
  content-addressed replay, typed parser seams, and lexical-time preservation
  (#535).
- **market context**: freeze provider-neutral economic event/series identity,
  publication-stage and schedule-status taxonomies, complete forecast scopes,
  calendar triplets, DST-aware lexical time evidence, auditable unit
  conversion, prior-only robust surprise policy, and bounded lossless Arrow
  interchange (#534).
- **market context**: salvage immutable economic release/revision,
  previous-as-known, forecast provenance, bounded query, and content-addressed
  replay mechanics behind a provider-neutral `as_known_at(t)` protocol; add an
  exhaustive migration map that retires the licensed-calendar branch without
  making it a canonical dependency (#682).
- **testing**: add commit-bound per-critical-module branch floors, bounded
  generated scientific invariants, exact focused/release mutation profiles,
  retained CI reports, and release-candidate evidence gates (#520).
- **reconstruction**: add stratified synthetic-delta physical/runtime
  measurement, residual-guarded campaign resource envelopes, exact
  all-member storage/inode/duration forecasts, frozen concurrency/shard and
  amplification policy, conservative packing review, and strong mounted-
  storage disconnect/remount admission evidence (#518).
- **scientific validation**: add an installed, content-addressed reconstruction
  math harness with independent negative-binomial, Hawkes stability,
  time-rescaling, proper-score, projection, triangle-envelope, quote-age, and
  no-future-use checks, plus a certification-bound machine report (#507).
- **research**: freeze a content-addressed reconstruction estimand and
  assumption ledger, explicit market-context/CFTC missingness states,
  generated-row claim constraints, v2.4 legacy-unbound replay policy, and
  experiment-to-certification lineage for the HistData-only v2.5 product
  (#506).
- **reconstruction**: materialize the complete HistData triangle campaign
  surface with adaptive resource-safe v2 plan shards, exact gap-free support
  maps, durable request/receipt sets, first-party Temporal status/cancel/resume,
  reconciled campaign product indexes, provider-neutral dataset publication,
  and installed bounded inspection commands (#498).
- **reconstruction**: add qualified historical cardinality conditioning,
  exact-or-bounded-prior asynchronous triangle support, explicit CFTC-
  unavailable conditioning, source-support/runtime agreement, and v3
  synthetic-delta products that retain immutable observed-anchor evidence
  without duplicating source rows (#498).
- **reconstruction**: document the end-to-end complete-campaign operator path,
  including mounted-storage qualification, full-intersection planning,
  refusal review, Temporal execution, forced crash/resume, product replay, and
  final dataset publication (#498).
- **certification**: advance the broker-neutral policy to v2.5.0 with explicit
  gap-free support, valid-data refusal, full Temporal campaign, complete
  retained-product rectangle, no-fabricated-liquidity, product-index, dataset,
  mounted-storage integrity, and disconnect/resume evidence gates (#498).
- **datasets**: add provider-neutral HistData and explicit-UTC fixture adapters,
  immutable observed/derived dataset manifests, qualified aliases, exact
  resolution/replay receipts, query-bound cursors, strong verification,
  provider-neutral reconstruction inventory preflight, V2 API lineage, and an
  installed bounded catalog CLI while preserving V1 HistData identities (#78).

- **reconstruction**: add a versioned broker-neutral v2.1 certification policy,
  hash/schema/subject-verified campaign runner, JSON-pointer observation
  extraction, atomic machine/human dossier publication, and installed
  `reconstruction certify` command while preserving legacy V1 evidence replay
  (#449).
- **reconstruction**: add exact paired nanosecond plan bounds for small
  representative-window campaigns while retaining complete touched-month
  source hashing, and report candidate amplification against aggregate
  estimated inputs instead of mixing all ensemble members with one raw-source
  denominator (#449).
- **reconstruction**: add content-addressed full-range plan sets and public
  `plan-set`/`preflight-set` operations so resource-safe daily windows can span
  the common history without exceeding the independent 64 MiB plan-artifact
  bound (#449).
- **reconstruction**: add atomic content-addressed write/readback verification
  for compact final-product activity manifests used by certification and
  downstream bar reconciliation (#449).

- **reconstruction**: expose the first-party reconstruction pipeline through
  an installed CLI family and typed Python facade, with explicit information
  mode/nonclaim requests, preflight resources and refusals, Temporal and local
  execution, aligned status/cancel/resume receipts, bounded lineage previews,
  replay verification, and stable exit codes (#467).
- **data-quality**: make weekend and expected-session-closure remediation
  guidance profile-aware with bounded calendar-policy context while preserving
  stable public weekend hint codes (#344).
- **data-quality**: preserve value-level quality-profile provenance during
  resolution across defaults, named profiles, files, YAML, API options, and CLI
  overrides, including previous source/value evidence (#367).

### Changed

- **dependencies**: update the supported GitHub Actions toolchain and Python
  packaging/lint dependencies while preserving the newer `dev`-only runtime,
  model, documentation, and timezone requirements (#477, #478, #479).

### Fixed

- **reconstruction**: preserve each operator-supplied artifact, output,
  checkpoint, and scratch base across adaptive plan-set shards so a campaign
  cannot silently redirect product and staging data into the local artifact
  tree instead of its qualified storage volume (#498).
- **reconstruction**: preserve and revalidate the exact content identity of
  retained v1 plan sets written before additive dataset, evidence, and
  cross-series source-spec defaults, without accepting changed shards,
  resources, or unknown fields; admit the earlier complete all-ensemble task
  layout while continuing to reject incomplete member/window grids (#491).
- **portability**: make peak-RSS measurement import-safe on Windows, ship
  the required IANA timezone data, use Python 3.10-compatible container
  timestamps and portable filesystem durability operations, and isolate
  numerical goldens from optimizer/runtime drift so clean wheel installs can
  load and verify the full analytics/reconstruction surface (#473, #474,
  #479).
- **reconstruction**: preserve Arrow partition-row order for equal-timestamp
  source ticks, externalize proposal and carving batch evidence into bounded
  content-addressed ledgers, enforce live RSS limits, and truncate very large
  cross-currency refusal lists with a deterministic count and digest; inject
  one bounded, source-grid-aligned anchor from the sparsest declared leg into
  missing proposal legs so independently sampled modern streams have genuine
  exact-time triangle support without replacing immutable observations, and
  try every declared synthetic projection target before refusing a feasible
  cross-currency point (#449).
- **reconstruction**: resolve exact nanosecond plan bounds to source months with
  integer time conversion so the last nanosecond before a month boundary cannot
  round into the following partition (#449).
- **reconstruction**: compact high-cardinality activity provenance into bounded
  retained IDs plus explicit occurrence counts and ordered SHA-256 evidence
  instead of refusing ordinary reconstructed products at 256 IDs (#449).
- **reconstruction**: de-duplicate source partitions and strong artifact
  verification across adaptive full-range plan shards, with stat-identity hash
  and qualified-input caching plus compact streaming aggregation, so split
  months report unique raw rows and neither construction nor public preflight
  repeatedly materializes immutable corpora or retains every full shard graph
  in memory (#449).
- **reconstruction**: retain scientifically unsupported full-range spans as
  bounded refusal-only plan shards with zero workflow/output estimates, so
  public planning stays exactly contiguous without converting missing context
  into executable work (#449).
- **reconstruction**: reconcile end-to-end window runtime and peak stage
  resources into run reports instead of exposing atomic-commit runtime as if it
  represented the whole seven-stage execution (#449).
- **ci**: reserve coverage for one required `dev`-to-`main` production
  promotion job instead of running it during routine pushes, issue closure,
  and every Python/OS test-matrix job (#420).

## 1.3.2 (2026-07-03)

### Added

- **workflow**: add reusable issue closure/readiness tooling with acceptance
  coverage, pre-mutation gates, slow-phase summaries, no-mutation checks, and
  compact closeout reports.
- **data-quality**: add cache-scale preflight evidence, freshness policy,
  latest validation discovery, and operator-facing quality guidance.
- **orchestration**: add scheduled submission preflight, overlap guards,
  schedule identity filters, run request export, and scheduled run bundles.
- **instruments**: add individual triangle pair groups and group discovery CLI
  coverage.

### Changed

- **runtime**: keep Linux and macOS worker-starting bundled-runtime release
  smokes blocking while documenting the Windows install/CLI-only release gate
  until the native Temporal/Nexus worker startup blocker is resolved.

### Fixed

- **runtime**: improve Windows worker startup diagnostics, retry reporting,
  process isolation, startup cleanup, and Temporal SDK compatibility bounds.
- **release**: strengthen signing preflight, bundled runtime smoke gates, and
  Windows runner diagnostics.
- **data-quality**: make cache-scale quality checks viable and ensure quality
  preflight artifacts remain disposable.

### Added

- **data-quality**: add cache-scale preflight decisions, no-target diagnostics,
  saved evidence checks, and non-blocking warnings before large cache-backed
  quality runs.

## 0.79.0 (2026-06-24)

### Changed

- **temporal-orchestration**: make the Temporal orchestration the default CLI/API runtime.
  The foreground runtime remains available as a compatibility rollback through
  `--foreground` or `Options.use_orchestration = False`, and default orchestration runs
  start the bundled local orchestration when needed.

## 0.78.4 (2022-12-13)

### Fix

- **histdatacom**: address keyboard inturrupt for all but import to influx stages

## 0.78.3 (2022-12-05)

### Fix

- **package**: removed bs4 proxy dependency

## 0.78.2 (2022-12-05)

### Fix

- **cli.py**: sort arguments into logical groups

## 0.78.1 (2022-12-05)

### Fix

- **package**: add [pandas],[arrow], and [jupyter] pip install flags

## 0.78.0 (2022-12-05)

### Feat

- **package**: add --version arg to report version
